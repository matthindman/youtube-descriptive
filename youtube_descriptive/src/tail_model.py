# Copied verbatim from Accelerator/common_data_weights/tail_model.py (2026-10-16; the user confirmed
# it is the current missing-mass estimator). Keep in sync with that file; do not edit the math here.

"""
Polynomial-in-log-alpha tail estimation, extracted from
/Workspace/Users/sean.norton@researchaccelerator.org/YouTube Update and Testing/Preliminary Views Analysis/Views Analysis

The math (fits, integrals, tail masses) is unchanged from that notebook. Changes:
  * The ranked relation is a parameter (`relation`, any table/view with columns
    view_count, rank) instead of the hard-coded `yt_ranked` temp view.
  * Every fitting query adds `rank BETWEEN lo AND hi` alongside the original
    `LN(rank) BETWEEN ...` so Delta data skipping prunes files on the rank column.

  * TailEstimates.total(model, obs_total_all) = all observed views + missing mass, where
    missing = modelled mass beyond r0 minus observed mass beyond r0, floored at 0.
  * Added mass_between (modelled mass on ranks r0..r_max) and TailEstimates.eta_coeffs
    for out-of-sample checks. The tail formulas are untouched.

Four nested estimates of the modelled tail mass beyond censoring boundary r0:
  Degree 0 - Power law:   alpha = alpha0                       (upper bound)
  Degree 1 - Constant-eta: log alpha = eta0*u
  Degree 2 - Linear-eta:   log alpha = eta0*u + 1/2 eta1*u^2
  Degree 3 - Quadratic-eta: log alpha = eta0*u + 1/2 eta1*u^2 + 1/6 eta2*u^3
Ordering: PL tail >= deg-1 >= deg-2 >= deg-3 >= truth.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, asdict
from typing import List, Tuple

import numpy as np
from scipy.integrate import quad
from pyspark.sql import SparkSession

# Fitting constants, unchanged from the Views Analysis notebook.
WINDOW_LOGRANK = 2.0
ANCHOR_LOGRANK = 0.05
DELTA = 0.25

_LOG_EXP_CLIP_LOW = -745.0
_LOG_EXP_CLIP_HIGH = 700.0


def _rank_bounds(log_center: float, window: float) -> Tuple[int, int]:
    """Integer rank range covering LN(rank) in [log_center - window, log_center] (padded by 1)."""
    lo = max(int(math.floor(math.exp(log_center - window))) - 1, 1)
    hi = int(math.ceil(math.exp(log_center + 1e-9))) + 1
    return lo, hi


# ── Spark fitting helpers ───────────────────────────────────────────────────────

def _spark_wls_linear(spark: SparkSession, relation: str, log_center: float, window: float) -> Tuple[float, float]:
    """Local linear WLS fit of log(view_count) ~ [1, dx] on rows with LN(rank) in [c - window, c]."""
    lo, hi = _rank_bounds(log_center, window)
    row = spark.sql(f"""
        SELECT
            SUM(w)           AS S0,
            SUM(w * dx)      AS S1,
            SUM(w * dx * dx) AS S2,
            SUM(w * z)       AS T0,
            SUM(w * dx * z)  AS T1,
            COUNT(*)         AS n
        FROM (
            SELECT
                LN(rank) - {log_center}   AS dx,
                LN(view_count)             AS z,
                POWER(GREATEST(
                    1.0 - POWER(
                        ABS(LN(rank) - {log_center}) / {window}, 3.0
                    ), 0.0), 3.0)          AS w
            FROM {relation}
            WHERE rank BETWEEN {lo} AND {hi}
              AND LN(rank) BETWEEN {log_center - window} AND {log_center + 1e-9}
              AND view_count > 0
        ) t
        WHERE w > 1e-15
    """).first()
    n = int(row["n"])
    if n < 5:
        raise ValueError(f"Only {n} rows in linear anchor window at log_r={log_center:.4f}.")
    A = np.array([[float(row["S0"]), float(row["S1"])],
                  [float(row["S1"]), float(row["S2"])]])
    b = np.array([float(row["T0"]), float(row["T1"])])
    return tuple(np.linalg.solve(A, b))


def _spark_wls_quadratic(spark: SparkSession, relation: str, log_center: float, window: float) -> Tuple[float, float, float, int]:
    """Local quadratic WLS fit of log(view_count) ~ [1, dx, 1/2 dx^2]. Returns (alpha, eta, window, n)."""
    lo, hi = _rank_bounds(log_center, window)
    row = spark.sql(f"""
        SELECT
            SUM(w)                  AS S0,
            SUM(w * dx)             AS S1,
            SUM(w * dx*dx)          AS S2,
            SUM(w * dx*dx*dx)       AS S3,
            SUM(w * dx*dx*dx*dx)    AS S4,
            SUM(w * z)              AS T0,
            SUM(w * dx * z)         AS T1,
            SUM(w * dx*dx * z)      AS T2,
            COUNT(*)                AS n
        FROM (
            SELECT
                LN(rank) - {log_center}   AS dx,
                LN(view_count)             AS z,
                POWER(GREATEST(
                    1.0 - POWER(
                        ABS(LN(rank) - {log_center}) / {window}, 3.0
                    ), 0.0), 3.0)          AS w
            FROM {relation}
            WHERE rank BETWEEN {lo} AND {hi}
              AND LN(rank) BETWEEN {log_center - window} AND {log_center + 1e-9}
              AND view_count > 0
        ) t
        WHERE w > 1e-15
    """).first()
    n = int(row["n"])
    if n < 10:
        raise ValueError(f"Only {n} rows in quadratic fitting window at log_r={log_center:.4f}.")
    A = np.array([
        [float(row["S0"]),       float(row["S1"]),       0.5 * float(row["S2"])],
        [float(row["S1"]),       float(row["S2"]),       0.5 * float(row["S3"])],
        [0.5 * float(row["S2"]), 0.5 * float(row["S3"]), 0.25 * float(row["S4"])],
    ])
    b = np.array([float(row["T0"]), float(row["T1"]), 0.5 * float(row["T2"])])
    beta = np.linalg.solve(A, b)
    alpha = max(-float(beta[1]), 0.1)
    eta = max(-float(beta[2]), 0.0) / alpha
    return alpha, eta, window, n


def _spark_anchor_y0(spark: SparkSession, relation: str, r0: float, window: float = ANCHOR_LOGRANK) -> Tuple[float, float]:
    """Boundary view count y0 via local linear fit on log-views near rank r0."""
    beta_0, _ = _spark_wls_linear(spark, relation, np.log(float(r0)), window)
    return float(np.exp(beta_0)), beta_0


def _spark_estimate_log_alpha_poly(spark: SparkSession, relation: str, log_r0: float,
                                   window: float = WINDOW_LOGRANK, delta: float = DELTA):
    """Polynomial-in-log-alpha coefficients from staggered local quadratics at log_r0, -delta, -2 delta."""
    alpha0, eta_0, _, n_used = _spark_wls_quadratic(spark, relation, log_r0, window)
    eta_vals = [eta_0]
    for k in range(1, 3):
        try:
            _, eta_k, _, _ = _spark_wls_quadratic(spark, relation, log_r0 - k * delta, window)
            eta_vals.append(eta_k)
        except Exception:
            eta_vals.append(eta_0)  # fallback: assume flat (as in the original notebook)
    eta1 = max((eta_vals[0] - eta_vals[1]) / delta, 0.0)
    eta2 = max((eta_vals[0] - 2 * eta_vals[1] + eta_vals[2]) / (delta ** 2), 0.0)
    return alpha0, eta_0, eta1, eta2, window, n_used


# ── Polynomial-in-log-alpha model (unchanged) ───────────────────────────────────

def _exp_from_log(log_value: float) -> float:
    return float(np.exp(np.clip(log_value, _LOG_EXP_CLIP_LOW, _LOG_EXP_CLIP_HIGH)))


def _log_alpha_poly(s: float, eta_coeffs: List[float]) -> float:
    val = 0.0
    factorial = 1.0
    for k, c in enumerate(eta_coeffs):
        factorial *= (k + 1)
        val += c * s ** (k + 1) / factorial
    return val


def _integrated_alpha(u_val: float, alpha0: float, eta_coeffs: List[float]) -> float:
    if abs(u_val) <= 1e-15:
        return 0.0

    def integrand(s):
        return alpha0 * _exp_from_log(_log_alpha_poly(s, eta_coeffs))

    if u_val > 0:
        val, _ = quad(integrand, 0, u_val, limit=200)
        return val
    val, _ = quad(integrand, u_val, 0, limit=200)
    return -val


def _alpha_at(u_val: float, alpha0: float, eta_coeffs: List[float]) -> float:
    return alpha0 * _exp_from_log(_log_alpha_poly(u_val, eta_coeffs))


def _tail_remainder_bound(u_val: float, alpha0: float, eta_coeffs: List[float]) -> float:
    alpha_u = _alpha_at(u_val, alpha0, eta_coeffs)
    if alpha_u <= 1.0 + 1e-12:
        return np.inf
    F = _integrated_alpha(u_val, alpha0, eta_coeffs)
    log_integrand = u_val - F
    if log_integrand <= _LOG_EXP_CLIP_LOW:
        return 0.0
    return _exp_from_log(log_integrand) / (alpha_u - 1.0)


def tail_mass_poly(y0, r0, alpha0, eta_coeffs):
    if all(abs(c) < 1e-12 for c in eta_coeffs):
        if alpha0 <= 1.0:
            return np.inf
        return float(y0) * float(r0) / (float(alpha0) - 1.0)

    def integrand(u):
        F = _integrated_alpha(u, alpha0, eta_coeffs)
        log_integrand = u - F
        if log_integrand <= _LOG_EXP_CLIP_LOW:
            return 0.0
        return _exp_from_log(log_integrand)

    total, u_left, step = 0.0, 0.0, 30.0
    for _ in range(12):
        u_right = u_left + step
        piece, _ = quad(integrand, u_left, u_right, limit=300)
        total += piece
        if _tail_remainder_bound(u_right, alpha0, eta_coeffs) <= 1e-10:
            return float(y0) * float(r0) * total
        u_left = u_right
        step *= 2.0
    return float(y0) * float(r0) * total


def tail_mass_pl(y0, r0, alpha0):
    if alpha0 <= 1.0:
        return np.inf
    return float(y0) * float(r0) / (float(alpha0) - 1.0)


def mass_between(y0, r0, alpha0, eta_coeffs, r_max):
    """
    Modelled views on ranks r0..r_max (added for out-of-sample checks; same model as the tail).

    With u = ln(r / r0) the fitted curve is y(u) = y0 * exp(-F(u)), F = integrated alpha, and
    mass = y0 * r0 * integral_0^U exp(u - F(u)) du with U = ln(r_max / r0).
    """
    U = math.log(float(r_max) / float(r0))
    if U <= 0:
        return 0.0
    if all(abs(c) < 1e-12 for c in eta_coeffs):
        if abs(float(alpha0) - 1.0) < 1e-12:
            return float(y0) * float(r0) * U
        a = float(alpha0)
        return float(y0) * float(r0) * (1.0 - math.exp((1.0 - a) * U)) / (a - 1.0)

    def integrand(u):
        F = _integrated_alpha(u, alpha0, eta_coeffs)
        log_integrand = u - F
        if log_integrand <= _LOG_EXP_CLIP_LOW:
            return 0.0
        return _exp_from_log(log_integrand)

    val, _ = quad(integrand, 0.0, U, limit=300)
    return float(y0) * float(r0) * val


# ── Combined fit ────────────────────────────────────────────────────────────────

@dataclass
class TailEstimates:
    r0: float
    y0: float
    alpha0: float
    eta0: float
    eta1: float
    eta2: float
    obs_total: float
    pl_tail: float
    d1_tail: float
    d2_tail: float
    d3_tail: float
    n_fit: int
    window_logrank: float

    def tail(self, model: str) -> float:
        return getattr(self, f"{model}_tail")

    def eta_coeffs(self, model: str) -> List[float]:
        """The eta coefficients each model passes to tail_mass_poly / mass_between."""
        return {"pl": [], "d1": [self.eta0], "d2": [self.eta0, self.eta1],
                "d3": [self.eta0, self.eta1, self.eta2]}[model]

    def total(self, model: str, obs_total_all: float) -> float:
        """
        Platform total = everything observed + estimated missing mass.

        missing = modelled mass beyond r0 (the tail) minus the observed mass beyond r0
        (obs_total_all - obs_total), floored at 0. Channels ranked below r0 count through their
        observed views; the tail adds only what it predicts beyond what we saw there.
        """
        t = self.tail(model)
        if not math.isfinite(t):
            return math.inf
        return obs_total_all + max(t - (obs_total_all - self.obs_total), 0.0)

    def as_dict(self) -> dict:
        return asdict(self)


def spark_fit_tail_estimates(spark: SparkSession, relation: str, r0: float, obs_total: float) -> TailEstimates:
    """
    Fit all four tail models at censoring boundary r0.

    obs_total = SUM(view_count) over rank <= r0, computed by the caller. The tail masses are
    as in the original notebook. The platform total is TailEstimates.total(model, obs_total_all):
    all observed views plus missing mass, where missing = modelled mass beyond r0 minus observed
    mass beyond r0, floored at 0. (The original notebook used obs_total + tail.)
    """
    y0, _ = _spark_anchor_y0(spark, relation, r0)
    alpha0, eta0, eta1, eta2, window, n_used = _spark_estimate_log_alpha_poly(spark, relation, np.log(float(r0)))
    return TailEstimates(
        r0=r0, y0=y0, alpha0=alpha0, eta0=eta0, eta1=eta1, eta2=eta2,
        obs_total=obs_total,
        pl_tail=tail_mass_pl(y0, r0, alpha0),
        d1_tail=tail_mass_poly(y0, r0, alpha0, [eta0]),
        d2_tail=tail_mass_poly(y0, r0, alpha0, [eta0, eta1]),
        d3_tail=tail_mass_poly(y0, r0, alpha0, [eta0, eta1, eta2]),
        n_fit=n_used, window_logrank=window,
    )
