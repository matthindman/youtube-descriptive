"""Pure helpers for the full-corpus dual-sample design.

This file intentionally has no Spark or Databricks dependency. The Databricks
notebook imports the same functions that local unit tests exercise.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any


HASH_SEPARATOR = "\x1f"


def load_design_config(path: str | Path) -> dict[str, Any]:
    """Load and validate the frozen JSON design configuration."""
    with Path(path).open(encoding="utf-8") as handle:
        config = json.load(handle)
    validate_design_config(config)
    return config


def validate_design_config(config: Mapping[str, Any]) -> None:
    """Reject incomplete or internally inconsistent design configurations."""
    required = {
        "design_version",
        "frame_version",
        "target_population",
        "source_tables",
        "output_catalog",
        "output_schema",
        "output_prefix",
        "t0_date",
        "t1_date",
        "elapsed_days",
        "subscriber_threshold",
        "samples",
        "simulation",
        "language",
        "collection",
        "topic_model",
        "topic_calibration",
        "analysis",
        "treemap",
        "execution",
    }
    missing = sorted(required - set(config))
    if missing:
        raise ValueError(f"Missing design configuration keys: {missing}")

    samples = config["samples"]
    for key in ("srs_target_n", "pps_expected_n"):
        if int(samples[key]) <= 0:
            raise ValueError(f"{key} must be positive")
    candidates = [float(value) for value in samples["alpha_candidates"]]
    selected = float(samples["selected_alpha"])
    if selected not in candidates:
        raise ValueError("selected_alpha must be present in alpha_candidates")
    if any(not 0.0 < value < 1.0 for value in candidates):
        raise ValueError("Every PPS alpha candidate must lie strictly between zero and one")
    if int(config["elapsed_days"]) <= 0:
        raise ValueError("elapsed_days must be positive")
    if int(config["subscriber_threshold"]) <= 0:
        raise ValueError("subscriber_threshold must be positive")
    simulation = config["simulation"]
    if int(simulation["final_replicates"]) < 5000:
        raise ValueError("The final repeated-sample comparison requires at least 5,000 replicates")
    pseudo_n = int(simulation["pseudo_population_max_n"])
    pseudo_top_n = int(simulation["pseudo_population_top_view_n"])
    if not 0 < pseudo_top_n < pseudo_n:
        raise ValueError("The pseudo-population top-view take-all count must lie inside its size cap")
    if samples["hash_algorithm"].lower() != "sha256":
        raise ValueError("The registered design requires SHA-256")
    if samples["hash_separator_hex"].lower() != "1f":
        raise ValueError("The registered hash separator is ASCII unit separator (0x1f)")
    if int(samples["pps_uniform_bits"]) != 64:
        raise ValueError("The registered PPS uniform uses the first 64 SHA-256 bits")

    topic_model = config["topic_model"]
    required_topic_keys = {
        "run_id",
        "model",
        "prompt_version",
        "validation_sample_n",
        "validation_seed",
        "secret_scope",
        "deepseek_secret_key",
    }
    missing_topic_keys = sorted(required_topic_keys - set(topic_model))
    if missing_topic_keys:
        raise ValueError(f"Missing topic-model configuration keys: {missing_topic_keys}")
    if int(topic_model["validation_sample_n"]) <= 0:
        raise ValueError("topic_model.validation_sample_n must be positive")

    topic_calibration = config["topic_calibration"]
    if topic_calibration.get("method") != "weighted_global_temperature":
        raise ValueError("The registered topic calibration is weighted_global_temperature")
    if int(topic_calibration["minimum_completed_channels"]) <= 0:
        raise ValueError("Topic calibration requires a positive completed-validation minimum")
    temperature_lower = float(topic_calibration["temperature_lower_bound"])
    temperature_upper = float(topic_calibration["temperature_upper_bound"])
    if not 0.0 < temperature_lower < 1.0 < temperature_upper:
        raise ValueError("Topic-calibration temperature bounds must contain one")

    collection = config["collection"]
    if int(collection["channel_batch_size"]) != 50:
        raise ValueError("YouTube channels.list requests must retain the registered 50-ID batch size")
    if not 1 <= int(collection["recent_videos_per_channel"]) <= 50:
        raise ValueError("recent_videos_per_channel must lie in [1, 50]")
    if int(collection["max_workers"]) <= 0 or int(collection["max_retries"]) < 0:
        raise ValueError("Collection worker and retry settings are invalid")

    analysis = config["analysis"]
    if analysis.get("primary_topic_variant") != "platform_only":
        raise ValueError("The primary topic variant must remain platform_only")
    if not analysis.get("model_completed_requires_calibrated_probabilities"):
        raise ValueError("Model-completed topics must require calibrated probabilities")
    lower = float(analysis["tail_total_ratio_lower_bound"])
    upper = float(analysis["tail_total_ratio_upper_bound"])
    if not 0.0 < lower <= 1.0 <= upper:
        raise ValueError("Tail total-ratio bounds must contain one and remain positive")

    treemap = config["treemap"]
    if treemap.get("primary_allocation_variant") != "platform_only":
        raise ValueError("The primary treemap allocation must remain platform_only")
    if treemap.get("primary_population_scope") not in {"known_subscriber", "all_retrievable"}:
        raise ValueError("Unsupported primary treemap population scope")
    if treemap.get("packing") != "squarify":
        raise ValueError("Treemap packing must remain squarify")
    if not 1 <= int(treemap["static_top_languages"]) <= 20:
        raise ValueError("Treemap static_top_languages must lie in [1, 20]")
    if not 1 <= int(treemap["static_cell_cap"]) <= 250:
        raise ValueError("Treemap static_cell_cap must lie in [1, 250]")
    leaf_min_frac = float(treemap["static_leaf_min_frac"])
    if not 0.003 <= leaf_min_frac <= 0.01:
        raise ValueError("Treemap static_leaf_min_frac must lie in [0.003, 0.01]")
    if int(treemap["interactive_initial_maxdepth"]) != 2:
        raise ValueError("The interactive treemap must open at maxdepth=2")

    execution = config["execution"]
    expected_execution = {
        "databricks_profile": "matt.hindman@researchaccelerator.org",
        "host": "https://adb-1335559103600339.19.azuredatabricks.net",
        "existing_cluster_id": "0601-203643-bkxsqffg",
        "sql_warehouse_id": "86100da4e1fe8713",
    }
    mismatched_execution = {
        key: (execution.get(key), expected)
        for key, expected in expected_execution.items()
        if execution.get(key) != expected
    }
    if mismatched_execution:
        raise ValueError(f"Registered Databricks execution settings changed: {mismatched_execution}")


def sha256_order_key(channel_id: str, frame_version: str, seed: str) -> str:
    """Return the registered deterministic SHA-256 order key."""
    payload = HASH_SEPARATOR.join((str(channel_id), str(frame_version), str(seed)))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sha256_uniform(channel_id: str, frame_version: str, seed: str) -> float:
    """Map the first 64 hash bits to the open interval (0, 1)."""
    key = sha256_order_key(channel_id, frame_version, seed)
    integer = int(key[:16], 16)
    return (integer + 0.5) / 2**64


def solve_capped_probabilities(q_values: Sequence[float], target_n: float) -> tuple[float, list[float]]:
    """Solve pi_i=min(1,c*q_i) for a small in-memory validation population."""
    if not q_values:
        raise ValueError("q_values must not be empty")
    if any(value <= 0 or not math.isfinite(value) for value in q_values):
        raise ValueError("q_values must be finite and strictly positive")
    if not math.isclose(sum(q_values), 1.0, rel_tol=1e-10, abs_tol=1e-12):
        raise ValueError("q_values must sum to one")
    if not 0 < target_n <= len(q_values):
        raise ValueError("target_n must be in (0, population size]")

    lower = 0.0
    upper = max(float(target_n), 1.0 / min(q_values))
    for _ in range(100):
        midpoint = (lower + upper) / 2.0
        expected = sum(min(1.0, midpoint * value) for value in q_values)
        if expected < target_n:
            lower = midpoint
        else:
            upper = midpoint
    c_value = (lower + upper) / 2.0
    probabilities = [min(1.0, c_value * value) for value in q_values]
    return c_value, probabilities


def allocate_stratified_counts(
    strata: Iterable[Mapping[str, float | int | str]],
    target_n: int,
    alpha: float,
) -> dict[str, int]:
    """Allocate an exact fixed sample across view bands using mixture mass.

    Each input row must provide ``stratum``, ``population_n``, and ``size_total``.
    Capped strata are take-all; residual slots are redistributed before largest-
    remainder rounding.
    """
    rows = [dict(row) for row in strata]
    if not rows:
        raise ValueError("At least one stratum is required")
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must lie strictly between zero and one")
    total_population = sum(int(row["population_n"]) for row in rows)
    total_size = sum(float(row["size_total"]) for row in rows)
    if not 0 < target_n <= total_population:
        raise ValueError("target_n must be in (0, population size]")
    if total_size <= 0:
        raise ValueError("The total PPS size measure must be positive")

    weights = {
        str(row["stratum"]): alpha * int(row["population_n"]) / total_population
        + (1.0 - alpha) * float(row["size_total"]) / total_size
        for row in rows
    }
    capacities = {str(row["stratum"]): int(row["population_n"]) for row in rows}
    active = set(weights)
    allocation: dict[str, float] = {key: 0.0 for key in weights}
    remaining = float(target_n)

    while active:
        active_weight = sum(weights[key] for key in active)
        if active_weight <= 0:
            equal = remaining / len(active)
            proposals = {key: equal for key in active}
        else:
            proposals = {key: remaining * weights[key] / active_weight for key in active}
        capped = [key for key in active if proposals[key] >= capacities[key]]
        if not capped:
            for key, value in proposals.items():
                allocation[key] = value
            break
        for key in capped:
            allocation[key] = float(capacities[key])
            remaining -= capacities[key]
            active.remove(key)

    integer = {key: min(capacities[key], int(math.floor(value))) for key, value in allocation.items()}
    slots = target_n - sum(integer.values())
    remainders = sorted(
        (
            (allocation[key] - integer[key], key)
            for key in integer
            if integer[key] < capacities[key]
        ),
        key=lambda item: (-item[0], item[1]),
    )
    for _, key in remainders[:slots]:
        integer[key] += 1
    if sum(integer.values()) != target_n:
        raise AssertionError("Stratified allocation did not conserve the target sample size")
    return integer


def expected_kish_effective_n(inclusion_probabilities: Sequence[float], population_n: int) -> float:
    """Approximate expected Kish effective n for inverse-probability weights."""
    if any(not 0.0 < value <= 1.0 for value in inclusion_probabilities):
        raise ValueError("Inclusion probabilities must lie in (0, 1]")
    denominator = sum(1.0 / value for value in inclusion_probabilities)
    return float(population_n) ** 2 / denominator


def srs_ht_total_variance(values: Sequence[float], population_n: int) -> tuple[float, float]:
    """Return the SRS Horvitz-Thompson total and finite-population variance.

    ``values`` are the observed sample values, including explicit zeroes. The
    variance is the standard design-unbiased SRSWOR estimator for a total.
    """
    sample_n = len(values)
    if not 1 < sample_n <= population_n:
        raise ValueError("SRS variance requires 1 < sample size <= population size")
    numeric = [float(value) for value in values]
    mean = sum(numeric) / sample_n
    sample_variance = sum((value - mean) ** 2 for value in numeric) / (sample_n - 1)
    total = float(population_n) * mean
    variance = float(population_n) ** 2 * (1.0 - sample_n / population_n) * sample_variance / sample_n
    return total, variance


def poisson_ht_total_variance(values: Sequence[float], probabilities: Sequence[float]) -> tuple[float, float]:
    """Return Poisson Horvitz-Thompson total and its unbiased variance estimate."""
    if len(values) != len(probabilities) or not values:
        raise ValueError("values and probabilities must have equal nonzero length")
    if any(not 0.0 < float(probability) <= 1.0 for probability in probabilities):
        raise ValueError("Poisson inclusion probabilities must lie in (0, 1]")
    total = sum(float(value) / float(probability) for value, probability in zip(values, probabilities))
    variance = sum(
        (1.0 - float(probability)) * float(value) ** 2 / float(probability) ** 2
        for value, probability in zip(values, probabilities)
    )
    return total, variance
