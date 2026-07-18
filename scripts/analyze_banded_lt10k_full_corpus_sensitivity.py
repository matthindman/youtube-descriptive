#!/usr/bin/env python3
"""Estimate how below-10K channels would change the language/topic treemap.

The pilot is an equal-n stratified SRS (200 channels in each 1,000-subscriber
band). Channel prevalence is post-stratified to exact frame counts. View shares
are calibrated to exact positive four-week view mass in each band. Standard
errors are conditional on the reconstructed frame and the SRS claim.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


ROOT = Path(__file__).resolve().parents[1]
RUN_DIR = ROOT / "outputs" / "banded_lt10k_full_corpus_sensitivity_20260716"
INPUT_DIR = RUN_DIR / "inputs"
BOOTSTRAP_REPS = 5_000
BOOTSTRAP_SEED = 20_260_716

HIERARCHY_PATH = ROOT / "config" / "youtube_topic_hierarchy_v2.yaml"
TOPIC_REMAP_PATH = ROOT / "config" / "topic_remap.yaml"
LANGUAGE_NORMALIZATION_PATH = ROOT / "config" / "language_normalization.yaml"
LANGUAGE_NAMES_PATH = ROOT / "config" / "iso639_language_names.csv"
THRESHOLD_TOTALS_PATH = ROOT / "artifacts" / "shorts_existing_assets_20260716" / "subscriber_threshold_28d.json"
PILOT = "dev_sean.matt.yt_lid_v3_banded_lt10k_20260716_treemap_pilot_channel_base"

UNMAPPED_FAMILY = "Other / Unmapped YouTube topic"
UNLABELED_FAMILY = "Unlabeled"
UNLABELED_LEAF = "No YouTube topicCategories"
UNDETERMINED = "Undetermined"


@dataclass
class EstimateBundle:
    columns: list[str]
    pilot_channel_hits: np.ndarray
    pilot_positive_view_hits: np.ndarray
    pilot_allocated_channel_sum: np.ndarray
    pilot_allocated_positive_views: np.ndarray
    channel_point: np.ndarray
    channel_se_linearized: np.ndarray
    channel_bootstrap: np.ndarray
    view_point: np.ndarray
    view_se_linearized: np.ndarray
    view_bootstrap: np.ndarray


def parse_bool(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes"})


def parse_array(value: object) -> list[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    text = str(value).strip()
    if not text:
        return []
    parsed = json.loads(text)
    return [str(item) for item in parsed]


def language_display_map() -> tuple[dict[str, str], dict[str, str]]:
    catalog = pd.read_csv(LANGUAGE_NAMES_PATH, dtype=str).fillna("")
    names = dict(zip(catalog["language_code"].str.lower(), catalog["display_name"]))
    cfg = yaml.safe_load(LANGUAGE_NORMALIZATION_PATH.read_text()) or {}
    overrides: dict[str, str] = {}
    for display_name, codes in (cfg.get("display_to_bases", {}) or {}).items():
        for code in codes or []:
            normalized = str(code).lower()
            if normalized in overrides and overrides[normalized] != str(display_name):
                raise ValueError(f"Conflicting language override for {normalized}")
            overrides[normalized] = str(display_name)
    return names, overrides


def display_language(code: object, names: dict[str, str], overrides: dict[str, str]) -> str:
    normalized = str(code).strip().lower()
    if normalized == "und":
        return UNDETERMINED
    if normalized in overrides:
        return overrides[normalized]
    if normalized in names:
        return names[normalized]
    return f"Unregistered code ({normalized})"


def build_topic_map() -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    hierarchy = yaml.safe_load(HIERARCHY_PATH.read_text()) or {}
    families = hierarchy.get("families", {}) or {}
    aliases = {str(k): str(v) for k, v in (hierarchy.get("aliases", {}) or {}).items()}
    remaps = (yaml.safe_load(TOPIC_REMAP_PATH.read_text()) or {}).get("unmapped_remap", {}) or {}
    mapping: dict[str, dict[str, str]] = {}

    for family, spec in families.items():
        for raw_parent in spec.get("parent_slugs") or []:
            canonical = aliases.get(str(raw_parent), str(raw_parent))
            mapping.setdefault(canonical, {
                "raw_family": str(family),
                "node_type": "parent",
                "family": str(family),
                "leaf": f"[{family}] - unspecified",
            })
        for raw_child, leaf in (spec.get("children") or {}).items():
            canonical = aliases.get(str(raw_child), str(raw_child))
            mapping.setdefault(canonical, {
                "raw_family": str(family),
                "node_type": "child",
                "family": str(family),
                "leaf": str(leaf),
            })

    for old_leaf, target in remaps.items():
        prefix = "Unmapped: "
        if not str(old_leaf).startswith(prefix):
            raise ValueError(f"Unsupported topic remap key: {old_leaf}")
        canonical = str(old_leaf)[len(prefix):]
        prior = mapping.get(canonical)
        mapping[canonical] = {
            "raw_family": prior["raw_family"] if prior else UNMAPPED_FAMILY,
            "node_type": prior["node_type"] if prior else "unmapped",
            "family": str(target["family"]),
            "leaf": str(target["leaf"]),
        }
    return mapping, aliases


def topic_slug(value: str) -> str:
    without_suffix = re.sub(r"[?#].*$", "", value.strip())
    without_slash = re.sub(r"/+$", "", without_suffix)
    return re.sub(r"\s+", "_", without_slash.rsplit("/", 1)[-1]).lower()


def project_topics(
    raw_topics: list[str], mapping: dict[str, dict[str, str]], aliases: dict[str, str]
) -> list[tuple[str, str, float]]:
    canonical_slugs = sorted({aliases.get(topic_slug(item), topic_slug(item)) for item in raw_topics if item})
    candidates: list[dict[str, str]] = []
    for slug in canonical_slugs:
        item = mapping.get(slug)
        if item is None:
            item = {
                "raw_family": UNMAPPED_FAMILY,
                "node_type": "unmapped",
                "family": UNMAPPED_FAMILY,
                "leaf": f"Unmapped: {slug}",
            }
        candidates.append(item)

    child_families = {item["raw_family"] for item in candidates if item["node_type"] == "child"}
    displayed = {
        (item["family"], item["leaf"])
        for item in candidates
        if item["node_type"] != "parent" or item["raw_family"] not in child_families
    }
    if not displayed:
        displayed = {(UNLABELED_FAMILY, UNLABELED_LEAF)}

    by_family: dict[str, list[str]] = {}
    for family, leaf in sorted(displayed):
        by_family.setdefault(family, []).append(leaf)
    family_count = len(by_family)
    return [
        (family, leaf, 1.0 / family_count / len(leaves))
        for family, leaves in by_family.items()
        for leaf in leaves
    ]


def one_hot(values: pd.Series) -> pd.DataFrame:
    columns = sorted(values.unique())
    matrix = pd.DataFrame(0.0, index=values.index, columns=columns)
    for column in columns:
        matrix.loc[values == column, column] = 1.0
    return matrix


def topic_matrices(projections: list[list[tuple[str, str, float]]], index: pd.Index) -> tuple[pd.DataFrame, pd.DataFrame]:
    families = sorted({family for items in projections for family, _leaf, _weight in items})
    leaves = sorted({f"{family} > {leaf}" for items in projections for family, leaf, _weight in items})
    family_matrix = pd.DataFrame(0.0, index=index, columns=families)
    leaf_matrix = pd.DataFrame(0.0, index=index, columns=leaves)
    for row_index, items in zip(index, projections):
        for family, leaf, weight in items:
            family_matrix.at[row_index, family] += weight
            leaf_matrix.at[row_index, f"{family} > {leaf}"] += weight
    if not np.allclose(family_matrix.sum(axis=1), 1.0):
        raise AssertionError("Family weights do not sum to one")
    if not np.allclose(leaf_matrix.sum(axis=1), 1.0):
        raise AssertionError("Leaf weights do not sum to one")
    return family_matrix, leaf_matrix


def estimate_stratified(
    pilot: pd.DataFrame,
    matrix: pd.DataFrame,
    margins: pd.DataFrame,
    rng: np.random.Generator,
) -> EstimateBundle:
    columns = list(matrix.columns)
    k = len(columns)
    channel_point = np.zeros(k)
    view_point = np.zeros(k)
    channel_var = np.zeros(k)
    view_var = np.zeros(k)
    channel_boot = np.zeros((BOOTSTRAP_REPS, k))
    view_boot = np.zeros((BOOTSTRAP_REPS, k))

    total_channels = float(margins["frame_channels"].sum())
    total_views = float(margins["positive_view_change"].sum())
    for margin in margins.itertuples(index=False):
        band = int(margin.sample_band)
        row_mask = pilot["sample_band"] == band
        x = matrix.loc[row_mask].to_numpy(dtype=float)
        y = pilot.loc[row_mask, "analysis_views"].to_numpy(dtype=float)
        n = len(y)
        if n != 200:
            raise AssertionError(f"Band {band} has {n} rows, expected 200")

        n_frame = float(margin.frame_channels)
        v_frame = float(margin.positive_view_change)
        weight_channels = n_frame / total_channels
        weight_views = v_frame / total_views
        sampling_fraction = n / n_frame

        p_h = x.mean(axis=0)
        channel_point += weight_channels * p_h
        channel_var += (
            weight_channels**2
            * (1.0 - sampling_fraction)
            * x.var(axis=0, ddof=1)
            / n
        )

        y_total = float(y.sum())
        if y_total <= 0:
            raise AssertionError(f"Band {band} has no sampled positive view mass")
        r_h = (y[:, None] * x).sum(axis=0) / y_total
        view_point += weight_views * r_h
        residual = y[:, None] * (x - r_h)
        y_bar = y.mean()
        view_var += (
            weight_views**2
            * (1.0 - sampling_fraction)
            * residual.var(axis=0, ddof=1)
            / n
            / (y_bar**2)
        )

        replicate_counts = rng.multinomial(n, np.full(n, 1.0 / n), size=BOOTSTRAP_REPS)
        channel_boot += weight_channels * (replicate_counts @ x) / n
        replicate_y = replicate_counts @ y
        replicate_numerator = replicate_counts @ (y[:, None] * x)
        replicate_ratio = np.divide(
            replicate_numerator,
            replicate_y[:, None],
            out=np.tile(r_h, (BOOTSTRAP_REPS, 1)),
            where=replicate_y[:, None] > 0,
        )
        view_boot += weight_views * replicate_ratio

    return EstimateBundle(
        columns=columns,
        pilot_channel_hits=(matrix.to_numpy(dtype=float) > 0).sum(axis=0),
        pilot_positive_view_hits=(
            (matrix.to_numpy(dtype=float) > 0)
            & (pilot["analysis_views"].to_numpy(dtype=float)[:, None] > 0)
        ).sum(axis=0),
        pilot_allocated_channel_sum=matrix.to_numpy(dtype=float).sum(axis=0),
        pilot_allocated_positive_views=(
            matrix.to_numpy(dtype=float)
            * pilot["analysis_views"].to_numpy(dtype=float)[:, None]
        ).sum(axis=0),
        channel_point=channel_point,
        channel_se_linearized=np.sqrt(np.maximum(channel_var, 0)),
        channel_bootstrap=channel_boot,
        view_point=view_point,
        view_se_linearized=np.sqrt(np.maximum(view_var, 0)),
        view_bootstrap=view_boot,
    )


def threshold_totals() -> dict[str, float]:
    payload = json.loads(THRESHOLD_TOTALS_PATH.read_text())
    rows = payload["rows"]
    below = [row for row in rows if row["subscriber_band"] <= "005000-009999"]
    above = [
        row for row in rows
        if row["subscriber_band"] not in {"unknown"}
        and row["subscriber_band"] >= "010000-049999"
    ]
    return {
        "below_channels": sum(int(row["n_t0_channels"]) for row in below),
        "above_channels": sum(int(row["n_t0_channels"]) for row in above),
        "below_views": sum(float(row["positive_view_change"]) for row in below),
        "above_views": sum(float(row["positive_view_change"]) for row in above),
        "unknown_channels": sum(int(row["n_t0_channels"]) for row in rows if row["subscriber_band"] == "unknown"),
        "unknown_views": sum(float(row["positive_view_change"]) for row in rows if row["subscriber_band"] == "unknown"),
    }


def baseline_series(kind: str, estimand: str) -> pd.Series:
    if kind == "language":
        if estimand == "view":
            frame = pd.read_csv(INPUT_DIR / "baseline_language.csv")
            return frame.set_index("language_display")["positive_view_change"].astype(float)
        frame = pd.read_csv(INPUT_DIR / "baseline_language_channels.csv")
        return frame.set_index("language_display")["allocated_channel_count"].astype(float)

    filename = "baseline_family_leaf.csv" if estimand == "view" else "baseline_family_leaf_channels.csv"
    frame = pd.read_csv(INPUT_DIR / filename)
    value_column = "positive_view_change" if estimand == "view" else "allocated_channel_count"
    if kind == "family":
        return frame.groupby("yt_family")[value_column].sum().astype(float)
    keys = frame["yt_family"].astype(str) + " > " + frame["yt_leaf"].astype(str)
    return pd.Series(frame[value_column].astype(float).to_numpy(), index=keys)


def result_table(kind: str, bundle: EstimateBundle, totals: dict[str, float]) -> pd.DataFrame:
    baseline_view = baseline_series(kind, "view")
    baseline_channels = baseline_series(kind, "channel")
    all_categories = sorted(set(baseline_view.index) | set(bundle.columns))
    bundle_index = {name: i for i, name in enumerate(bundle.columns)}

    exact_above_views = totals["above_views"]
    exact_below_views = totals["below_views"]
    exact_all_views = exact_above_views + exact_below_views
    exact_above_channels = totals["above_channels"]
    exact_below_channels = totals["below_channels"]
    exact_all_channels = exact_above_channels + exact_below_channels

    baseline_view_share = baseline_view / baseline_view.sum()
    baseline_channel_share = baseline_channels / baseline_channels.sum()
    view_below_weight = exact_below_views / exact_all_views
    channel_below_weight = exact_below_channels / exact_all_channels

    rows = []
    for category in all_categories:
        observed = category in bundle_index
        if observed:
            i = bundle_index[category]
            pilot_channel_hits = int(bundle.pilot_channel_hits[i])
            pilot_positive_view_hits = int(bundle.pilot_positive_view_hits[i])
            pilot_allocated_channel_sum = float(bundle.pilot_allocated_channel_sum[i])
            pilot_allocated_positive_views = float(bundle.pilot_allocated_positive_views[i])
            below_channel = bundle.channel_point[i]
            below_channel_se_lin = bundle.channel_se_linearized[i]
            below_channel_boot = bundle.channel_bootstrap[:, i]
            below_view = bundle.view_point[i]
            below_view_se_lin = bundle.view_se_linearized[i]
            below_view_boot = bundle.view_bootstrap[:, i]
        else:
            pilot_channel_hits = pilot_positive_view_hits = 0
            pilot_allocated_channel_sum = pilot_allocated_positive_views = 0.0
            below_channel = below_channel_se_lin = below_view = below_view_se_lin = 0.0
            below_channel_boot = np.zeros(BOOTSTRAP_REPS)
            below_view_boot = np.zeros(BOOTSTRAP_REPS)

        current_view = float(baseline_view_share.get(category, 0.0))
        current_channel = float(baseline_channel_share.get(category, 0.0))
        full_view_boot = (1.0 - view_below_weight) * current_view + view_below_weight * below_view_boot
        full_channel_boot = (
            (1.0 - channel_below_weight) * current_channel
            + channel_below_weight * below_channel_boot
        )
        full_view = (1.0 - view_below_weight) * current_view + view_below_weight * below_view
        full_channel = (
            (1.0 - channel_below_weight) * current_channel
            + channel_below_weight * below_channel
        )
        rows.append({
            "category": category,
            "observed_in_pilot": observed,
            "pilot_channel_hits": pilot_channel_hits,
            "pilot_positive_view_hits": pilot_positive_view_hits,
            "pilot_allocated_channel_sum": pilot_allocated_channel_sum,
            "pilot_allocated_positive_views": pilot_allocated_positive_views,
            "below_channel_share": below_channel,
            "below_channel_se_linearized": below_channel_se_lin,
            "below_channel_se_bootstrap": float(np.std(below_channel_boot, ddof=1)),
            "below_channel_ci_low": float(np.quantile(below_channel_boot, 0.025)),
            "below_channel_ci_high": float(np.quantile(below_channel_boot, 0.975)),
            "below_view_share": below_view,
            "below_view_se_linearized": below_view_se_lin,
            "below_view_se_bootstrap": float(np.std(below_view_boot, ddof=1)),
            "below_view_ci_low": float(np.quantile(below_view_boot, 0.025)),
            "below_view_ci_high": float(np.quantile(below_view_boot, 0.975)),
            "current_above10k_channel_share": current_channel,
            "projected_full_channel_share": full_channel,
            "channel_share_change_pp": 100.0 * (full_channel - current_channel),
            "projected_full_channel_se": channel_below_weight * below_channel_se_lin,
            "projected_full_channel_se_bootstrap": float(np.std(full_channel_boot, ddof=1)),
            "projected_full_channel_ci_low": float(np.quantile(full_channel_boot, 0.025)),
            "projected_full_channel_ci_high": float(np.quantile(full_channel_boot, 0.975)),
            "channel_share_change_ci_low_pp": 100.0 * (
                float(np.quantile(full_channel_boot, 0.025)) - current_channel
            ),
            "channel_share_change_ci_high_pp": 100.0 * (
                float(np.quantile(full_channel_boot, 0.975)) - current_channel
            ),
            "current_above10k_view_share": current_view,
            "projected_full_view_share": full_view,
            "view_share_change_pp": 100.0 * (full_view - current_view),
            "projected_full_view_se": view_below_weight * below_view_se_lin,
            "projected_full_view_se_bootstrap": float(np.std(full_view_boot, ddof=1)),
            "projected_full_view_ci_low": float(np.quantile(full_view_boot, 0.025)),
            "projected_full_view_ci_high": float(np.quantile(full_view_boot, 0.975)),
            "view_share_change_ci_low_pp": 100.0 * (
                float(np.quantile(full_view_boot, 0.025)) - current_view
            ),
            "view_share_change_ci_high_pp": 100.0 * (
                float(np.quantile(full_view_boot, 0.975)) - current_view
            ),
        })
    return pd.DataFrame(rows).sort_values("projected_full_view_share", ascending=False)


def band_diagnostics(pilot: pd.DataFrame, margins: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for margin in margins.itertuples(index=False):
        band = int(margin.sample_band)
        subset = pilot.loc[pilot["sample_band"] == band]
        y = subset["analysis_views"].to_numpy(dtype=float)
        y_total = y.sum()
        rows.append({
            "sample_band": band,
            "subscriber_range": f"{band * 1000:,}-{band * 1000 + 999:,}",
            "sample_channels": len(subset),
            "frame_channels_2026_06_15": int(margin.frame_channels),
            "sampling_fraction": len(subset) / float(margin.frame_channels),
            "exact_positive_view_change": float(margin.positive_view_change),
            "sample_positive_view_change": y_total,
            "sample_positive_channels": int((y > 0).sum()),
            "sample_view_effective_n": float(y_total**2 / np.square(y).sum()) if np.square(y).sum() else 0.0,
            "largest_sample_channel_view_share": float(y.max() / y_total) if y_total else 0.0,
            "language_classified_rate": float((subset["language_display"] != UNDETERMINED).mean()),
            "nonempty_topic_rate": float(subset["has_nonempty_topic_categories"].mean()),
            "valid_traffic_rate": float(subset["has_valid_4wk_views"].mean()),
            "sample_band_differs_from_prior_snapshot": int(
                (
                    np.floor(pd.to_numeric(subset["prior_subscriber_count"], errors="coerce") / 1000)
                    != band
                ).fillna(True).sum()
            ),
        })
    return pd.DataFrame(rows)


def main() -> None:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    pilot = pd.read_csv(INPUT_DIR / "pilot_rows.csv")
    margins = pd.read_csv(INPUT_DIR / "band_margins_1k.csv")
    numeric_columns = [
        "sampled_subscriber_count",
        "sample_band",
        "prior_subscriber_count",
        "current_subscriber_count",
        "raw_4wk_views",
        "view_count_4wk",
    ]
    for column in numeric_columns:
        pilot[column] = pd.to_numeric(pilot[column], errors="coerce")
    for column in [
        "is_language_classified",
        "is_mixed_language",
        "is_script_ambiguous",
        "has_valid_4wk_views",
        "has_invalid_negative_delta",
        "topic_row_present",
        "has_nonempty_topic_categories",
    ]:
        pilot[column] = parse_bool(pilot[column])
    pilot["analysis_views"] = pilot["view_count_4wk"].fillna(0.0).clip(lower=0.0)
    pilot["raw_topic_categories"] = pilot["raw_topic_categories"].map(parse_array)

    names, overrides = language_display_map()
    pilot["language_display"] = pilot["channel_language"].map(
        lambda code: display_language(code, names, overrides)
    )
    topic_map, aliases = build_topic_map()
    projections = [project_topics(topics, topic_map, aliases) for topics in pilot["raw_topic_categories"]]

    language_matrix = one_hot(pilot["language_display"])
    family_matrix, leaf_matrix = topic_matrices(projections, pilot.index)
    combined = pd.concat(
        [
            language_matrix.add_prefix("language::"),
            family_matrix.add_prefix("family::"),
            leaf_matrix.add_prefix("leaf::"),
        ],
        axis=1,
    )

    rng = np.random.default_rng(BOOTSTRAP_SEED)
    combined_bundle = estimate_stratified(pilot, combined, margins, rng)

    def subset_bundle(prefix: str) -> EstimateBundle:
        indices = [i for i, name in enumerate(combined_bundle.columns) if name.startswith(prefix)]
        return EstimateBundle(
            columns=[combined_bundle.columns[i][len(prefix):] for i in indices],
            pilot_channel_hits=combined_bundle.pilot_channel_hits[indices],
            pilot_positive_view_hits=combined_bundle.pilot_positive_view_hits[indices],
            pilot_allocated_channel_sum=combined_bundle.pilot_allocated_channel_sum[indices],
            pilot_allocated_positive_views=combined_bundle.pilot_allocated_positive_views[indices],
            channel_point=combined_bundle.channel_point[indices],
            channel_se_linearized=combined_bundle.channel_se_linearized[indices],
            channel_bootstrap=combined_bundle.channel_bootstrap[:, indices],
            view_point=combined_bundle.view_point[indices],
            view_se_linearized=combined_bundle.view_se_linearized[indices],
            view_bootstrap=combined_bundle.view_bootstrap[:, indices],
        )

    totals = threshold_totals()
    margin_channels = int(margins["frame_channels"].sum())
    margin_views = float(margins["positive_view_change"].sum())
    if margin_channels != int(totals["below_channels"]):
        raise AssertionError("Detailed and broad below-10K channel margins disagree")
    if not math.isclose(margin_views, totals["below_views"], rel_tol=1e-12, abs_tol=1.0):
        raise AssertionError("Detailed and broad below-10K view margins disagree")

    outputs = {
        "language": result_table("language", subset_bundle("language::"), totals),
        "family": result_table("family", subset_bundle("family::"), totals),
        "leaf": result_table("leaf", subset_bundle("leaf::"), totals),
    }
    for kind, frame in outputs.items():
        frame.to_csv(RUN_DIR / f"{kind}_estimates.csv", index=False)

    diagnostics = band_diagnostics(pilot, margins)
    diagnostics.to_csv(RUN_DIR / "band_diagnostics.csv", index=False)

    calibrated_view_weights: list[float] = []
    for margin in margins.itertuples(index=False):
        subset = pilot.loc[pilot["sample_band"] == int(margin.sample_band), "analysis_views"]
        within_band = subset.to_numpy(dtype=float)
        calibrated_view_weights.extend(
            (
                float(margin.positive_view_change)
                / margin_views
                * within_band
                / within_band.sum()
            ).tolist()
        )
    calibrated_view_weights_array = np.asarray(calibrated_view_weights)

    manifest = {
        "analysis_version": "banded_lt10k_full_corpus_sensitivity_20260716_v1",
        "bootstrap_replicates": BOOTSTRAP_REPS,
        "bootstrap_seed": BOOTSTRAP_SEED,
        "pilot_rows": len(pilot),
        "sample_channels_per_band": pilot.groupby("sample_band").size().astype(int).to_dict(),
        "frame_below10k_channels": margin_channels,
        "frame_above10k_channels": int(totals["above_channels"]),
        "frame_below10k_positive_views": margin_views,
        "frame_above10k_positive_views": totals["above_views"],
        "below10k_channel_share": margin_channels / (margin_channels + totals["above_channels"]),
        "below10k_positive_view_share": margin_views / (margin_views + totals["above_views"]),
        "pilot_calibrated_view_effective_n": float(
            calibrated_view_weights_array.sum() ** 2
            / np.square(calibrated_view_weights_array).sum()
        ),
        "pilot_largest_calibrated_view_weight": float(calibrated_view_weights_array.max()),
        "unknown_subscriber_channels_excluded": int(totals["unknown_channels"]),
        "unknown_subscriber_positive_views_excluded": totals["unknown_views"],
        "frame_snapshot": "2026-06-15",
        "traffic_current_snapshot": "2026-07-13",
        "traffic_estimand": "positive accepted 28-day lifetime-view delta; negative revisions contribute zero mass",
        "language_classified": int((pilot["language_display"] != UNDETERMINED).sum()),
        "language_undetermined": int((pilot["language_display"] == UNDETERMINED).sum()),
        "topic_nonempty": int(pilot["has_nonempty_topic_categories"].sum()),
        "valid_traffic": int(pilot["has_valid_4wk_views"].sum()),
        "baseline_projection_view_coverage": float(baseline_series("language", "view").sum() / totals["above_views"]),
        "baseline_projection_channel_coverage": float(baseline_series("language", "channel").sum() / totals["above_channels"]),
        "estimands": {
            "channel": "post-stratified SRS share using exact 2026-06-15 frame counts",
            "view": "within-band ratio share calibrated to exact positive 2026-06-15/2026-07-13 view mass",
            "full": "exact above/below threshold margins; labelled >=10K composition scaled to exact >=10K total",
        },
        "source_tables": {
            "pilot": PILOT,
            "stats": "dev_sean.default.yt_channel_stats_full",
            "baseline_projection": "dev_sean.matt.yt_treemap_full_corpus_lid_v3_20260715_v1_topic_projection",
        },
        "input_statement_ids": {
            path.stem: json.loads(path.read_text())["statement_id"]
            for path in sorted(INPUT_DIR.glob("*.json"))
        },
    }
    (RUN_DIR / "analysis_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    for kind, frame in outputs.items():
        share_sum = frame["projected_full_view_share"].sum()
        if not math.isclose(share_sum, 1.0, rel_tol=1e-9, abs_tol=1e-9):
            raise AssertionError(f"{kind} projected full view shares sum to {share_sum}")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
