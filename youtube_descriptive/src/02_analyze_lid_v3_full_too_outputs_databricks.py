# Databricks notebook source
# MAGIC %md
# MAGIC # Analyze full TOO LID v3 outputs
# MAGIC
# MAGIC Reads a completed full top-of-ocean LID v3 output family, writes compact evaluation tables, and stores
# MAGIC figures as SVG text in Delta so the run can be reviewed without depending on local driver files.

# COMMAND ----------
from datetime import datetime, timezone
from io import StringIO
import json
import os
import re
from typing import Iterable, List, Optional

from pyspark.sql import functions as F

# COMMAND ----------
def _create_text_widget(name: str, default: str) -> None:
    try:
        dbutils.widgets.text(name, default, name)
    except Exception:
        pass


def _get_widget(name: str, default: str) -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value is not None and value != "" else default
    except Exception:
        return os.environ.get(name.upper(), default)


def _get_bool_widget(name: str, default: bool) -> bool:
    raw = _get_widget(name, str(default)).strip().lower()
    return raw in {"1", "true", "t", "yes", "y"}


def _get_int_widget(name: str, default: int) -> int:
    raw = _get_widget(name, str(default)).strip()
    return int(raw) if raw else default


def _safe_token(raw: str, default: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_]", "_", (raw or "").strip())
    token = re.sub(r"_+", "_", token).strip("_")
    return token or default


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _fqtn(catalog: str, schema: str, table: str) -> str:
    return f"{_quote(catalog)}.{_quote(schema)}.{_quote(table)}"


def _table_exists(catalog: str, schema: str, table: str) -> bool:
    try:
        spark.table(_fqtn(catalog, schema, table)).limit(0).count()
        return True
    except Exception:
        return False


def _current_run(df):
    return df.where(F.col("run_id") == F.lit(RUN_ID)) if "run_id" in df.columns else df


def _overwrite_delta(df, table_full: str, partition_cols: Optional[Iterable[str]] = None) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(table_full)


def _save_table(df, table_name: str, partition_cols: Optional[Iterable[str]] = None) -> str:
    full = _fqtn(OUTPUT_CATALOG, OUTPUT_SCHEMA, table_name)
    _overwrite_delta(df, full, partition_cols=partition_cols)
    print("Wrote", full)
    return full


def _rows(df, limit: int = 100) -> List[dict]:
    return [r.asDict(recursive=True) for r in df.limit(limit).collect()]

# COMMAND ----------
_create_text_widget("output_catalog", "dev_sean")
_create_text_widget("output_schema", "matt")
_create_text_widget("output_prefix", "yt_lid_v3_too_full")
_create_text_widget("run_id", "too_full")
_create_text_widget("top_language_n", "30")
_create_text_widget("review_queue_limit", "5000")
_create_text_widget("count_large_tables", "false")

OUTPUT_CATALOG = _get_widget("output_catalog", "dev_sean")
OUTPUT_SCHEMA = _get_widget("output_schema", "matt")
OUTPUT_PREFIX = _safe_token(_get_widget("output_prefix", "yt_lid_v3_too_full"), "yt_lid_v3_too_full")
RUN_ID = _get_widget("run_id", "too_full")
TOP_LANGUAGE_N = _get_int_widget("top_language_n", 30)
REVIEW_QUEUE_LIMIT = _get_int_widget("review_queue_limit", 5000)
COUNT_LARGE_TABLES = _get_bool_widget("count_large_tables", False)

ANALYSIS_PREFIX = f"{OUTPUT_PREFIX}_analysis"

TABLE_SUFFIXES = {
    "segments_input": "segments_input",
    "openlid_compact": "openlid_predictions_compact",
    "glotlid_compact": "glotlid_predictions_compact",
    "channel_text_features": "channel_text_features",
    "segment_model_comparison": "segment_model_comparison",
    "channel_votes": "channel_votes",
    "channel_model_aggregation": "channel_model_aggregation",
    "channel_model_comparison": "channel_model_comparison",
    "channels": "channels",
    "language_summary_full": "language_summary_full",
    "language_summary_rollup": "language_summary_rollup",
    "model_agreement_summary": "model_agreement_summary",
    "mixed_language_candidates": "mixed_language_candidates",
    "hindi_indic_audit_candidates": "hindi_indic_audit_candidates",
    "suspect_tail_audit_sample": "suspect_tail_audit_sample",
    "high_risk_redirect_diagnostic": "high_risk_redirect_diagnostic",
    "manual_validation_sample": "manual_validation_sample",
    "unclassified_audit": "unclassified_audit",
    "source_language_confusion": "source_language_confusion",
    "dedupe_qa": "dedupe_qa",
    "preflight_estimate": "preflight_estimate",
    "ablation_summary": "ablation_summary",
    "run_progress": "run_progress",
}

LARGE_TABLE_KEYS = {"segments_input", "openlid_compact", "glotlid_compact", "segment_model_comparison"}

print("Output family:", f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{OUTPUT_PREFIX}_*")
print("Run id:", RUN_ID)
print("Count large tables:", COUNT_LARGE_TABLES)

# COMMAND ----------
missing = [
    f"{OUTPUT_PREFIX}_{suffix}"
    for suffix in ["channels", "language_summary_full", "language_summary_rollup", "model_agreement_summary", "preflight_estimate"]
    if not _table_exists(OUTPUT_CATALOG, OUTPUT_SCHEMA, f"{OUTPUT_PREFIX}_{suffix}")
]
if missing:
    raise ValueError(f"Missing required LID output tables: {missing}")

tables = {
    key: spark.table(_fqtn(OUTPUT_CATALOG, OUTPUT_SCHEMA, f"{OUTPUT_PREFIX}_{suffix}"))
    for key, suffix in TABLE_SUFFIXES.items()
    if _table_exists(OUTPUT_CATALOG, OUTPUT_SCHEMA, f"{OUTPUT_PREFIX}_{suffix}")
}

channels = _current_run(tables["channels"]).cache()

# COMMAND ----------
table_count_rows = []
for key, suffix in TABLE_SUFFIXES.items():
    table_name = f"{OUTPUT_PREFIX}_{suffix}"
    full = f"{OUTPUT_CATALOG}.{OUTPUT_SCHEMA}.{table_name}"
    exists = key in tables
    if not exists:
        table_count_rows.append((key, full, False, None, "missing"))
    elif key in LARGE_TABLE_KEYS and not COUNT_LARGE_TABLES:
        table_count_rows.append((key, full, True, None, "not_counted_by_default"))
    else:
        table_count_rows.append((key, full, True, _current_run(tables[key]).count(), "counted"))

table_counts = spark.createDataFrame(
    table_count_rows,
    "table_key string, table_name string, table_exists boolean, n_rows long, count_status string",
).withColumn("run_id", F.lit(RUN_ID)).withColumn("analysis_timestamp", F.current_timestamp())
table_counts_full = _save_table(table_counts, f"{ANALYSIS_PREFIX}_table_counts")

preflight = _current_run(tables["preflight_estimate"]).withColumn("analysis_timestamp", F.current_timestamp())
preflight_full = _save_table(preflight, f"{ANALYSIS_PREFIX}_preflight_estimate")

if "run_progress" in tables:
    progress = (
        _current_run(tables["run_progress"])
        .groupBy("stage", "status", "metric")
        .agg(F.max("event_timestamp").alias("latest_event_timestamp"), F.max("value").alias("latest_value"))
        .orderBy("latest_event_timestamp", "stage", "metric")
        .withColumn("analysis_timestamp", F.current_timestamp())
    )
else:
    progress = spark.createDataFrame([], "stage string, status string, metric string, latest_event_timestamp timestamp, latest_value string")
progress_full = _save_table(progress, f"{ANALYSIS_PREFIX}_run_progress_latest")

# COMMAND ----------
status_summary = (
    channels
    .groupBy("language_status", "consensus_status", "requires_manual_adjudication")
    .agg(F.count(F.lit(1)).alias("n_channels"))
    .orderBy(F.desc("n_channels"), "language_status", "consensus_status")
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("analysis_timestamp", F.current_timestamp())
)
status_summary_full = _save_table(status_summary, f"{ANALYSIS_PREFIX}_status_summary")

language_summary = (
    channels
    .groupBy(
        "consensus_language_label",
        "consensus_language_iso639_3",
        "consensus_for_rollup_label",
        "requires_manual_adjudication",
    )
    .agg(
        F.count(F.lit(1)).alias("n_channels"),
        F.avg("openlid_primary_language_vote_share_with_top2").alias("mean_openlid_vote_share"),
        F.avg("glotlid_primary_language_vote_share_with_top2").alias("mean_glotlid_vote_share"),
        F.sum(F.coalesce(F.col("is_mixed_language_candidate"), F.lit(False)).cast("int")).alias("n_mixed_language_candidate"),
    )
    .orderBy(F.desc("n_channels"), "consensus_language_label")
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("analysis_timestamp", F.current_timestamp())
)
language_summary_full = _save_table(language_summary, f"{ANALYSIS_PREFIX}_consensus_language_summary")

top_languages = language_summary.limit(TOP_LANGUAGE_N)
top_languages_full = _save_table(top_languages, f"{ANALYSIS_PREFIX}_top_languages")

review_queue = (
    channels
    .where(
        F.coalesce(F.col("requires_manual_adjudication"), F.lit(False))
        | (F.col("language_status") != F.lit("classified"))
        | F.coalesce(F.col("is_mixed_language_candidate"), F.lit(False))
        | (F.col("hindi_indic_candidate_status") != F.lit("no_hindi_or_indic_signal"))
    )
    .select(
        "run_id",
        "channel_id",
        "language_status",
        "consensus_status",
        "consensus_language_label",
        "consensus_for_rollup_label",
        "requires_manual_adjudication",
        "is_mixed_language_candidate",
        "hindi_indic_candidate_status",
        "openlid_primary_language_label",
        "openlid_primary_language_vote_share_with_top2",
        "glotlid_primary_language_label",
        "glotlid_primary_language_vote_share_with_top2",
        "openlid_primary_is_high_risk",
        "glotlid_primary_is_high_risk",
    )
    .orderBy(F.desc("requires_manual_adjudication"), "consensus_status", "channel_id")
    .limit(REVIEW_QUEUE_LIMIT)
    .withColumn("analysis_timestamp", F.current_timestamp())
)
review_queue_full = _save_table(review_queue, f"{ANALYSIS_PREFIX}_review_queue_sample")

model_agreement = (
    _current_run(tables["model_agreement_summary"])
    .orderBy(F.desc("n_channels"), "openlid_primary_language_iso639_3")
    .withColumn("analysis_timestamp", F.current_timestamp())
)
model_agreement_full = _save_table(model_agreement, f"{ANALYSIS_PREFIX}_model_agreement_summary")

# COMMAND ----------
figure_rows = []


def _add_figure_from_pandas(name: str, pdf, plot_fn) -> None:
    created_at = datetime.now(timezone.utc).isoformat()
    try:
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(10, 6))
        plot_fn(pdf, ax)
        fig.tight_layout()
        buf = StringIO()
        fig.savefig(buf, format="svg")
        plt.close(fig)
        figure_rows.append((RUN_ID, name, buf.getvalue(), "ok", "", created_at))
    except Exception as exc:
        figure_rows.append((RUN_ID, name, "", "error", " ".join(str(exc).split())[:500], created_at))


status_pdf = status_summary.select("language_status", "consensus_status", "n_channels").toPandas()
_add_figure_from_pandas(
    "status_summary.svg",
    status_pdf,
    lambda pdf, ax: (
        pdf.assign(label=pdf["language_status"].fillna("null") + " / " + pdf["consensus_status"].fillna("null"))
        .head(20)
        .sort_values("n_channels")
        .plot.barh(x="label", y="n_channels", ax=ax, legend=False, title="Top LID status combinations")
    ),
)

top_lang_pdf = top_languages.select("consensus_language_label", "consensus_for_rollup_label", "n_channels").toPandas()
_add_figure_from_pandas(
    "top_consensus_languages.svg",
    top_lang_pdf,
    lambda pdf, ax: (
        pdf.assign(label=pdf["consensus_language_label"].fillna(pdf["consensus_for_rollup_label"]).fillna("null"))
        .sort_values("n_channels")
        .plot.barh(x="label", y="n_channels", ax=ax, legend=False, title="Top consensus languages")
    ),
)

agreement_cols = [c for c in ["exact_agreement_rate", "iso_agreement_rate", "cluster_agreement_rate"] if c in model_agreement.columns]
if agreement_cols:
    agreement_pdf = model_agreement.select(*agreement_cols).limit(1).toPandas()
    _add_figure_from_pandas(
        "overall_model_agreement_rates.svg",
        agreement_pdf,
        lambda pdf, ax: (
            pdf.iloc[0]
            .rename({
                "exact_agreement_rate": "Exact",
                "iso_agreement_rate": "ISO",
                "cluster_agreement_rate": "Cluster",
            })
            .plot.bar(ax=ax, title="Overall model agreement rates")
        ),
    )

figures = spark.createDataFrame(
    figure_rows,
    "run_id string, file_name string, svg_text string, status string, message string, created_at_utc string",
).withColumn("analysis_timestamp", F.current_timestamp())
figures_full = _save_table(figures, f"{ANALYSIS_PREFIX}_figures_svg")

# COMMAND ----------
summary = {
    "output_prefix": OUTPUT_PREFIX,
    "run_id": RUN_ID,
    "tables": {
        "table_counts": table_counts_full,
        "preflight": preflight_full,
        "progress": progress_full,
        "status_summary": status_summary_full,
        "language_summary": language_summary_full,
        "top_languages": top_languages_full,
        "review_queue": review_queue_full,
        "model_agreement": model_agreement_full,
        "figures": figures_full,
    },
    "channel_count": channels.count(),
    "top_languages": _rows(top_languages, TOP_LANGUAGE_N),
    "figures": [{"file_name": r[1], "status": r[3], "chars": len(r[2] or "")} for r in figure_rows],
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
