# Databricks notebook source
# MAGIC %md
# MAGIC # Analyze LID v3 10k validation outputs
# MAGIC
# MAGIC Reads the saved LID v3 output table family for one run, joins it back to the
# MAGIC deterministic validation cohort, writes compact analysis tables, and saves
# MAGIC plot artifacts as SVG text in a Delta table.

# COMMAND ----------
from datetime import datetime, timezone
from io import StringIO
import json
import os
import re
from typing import Dict, Iterable, List, Optional

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

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
        return default


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
    if "run_id" in df.columns:
        return df.where(F.col("run_id") == F.lit(RUN_ID))
    return df


def _overwrite_delta(df, table_full: str, partition_cols: Optional[Iterable[str]] = None) -> None:
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.saveAsTable(table_full)


def _metric_df(rows):
    return spark.createDataFrame(
        [(str(k), str(v), datetime.now(timezone.utc).isoformat()) for k, v in rows],
        "metric string, value string, recorded_at string",
    )


def _save_plot_table(df, table_name: str) -> str:
    full = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, table_name)
    _overwrite_delta(df, full)
    return f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{table_name}"


def _brief_exception(exc: Exception, max_chars: int = 320) -> str:
    text = " ".join(str(exc).split())
    return text[:max_chars] + ("..." if len(text) > max_chars else "")

# COMMAND ----------
_create_text_widget("scratch_catalog", "dev_sean")
_create_text_widget("scratch_schema", "matt")
_create_text_widget("output_prefix", "yt_lid_v3_validation_10k")
_create_text_widget("run_id", "codex_10k")
_create_text_widget("figures_dbfs_dir", "")  # optional; analysis_figures_svg is the canonical artifact
_create_text_widget("top_language_n", "20")

SCRATCH_CATALOG = _get_widget("scratch_catalog", "dev_sean")
SCRATCH_SCHEMA = _get_widget("scratch_schema", "matt")
OUTPUT_PREFIX = _safe_token(_get_widget("output_prefix", "yt_lid_v3_validation_10k"), "yt_lid_v3_validation_10k")
RUN_ID = _get_widget("run_id", "codex_10k")
FIGURES_DBFS_DIR = _get_widget("figures_dbfs_dir", "").rstrip("/")
TOP_LANGUAGE_N = _get_int_widget("top_language_n", 20)

FIGURES_LOCAL_DIR = os.path.join("/local_disk0/tmp", "yt_lid_v3_figures", OUTPUT_PREFIX, RUN_ID)
try:
    os.makedirs(FIGURES_LOCAL_DIR, exist_ok=True)
except Exception as exc:
    print(f"WARNING: local figure directory unavailable ({FIGURES_LOCAL_DIR}): {exc}")
    FIGURES_LOCAL_DIR = ""
if FIGURES_DBFS_DIR:
    try:
        dbutils.fs.mkdirs(FIGURES_DBFS_DIR)
    except Exception as exc:
        print(f"WARNING: optional figure file directory unavailable ({FIGURES_DBFS_DIR}): {_brief_exception(exc)}")
        FIGURES_DBFS_DIR = ""
else:
    print("Figure file copy disabled; SVGs will be saved in the analysis_figures_svg Delta table.")

COHORT_TABLE = f"{OUTPUT_PREFIX}_cohort_sample"
ANALYSIS_METRICS_TABLE = f"{OUTPUT_PREFIX}_analysis_metrics"
ANALYSIS_TABLE_COUNTS_TABLE = f"{OUTPUT_PREFIX}_analysis_table_counts"
ANALYSIS_STATUS_BY_STRATUM_TABLE = f"{OUTPUT_PREFIX}_analysis_status_by_stratum"
ANALYSIS_CONSENSUS_BY_STRATUM_TABLE = f"{OUTPUT_PREFIX}_analysis_consensus_by_stratum"
ANALYSIS_AGREEMENT_BY_STRATUM_TABLE = f"{OUTPUT_PREFIX}_analysis_agreement_by_stratum"
ANALYSIS_TOP_LANGUAGES_TABLE = f"{OUTPUT_PREFIX}_analysis_top_languages"
ANALYSIS_VALIDATION_SAMPLE_STRATA_TABLE = f"{OUTPUT_PREFIX}_analysis_manual_validation_sample_strata"
ANALYSIS_FIGURES_TABLE = f"{OUTPUT_PREFIX}_analysis_figures_svg"

OUTPUT_TABLE_SUFFIXES = {
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
}

print("Analyzing output prefix:", OUTPUT_PREFIX)
print("Run id:", RUN_ID)
print("Figures:", FIGURES_DBFS_DIR)

# COMMAND ----------
if not _table_exists(SCRATCH_CATALOG, SCRATCH_SCHEMA, COHORT_TABLE):
    raise ValueError(f"Missing cohort table: {SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{COHORT_TABLE}")

missing_tables = [
    f"{OUTPUT_PREFIX}_{suffix}"
    for suffix in OUTPUT_TABLE_SUFFIXES.values()
    if not _table_exists(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{OUTPUT_PREFIX}_{suffix}")
]
if missing_tables:
    raise ValueError(f"Missing expected LID output tables: {missing_tables}")

tables = {
    key: spark.table(_fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, f"{OUTPUT_PREFIX}_{suffix}"))
    for key, suffix in OUTPUT_TABLE_SUFFIXES.items()
}
cohort = spark.table(_fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, COHORT_TABLE))

table_counts_rows = []
for key, df in tables.items():
    table_counts_rows.append((key, f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{OUTPUT_PREFIX}_{OUTPUT_TABLE_SUFFIXES[key]}", _current_run(df).count()))

table_counts = spark.createDataFrame(table_counts_rows, "table_key string, table_name string, n_rows long")
_overwrite_delta(table_counts, _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, ANALYSIS_TABLE_COUNTS_TABLE))

# COMMAND ----------
cohort_sel = cohort.select(
    "channel_id",
    "validation_stratum",
    "selection_rank",
    F.col("openlid_primary_language_label").alias("previous_openlid_primary_language_label"),
    F.col("glotlid_primary_language_label").alias("previous_glotlid_primary_language_label"),
    F.col("consensus_status").alias("previous_consensus_status"),
    F.col("previous_models_agree_exact_primary").alias("previous_models_agree_exact_primary"),
)

channels_sel = _current_run(tables["channels"]).select(
    "channel_id",
    "language_status",
    "consensus_status",
    "consensus_source",
    "consensus_language_label",
    "requires_manual_adjudication",
    "openlid_primary_language_label",
    "glotlid_primary_language_label",
    "models_agree_exact_primary",
    "models_agree_iso_primary",
    "models_agree_analysis_cluster_primary",
    "openlid_primary_is_high_risk",
    "glotlid_primary_is_high_risk",
)

joined = cohort_sel.join(channels_sel, on="channel_id", how="left").cache()
joined_count = joined.count()
missing_output_count = joined.where(F.col("language_status").isNull()).count()
if missing_output_count:
    raise AssertionError(f"{missing_output_count} sampled cohort channels are missing from final output.")

# COMMAND ----------
status_by_stratum = (
    joined
    .groupBy("validation_stratum", "language_status")
    .count()
    .orderBy("validation_stratum", F.desc("count"), "language_status")
)
consensus_by_stratum = (
    joined
    .groupBy("validation_stratum", "consensus_status")
    .count()
    .orderBy("validation_stratum", F.desc("count"), "consensus_status")
)
agreement_by_stratum = (
    joined
    .groupBy("validation_stratum")
    .agg(
        F.count(F.lit(1)).alias("n_channels"),
        F.avg(F.col("models_agree_exact_primary").cast("double")).alias("current_exact_agreement_rate"),
        F.avg(F.col("models_agree_iso_primary").cast("double")).alias("current_iso_agreement_rate"),
        F.avg(F.col("requires_manual_adjudication").cast("double")).alias("manual_adjudication_rate"),
        F.avg(F.col("openlid_primary_is_high_risk").cast("double")).alias("openlid_high_risk_rate"),
        F.avg(F.col("glotlid_primary_is_high_risk").cast("double")).alias("glotlid_high_risk_rate"),
    )
    .orderBy("validation_stratum")
)
top_languages = (
    joined
    .where(F.col("consensus_language_label").isNotNull())
    .groupBy("validation_stratum", "consensus_language_label")
    .count()
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("validation_stratum").orderBy(F.desc("count"), "consensus_language_label")
    ))
    .where(F.col("_rn") <= F.lit(TOP_LANGUAGE_N))
    .drop("_rn")
    .orderBy("validation_stratum", F.desc("count"), "consensus_language_label")
)

manual_validation_sample = _current_run(tables["manual_validation_sample"])
if "primary_stratum" in manual_validation_sample.columns:
    validation_sample_strata = (
        manual_validation_sample
        .groupBy("primary_stratum")
        .count()
        .orderBy(F.desc("count"), "primary_stratum")
    )
else:
    validation_sample_strata = spark.createDataFrame([], "primary_stratum string, count long")

status_table = _save_plot_table(status_by_stratum, ANALYSIS_STATUS_BY_STRATUM_TABLE)
consensus_table = _save_plot_table(consensus_by_stratum, ANALYSIS_CONSENSUS_BY_STRATUM_TABLE)
agreement_table = _save_plot_table(agreement_by_stratum, ANALYSIS_AGREEMENT_BY_STRATUM_TABLE)
top_languages_table = _save_plot_table(top_languages, ANALYSIS_TOP_LANGUAGES_TABLE)
validation_sample_table = _save_plot_table(validation_sample_strata, ANALYSIS_VALIDATION_SAMPLE_STRATA_TABLE)

# COMMAND ----------
segments_input = _current_run(tables["segments_input"])
valid_segment_count = segments_input.where(F.col("is_valid_text_for_lid")).count() if "is_valid_text_for_lid" in segments_input.columns else None
openlid_count = _current_run(tables["openlid_compact"]).count()
glotlid_count = _current_run(tables["glotlid_compact"]).count()
final_channels_count = _current_run(tables["channels"]).count()
final_channels_distinct = _current_run(tables["channels"]).select("channel_id").distinct().count()
manual_validation_count = manual_validation_sample.count()
ablation_count = _current_run(tables["ablation_summary"]).count()
high_risk_redirect_count = _current_run(tables["high_risk_redirect_diagnostic"]).count()

agreement_rows = {r["validation_stratum"]: r.asDict() for r in agreement_by_stratum.collect()}
metrics_rows = [
    ("cohort_channels", joined_count),
    ("missing_output_channels", missing_output_count),
    ("valid_segments", valid_segment_count),
    ("openlid_compact_rows", openlid_count),
    ("glotlid_compact_rows", glotlid_count),
    ("compact_prediction_row_parity", openlid_count == glotlid_count == valid_segment_count),
    ("final_channels_rows", final_channels_count),
    ("final_channels_distinct", final_channels_distinct),
    ("manual_validation_sample_rows", manual_validation_count),
    ("ablation_summary_rows", ablation_count),
    ("high_risk_redirect_rows", high_risk_redirect_count),
]
for stratum, row in agreement_rows.items():
    metrics_rows.extend([
        (f"{stratum}.n_channels", row.get("n_channels")),
        (f"{stratum}.current_exact_agreement_rate", row.get("current_exact_agreement_rate")),
        (f"{stratum}.current_iso_agreement_rate", row.get("current_iso_agreement_rate")),
        (f"{stratum}.manual_adjudication_rate", row.get("manual_adjudication_rate")),
    ])

metrics_table_full = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, ANALYSIS_METRICS_TABLE)
_overwrite_delta(_metric_df(metrics_rows), metrics_table_full)

# COMMAND ----------
figure_paths: List[str] = []
figure_rows: List[tuple] = []
try:
    import matplotlib.pyplot as plt

    def _persist_figure(fig, local_path: str, file_name: str) -> None:
        buffer = StringIO()
        fig.savefig(buffer, format="svg")
        svg_text = buffer.getvalue()
        created_at = datetime.now(timezone.utc).isoformat()
        dbfs_path = (FIGURES_DBFS_DIR + "/" + file_name) if FIGURES_DBFS_DIR else None
        status = "ok"
        messages: List[str] = []

        try:
            if not local_path:
                raise ValueError("local figure path unavailable")
            with open(local_path, "w", encoding="utf-8") as fh:
                fh.write(svg_text)
        except Exception as exc:
            messages.append(f"local copy skipped or failed: {_brief_exception(exc)}")

        if dbfs_path:
            try:
                dbutils.fs.put(dbfs_path, svg_text, True)
                figure_paths.append(dbfs_path)
            except Exception as exc:
                print(f"WARNING: could not copy figure to {dbfs_path}: {exc}")
                messages.append(f"dbfs copy failed: {_brief_exception(exc)}")
                figure_paths.append("")
        else:
            figure_paths.append("")

        figure_rows.append((RUN_ID, file_name, dbfs_path, local_path, svg_text, status, "; ".join(messages), created_at))

    def _stacked_bar(pdf, index_col: str, category_col: str, value_col: str, title: str, file_name: str) -> None:
        if pdf.empty:
            return
        pivot = pdf.pivot_table(index=index_col, columns=category_col, values=value_col, aggfunc="sum", fill_value=0)
        ax = pivot.plot(kind="bar", stacked=True, figsize=(11, 5))
        ax.set_title(title)
        ax.set_xlabel("")
        ax.set_ylabel("channels")
        ax.legend(loc="center left", bbox_to_anchor=(1.0, 0.5), fontsize=8)
        plt.tight_layout()
        out = os.path.join(FIGURES_LOCAL_DIR, file_name) if FIGURES_LOCAL_DIR else ""
        _persist_figure(ax.get_figure(), out, file_name)
        plt.close()

    _stacked_bar(
        status_by_stratum.toPandas(),
        "validation_stratum",
        "language_status",
        "count",
        "Language Status by Prior Agreement Stratum",
        "language_status_by_prior_stratum.svg",
    )
    consensus_pdf = consensus_by_stratum.toPandas()
    if not consensus_pdf.empty:
        top_statuses = (
            consensus_pdf.groupby("consensus_status")["count"].sum().sort_values(ascending=False).head(12).index.tolist()
        )
        _stacked_bar(
            consensus_pdf[consensus_pdf["consensus_status"].isin(top_statuses)],
            "validation_stratum",
            "consensus_status",
            "count",
            "Consensus Status by Prior Agreement Stratum",
            "consensus_status_by_prior_stratum.svg",
        )

    agreement_pdf = agreement_by_stratum.toPandas()
    if not agreement_pdf.empty:
        ax = agreement_pdf.plot(
            x="validation_stratum",
            y=["current_exact_agreement_rate", "current_iso_agreement_rate", "manual_adjudication_rate"],
            kind="bar",
            figsize=(10, 5),
            ylim=(0, 1),
        )
        ax.set_title("Current Agreement and Review Rates by Prior Stratum")
        ax.set_xlabel("")
        ax.set_ylabel("rate")
        ax.legend(loc="best", fontsize=8)
        plt.tight_layout()
        out = os.path.join(FIGURES_LOCAL_DIR, "agreement_rates_by_prior_stratum.svg") if FIGURES_LOCAL_DIR else ""
        _persist_figure(ax.get_figure(), out, "agreement_rates_by_prior_stratum.svg")
        plt.close()
except Exception as exc:
    print(f"WARNING: figure creation failed: {exc}")

figure_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("dbfs_path", StringType(), True),
    StructField("driver_local_path", StringType(), True),
    StructField("svg_text", StringType(), True),
    StructField("status", StringType(), False),
    StructField("message", StringType(), True),
    StructField("created_at", StringType(), False),
])
figures_df = spark.createDataFrame(figure_rows, schema=figure_schema)
figures_table_full = _fqtn(SCRATCH_CATALOG, SCRATCH_SCHEMA, ANALYSIS_FIGURES_TABLE)
_overwrite_delta(figures_df, figures_table_full)

# COMMAND ----------
result = {
    "status": "ok",
    "run_id": RUN_ID,
    "output_prefix": OUTPUT_PREFIX,
    "metrics_table": f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{ANALYSIS_METRICS_TABLE}",
    "table_counts_table": f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{ANALYSIS_TABLE_COUNTS_TABLE}",
    "status_by_stratum_table": status_table,
    "consensus_by_stratum_table": consensus_table,
    "agreement_by_stratum_table": agreement_table,
    "top_languages_table": top_languages_table,
    "manual_validation_sample_strata_table": validation_sample_table,
    "figures_table": f"{SCRATCH_CATALOG}.{SCRATCH_SCHEMA}.{ANALYSIS_FIGURES_TABLE}",
    "figure_paths": figure_paths,
    "figures": [{"file_name": r[1], "status": r[5], "chars": len(r[4] or "")} for r in figure_rows],
    "metrics": {str(k): str(v) for k, v in metrics_rows},
}
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
