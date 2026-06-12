# Databricks notebook source
import json
import os

from pyspark.sql import functions as F


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


_create_text_widget("catalog", "dev_sean")
_create_text_widget("schema", "matt")
_create_text_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
_create_text_widget("source_run_id", "too_full_20260609")
_create_text_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
_create_text_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
_create_text_widget("comparison_table", "yt_lid_v3_too_full_20260609_channel_model_comparison")
_create_text_widget("output_dir", "/dbfs/FileStore/youtube_lid_panel_batches/analysis")
_create_text_widget("min_shared_classified", "30")
_create_text_widget("extra_deepseek_run_ids_json", "[\"too_full_20260609_lid_iso_disagree_1k_deepseek_lowthink_2k_20260611\"]")
_create_text_widget(
    "extra_deepseek_run_labels_json",
    "{\"too_full_20260609_lid_iso_disagree_1k_deepseek_lowthink_20260611\":\"thinking low, 600 cap\","
    "\"too_full_20260609_lid_iso_disagree_1k_deepseek_lowthink_2k_20260611\":\"thinking low, 2k cap\"}",
)

CATALOG = _get_widget("catalog", "dev_sean")
SCHEMA = _get_widget("schema", "matt")
RUN_ID = _get_widget("run_id", "too_full_20260609_lid_iso_disagree_1k_20260611")
SOURCE_RUN_ID = _get_widget("source_run_id", "too_full_20260609")
RAW_RESULTS_TABLE = _get_widget("raw_results_table", "yt_lid_v3_too_full_20260609_llm_validation_raw_results")
REQUESTS_TABLE = _get_widget("requests_table", "yt_lid_v3_too_full_20260609_llm_validation_requests")
COMPARISON_TABLE = _get_widget("comparison_table", "yt_lid_v3_too_full_20260609_channel_model_comparison")
OUTPUT_DIR = _get_widget("output_dir", "/dbfs/FileStore/youtube_lid_panel_batches/analysis")
MIN_SHARED_CLASSIFIED = int(_get_widget("min_shared_classified", "30"))
try:
    EXTRA_DEEPSEEK_RUN_IDS = [
        str(x).strip()
        for x in json.loads(_get_widget("extra_deepseek_run_ids_json", "[]"))
        if str(x).strip()
    ]
except Exception as exc:
    raise ValueError(f"extra_deepseek_run_ids_json must be a JSON array: {exc}") from exc
try:
    EXTRA_DEEPSEEK_RUN_LABELS = {
        str(k): str(v)
        for k, v in json.loads(_get_widget("extra_deepseek_run_labels_json", "{}")).items()
        if str(k).strip() and str(v).strip()
    }
except Exception as exc:
    raise ValueError(f"extra_deepseek_run_labels_json must be a JSON object: {exc}") from exc

ARABIC_FAMILY_ISO = {"ara", "arb", "ary", "arz", "arq", "apc", "ars", "ajp", "aeb", "acm", "acq", "aec", "afb", "ayl", "ayn"}
NON_LANGUAGE_BASE_ISO = {"und", "zxx", "mul"}
CANONICAL_BASE_ISO = {
    "ar": "ara",
    "arabic": "ara",
    "arb": "ara",
    "ary": "ara",
    "arz": "ara",
    "arq": "ara",
    "apc": "ara",
    "ars": "ara",
    "ajp": "ara",
    "aeb": "ara",
    "acm": "ara",
    "acq": "ara",
    "aec": "ara",
    "afb": "ara",
    "ayl": "ara",
    "ayn": "ara",
    "bengali": "ben",
    "bosnian": "bos",
    "braj": "bra",
    "brij": "bra",
    "bundeli": "bns",
    "bundelkhandi": "bns",
    "bundleli": "bns",
    "chinese": "cmn",
    "mandarin": "cmn",
    "zh": "cmn",
    "zho": "cmn",
    "cmn": "cmn",
    "croatian": "hrv",
    "english": "eng",
    "french": "fra",
    "gujarati": "guj",
    "guj": "guj",
    "haryanvi": "bgc",
    "hindko": "hnd",
    "hnd": "hnd",
    "tagalog": "fil",
    "filipino": "fil",
    "tgl": "fil",
    "fil": "fil",
    "odia": "ory",
    "ori": "ory",
    "ory": "ory",
    "uzbek": "uzb",
    "uzn": "uzb",
    "uzb": "uzb",
    "malay": "zsm",
    "msa": "zsm",
    "zsm": "zsm",
    "nepali": "npi",
    "nep": "npi",
    "npi": "npi",
    "kurdish": "kmr",
    "kur": "kmr",
    "ku": "kmr",
    "hindi": "hin",
    "javanese": "jav",
    "jv": "jav",
    "jw": "jav",
    "kashmiri": "kas",
    "kas": "kas",
    "khasi": "kha",
    "korean": "kor",
    "kutchi": "kfr",
    "kachchi": "kfr",
    "kutch": "kfr",
    "kfr": "kfr",
    "marwari": "mwr",
    "nagpuri": "sck",
    "sadani": "sck",
    "sadri": "sck",
    "punjabi": "pan",
    "pashto": "pus",
    "pashtun": "pus",
    "rajasthani": "raj",
    "serbian": "srp",
    "serbo-croatian": "hbs",
    "serbocroatian": "hbs",
    "bcs": "hbs",
    "cantonese": "yue",
    "tulu": "tcy",
    "tcy": "tcy",
}
SCRIPT_FAMILY = {
    "arab": "Arab",
    "arabic": "Arab",
    "cyrl": "Cyrl",
    "cyrillic": "Cyrl",
    "deva": "Deva",
    "devanagari": "Deva",
    "guru": "Guru",
    "gurmukhi": "Guru",
    "hani": "Hani",
    "hans": "Hani",
    "hant": "Hani",
    "han": "Hani",
    "hang": "Hang",
    "hangul": "Hang",
    "kore": "Hang",
    "jpan": "Jpan",
    "japanese": "Jpan",
    "latn": "Latn",
    "latin": "Latn",
    "taml": "Taml",
    "tamil": "Taml",
    "telu": "Telu",
    "telugu": "Telu",
    "thai": "Thai",
}


def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def safe_split_get_expr(col, sep: str, idx: int):
    arr = F.split(F.coalesce(col.cast("string"), F.lit("")), sep)
    padded = F.concat(arr, F.array_repeat(F.lit(""), idx + 1))
    return F.element_at(padded, idx + 1)


def canonical_base_iso_expr(col):
    iso = F.lower(F.trim(col.cast("string")))
    iso = F.when(iso.isin("", "null", "none"), F.lit(None).cast("string")).otherwise(iso)
    iso = F.when(iso.isin(*sorted(ARABIC_FAMILY_ISO)), F.lit("ara")).otherwise(iso)
    mapping = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in CANONICAL_BASE_ISO.items()], []))
    mapped = F.coalesce(F.element_at(mapping, iso), iso)
    return (
        F.when(mapped.isin(*sorted(NON_LANGUAGE_BASE_ISO)), F.lit(None).cast("string"))
        .when(mapped.rlike("^[a-z]{3}$"), mapped)
        .otherwise(F.lit(None).cast("string"))
    )


def script_family_expr(col):
    script = F.trim(col.cast("string"))
    script_l = F.lower(script)
    mapping = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in SCRIPT_FAMILY.items()], []))
    return F.coalesce(F.element_at(mapping, script_l), script)


def normalized_language_label_expr(iso_col, script_col):
    iso = canonical_base_iso_expr(iso_col)
    script = script_family_expr(script_col)
    return (
        F.when(iso.isNull(), F.lit(None).cast("string"))
        .when(script.isNull() | script.isin("", "null", "none"), iso)
        .otherwise(F.concat_ws("_", iso, script))
    )


requests = spark.table(fqtn(REQUESTS_TABLE)).where(F.col("run_id") == F.lit(RUN_ID))
sample_channels = requests.select("channel_id").distinct().persist()

extra_run_rows = [
    (
        run_id,
        f"thinking_{idx + 1}",
        EXTRA_DEEPSEEK_RUN_LABELS.get(run_id, f"thinking run {idx + 1}"),
    )
    for idx, run_id in enumerate(EXTRA_DEEPSEEK_RUN_IDS)
]
extra_runs_df = spark.createDataFrame(extra_run_rows or [("__no_extra_deepseek_run__", "", "")], ["run_id", "extra_setting_key", "extra_setting_display"])

raw_run_ids = [RUN_ID] + EXTRA_DEEPSEEK_RUN_IDS
raw = (
    spark.table(fqtn(RAW_RESULTS_TABLE))
    .where(F.col("run_id").isin(*raw_run_ids))
    .join(sample_channels, "channel_id", "inner")
    .join(extra_runs_df, on="run_id", how="left")
    .where((F.col("run_id") == F.lit(RUN_ID)) | (F.lower(F.col("provider")) == F.lit("deepseek")))
)
llm_votes = (
    raw.where(F.col("is_valid_panel_vote") == F.lit(True))
    .withColumn("_provider_l", F.lower(F.col("provider")))
    .withColumn(
        "setting_key",
        F.when((F.col("run_id") == F.lit(RUN_ID)) & (F.col("_provider_l") == F.lit("deepseek")), F.lit("no_thinking"))
        .when((F.col("_provider_l") == F.lit("deepseek")) & F.col("extra_setting_key").isNotNull(), F.col("extra_setting_key"))
        .otherwise(F.lit("baseline")),
    )
    .withColumn(
        "setting_display",
        F.when((F.col("run_id") == F.lit(RUN_ID)) & (F.col("_provider_l") == F.lit("deepseek")), F.lit("no thinking"))
        .when((F.col("_provider_l") == F.lit("deepseek")) & F.col("extra_setting_display").isNotNull(), F.col("extra_setting_display"))
        .otherwise(F.lit("")),
    )
    .withColumn(
        "model_key",
        F.when(
            F.col("_provider_l") == F.lit("deepseek"),
            F.concat_ws(":", F.col("_provider_l"), F.col("model"), F.col("setting_key")),
        ).otherwise(F.concat_ws(":", F.col("_provider_l"), F.col("model"))),
    )
    .withColumn(
        "display_name",
        F.when(
            F.col("_provider_l") == F.lit("deepseek"),
            F.concat(F.col("_provider_l"), F.lit(":"), F.col("model"), F.lit(" ("), F.col("setting_display"), F.lit(")")),
        ).otherwise(F.concat_ws(":", F.col("_provider_l"), F.col("model"))),
    )
    .select(
        "channel_id",
        F.col("_provider_l").alias("provider"),
        "model",
        "model_tier",
        "model_key",
        "display_name",
        canonical_base_iso_expr(F.col("pred_normalized_base_iso")).alias("normalized_base_iso"),
        normalized_language_label_expr(F.col("pred_normalized_base_iso"), safe_split_get_expr(F.col("pred_normalized_language_label"), "_", 1)).alias("normalized_language_label"),
    )
    .where(F.col("normalized_base_iso").isNotNull())
)

cmp = spark.table(fqtn(COMPARISON_TABLE)).where(F.col("run_id") == F.lit(SOURCE_RUN_ID)).join(sample_channels, "channel_id", "inner")
cmp_cols = set(cmp.columns)


def lid_vote(prefix: str, display: str):
    label_col = F.col(f"{prefix}_primary_language_label") if f"{prefix}_primary_language_label" in cmp_cols else F.lit(None).cast("string")
    iso_col = (
        F.col(f"{prefix}_primary_language_iso639_3")
        if f"{prefix}_primary_language_iso639_3" in cmp_cols
        else safe_split_get_expr(label_col, "_", 0)
    )
    script_col = (
        F.col(f"{prefix}_primary_language_script")
        if f"{prefix}_primary_language_script" in cmp_cols
        else safe_split_get_expr(label_col, "_", 1)
    )
    return (
        cmp.select(
            "channel_id",
            F.lit("lid").alias("provider"),
            F.lit(display).alias("model"),
            F.lit("fasttext").alias("model_tier"),
            F.lit(f"lid:{display}").alias("model_key"),
            F.lit(display).alias("display_name"),
            canonical_base_iso_expr(iso_col).alias("normalized_base_iso"),
            normalized_language_label_expr(iso_col, script_col).alias("normalized_language_label"),
        )
        .where(F.col("normalized_base_iso").isNotNull())
    )


votes = llm_votes.unionByName(lid_vote("openlid", "OpenLID")).unionByName(lid_vote("glotlid", "GlotLID")).persist()

models = (
    votes.groupBy("model_key", "display_name", "provider", "model", "model_tier")
    .agg(F.countDistinct("channel_id").alias("n_valid_votes"))
    .collect()
)

provider_order = {
    "lid": 0,
    "openai": 1,
    "anthropic": 2,
    "gemini": 3,
    "deepseek": 4,
}
tier_order = {
    "fasttext": 0,
    "frontier": 1,
    "mid": 2,
    "small": 3,
    "nano": 4,
    "nano_low_cost": 5,
}
model_rows = [row.asDict(recursive=True) for row in models]
model_rows = sorted(
    model_rows,
    key=lambda r: (
        provider_order.get(r["provider"], 99),
        tier_order.get(r["model_tier"], 99),
        r["display_name"],
    ),
)
model_order = {r["model_key"]: i for i, r in enumerate(model_rows)}

a = votes.alias("a")
b = votes.alias("b")
pairwise = (
    a.join(b, on="channel_id", how="inner")
    .where(F.col("a.model_key") < F.col("b.model_key"))
    .groupBy(
        F.col("a.model_key").alias("model_key_a"),
        F.col("a.display_name").alias("display_name_a"),
        F.col("a.provider").alias("provider_a"),
        F.col("a.model_tier").alias("model_tier_a"),
        F.col("b.model_key").alias("model_key_b"),
        F.col("b.display_name").alias("display_name_b"),
        F.col("b.provider").alias("provider_b"),
        F.col("b.model_tier").alias("model_tier_b"),
    )
    .agg(
        F.count(F.lit(1)).alias("n_both_classified"),
        F.sum(F.when(F.col("a.normalized_base_iso") == F.col("b.normalized_base_iso"), 1).otherwise(0)).alias("n_normalized_base_iso_agree"),
        F.sum(F.when(F.col("a.normalized_language_label") == F.col("b.normalized_language_label"), 1).otherwise(0)).alias("n_normalized_label_agree"),
    )
    .withColumn("normalized_base_iso_agreement_rate", F.round(F.col("n_normalized_base_iso_agree") / F.col("n_both_classified"), 4))
    .withColumn("normalized_label_agreement_rate", F.round(F.col("n_normalized_label_agree") / F.col("n_both_classified"), 4))
    .where(F.col("n_both_classified") >= F.lit(MIN_SHARED_CLASSIFIED))
)
pair_rows = [row.asDict(recursive=True) for row in pairwise.collect()]
pair_rows = sorted(
    pair_rows,
    key=lambda r: (
        model_order.get(r["model_key_a"], 999),
        model_order.get(r["model_key_b"], 999),
    ),
)

result = {
    "run_id": RUN_ID,
    "source_run_id": SOURCE_RUN_ID,
    "extra_deepseek_run_ids": EXTRA_DEEPSEEK_RUN_IDS,
    "extra_deepseek_run_labels": EXTRA_DEEPSEEK_RUN_LABELS,
    "min_shared_classified": MIN_SHARED_CLASSIFIED,
    "metric": "normalized_base_iso_agreement_rate",
    "n_sample_channels": sample_channels.count(),
    "models": model_rows,
    "pairwise": pair_rows,
}

out_dir = os.path.join(OUTPUT_DIR, RUN_ID)
os.makedirs(out_dir, exist_ok=True)
out_path = os.path.join(out_dir, "agreement_with_lid_visual_data.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2, sort_keys=True, default=str)

dbfs_path = out_path.replace("/dbfs/", "dbfs:/", 1)
summary = {
    "run_id": RUN_ID,
    "n_models": len(model_rows),
    "n_pairs": len(pair_rows),
    "n_sample_channels": result["n_sample_channels"],
    "output_path": out_path,
    "dbfs_path": dbfs_path,
    "models": [{"display_name": r["display_name"], "n_valid_votes": r["n_valid_votes"]} for r in model_rows],
}
print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(summary, ensure_ascii=False, sort_keys=True))
