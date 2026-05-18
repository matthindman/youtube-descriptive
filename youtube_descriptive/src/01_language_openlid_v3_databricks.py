# Databricks notebook source
# MAGIC %md
# MAGIC # YouTube Census: language classification with OpenLID-v3
# MAGIC
# MAGIC This notebook performs first-cut channel language classification using OpenLID-v3. It classifies text segments separately, stores segment-level predictions, then aggregates to channel-level language labels. GlotLID code is included but disabled by default.
# MAGIC
# MAGIC **Expected source tables, based on the current Unity Catalog layout:**
# MAGIC - `prod_tads.youtube.yt_sl_channels`
# MAGIC - `prod_tads.youtube.yt_sl_videos`
# MAGIC
# MAGIC **Output tables:**
# MAGIC - `dev_sean.matt.yt_lid_openlid_v3_segments`
# MAGIC - `dev_sean.matt.yt_lid_openlid_v3_channel_votes`
# MAGIC - `dev_sean.matt.yt_lid_openlid_v3_channels`

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Install notebook-scoped dependencies
# MAGIC
# MAGIC OpenLID-v3 is a fastText model. The model card recommends `numpy<2`, `fasttext`, `huggingface-hub`, and `regex`. Run this cell first, then allow the notebook to restart Python.

# COMMAND ----------
# MAGIC %pip install "numpy<2" fasttext==0.9.3 huggingface-hub==0.35.3 regex==2024.4.28 pandas pyarrow

# COMMAND ----------
# Restart after %pip installs so Python sees newly installed libraries.
# Databricks recommends restartPython after notebook-scoped %pip installs.
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Parameters
# MAGIC
# MAGIC Use these widgets to switch catalogs/schemas, choose a model path, run a sample, or update the source channel table.

# COMMAND ----------
import json
import os
import shutil
import time
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import regex
import fasttext
from huggingface_hub import hf_hub_download

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------
# Widget helpers.
def _create_text_widget(name: str, default: str, label: Optional[str] = None) -> None:
    try:
        dbutils.widgets.text(name, default, label or name)
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


def _get_float_widget(name: str, default: float) -> float:
    raw = _get_widget(name, str(default)).strip()
    return float(raw) if raw else default


# Source table parameters.
_create_text_widget("catalog", "prod_tads")
_create_text_widget("schema", "youtube")
_create_text_widget("channels_table", "yt_sl_channels")
_create_text_widget("videos_table", "yt_sl_videos")

# Output table parameters.
_create_text_widget("output_catalog", "dev_sean")
_create_text_widget("output_schema", "matt")
_create_text_widget("output_segment_table", "yt_lid_openlid_v3_segments")
_create_text_widget("output_votes_table", "yt_lid_openlid_v3_channel_votes")
_create_text_widget("output_channel_table", "yt_lid_openlid_v3_channels")

# Model parameters.
_create_text_widget("model_repo", "HPLT/OpenLID-v3")
_create_text_widget("model_filename", "openlid-v3.bin")
_create_text_widget("model_local_path", "/Volumes/dev_sean/matt/models/openlid-v3.bin")
_create_text_widget("download_model_if_missing", "false")
_create_text_widget("model_distribution_mode", "direct_path")  # direct_path or sparkfiles

# GlotLID fallback. Leave disabled for first cut.
_create_text_widget("enable_glotlid_fallback", "false")
_create_text_widget("glotlid_repo", "cis-lmu/glotlid")
_create_text_widget("glotlid_filename", "model.bin")
_create_text_widget("glotlid_local_path", "/dbfs/models/glotlid/model.bin")

# Run controls.
_create_text_widget("limit_channels", "0")  # 0 = full corpus
_create_text_widget("videos_per_channel", "10")
_create_text_widget("video_rank_column", "")  # blank = auto-detect
_create_text_widget("cap_videos_without_rank_column", "false")
_create_text_widget("max_segment_chars", "2000")
_create_text_widget("min_clean_chars", "20")
_create_text_widget("top_k", "3")
_create_text_widget("num_partitions", "800")
_create_text_widget("score_threshold", "0.0")
_create_text_widget("mixed_language_ratio_threshold", "0.40")
_create_text_widget("mixed_language_min_secondary_segments", "2")
_create_text_widget("secondary_label_vote_weight", "0.35")
_create_text_widget("use_lid_length_weighting", "true")
_create_text_widget("source_update_format", "label")  # label, iso639_3, or scriptless_label; used only if update_source_detected_language=true
_create_text_widget("update_source_detected_language", "false")

# Text-column overrides. Leave blank to use auto-detected candidate columns.
_create_text_widget("channel_id_column", "channel_id")
_create_text_widget("video_id_column", "video_id")
_create_text_widget("channel_name_column", "")
_create_text_widget("channel_description_column", "")
_create_text_widget("video_title_column", "")
_create_text_widget("video_description_column", "")
_create_text_widget("video_tags_column", "")

# COMMAND ----------
CATALOG = _get_widget("catalog", "prod_tads")
SCHEMA = _get_widget("schema", "youtube")
CHANNELS_TABLE = _get_widget("channels_table", "yt_sl_channels")
VIDEOS_TABLE = _get_widget("videos_table", "yt_sl_videos")

OUTPUT_CATALOG = _get_widget("output_catalog", "dev_sean")
OUTPUT_SCHEMA = _get_widget("output_schema", "matt")
OUTPUT_SEGMENT_TABLE = _get_widget("output_segment_table", "yt_lid_openlid_v3_segments")
OUTPUT_VOTES_TABLE = _get_widget("output_votes_table", "yt_lid_openlid_v3_channel_votes")
OUTPUT_CHANNEL_TABLE = _get_widget("output_channel_table", "yt_lid_openlid_v3_channels")

MODEL_REPO = _get_widget("model_repo", "HPLT/OpenLID-v3")
MODEL_FILENAME = _get_widget("model_filename", "openlid-v3.bin")
MODEL_LOCAL_PATH = _get_widget("model_local_path", "/Volumes/dev_sean/matt/models/openlid-v3.bin")
DOWNLOAD_MODEL_IF_MISSING = _get_bool_widget("download_model_if_missing", False)
MODEL_DISTRIBUTION_MODE = _get_widget("model_distribution_mode", "direct_path").strip().lower()

ENABLE_GLOTLID_FALLBACK = _get_bool_widget("enable_glotlid_fallback", False)
GLOTLID_REPO = _get_widget("glotlid_repo", "cis-lmu/glotlid")
GLOTLID_FILENAME = _get_widget("glotlid_filename", "model.bin")
GLOTLID_LOCAL_PATH = _get_widget("glotlid_local_path", "/dbfs/models/glotlid/model.bin")

LIMIT_CHANNELS = _get_int_widget("limit_channels", 0)
VIDEOS_PER_CHANNEL = _get_int_widget("videos_per_channel", 10)
VIDEO_RANK_COLUMN = _get_widget("video_rank_column", "").strip()
CAP_VIDEOS_WITHOUT_RANK_COLUMN = _get_bool_widget("cap_videos_without_rank_column", False)
MAX_SEGMENT_CHARS = _get_int_widget("max_segment_chars", 2000)
MIN_CLEAN_CHARS = _get_int_widget("min_clean_chars", 20)
TOP_K = _get_int_widget("top_k", 3)
NUM_PARTITIONS = _get_int_widget("num_partitions", 800)
SCORE_THRESHOLD = _get_float_widget("score_threshold", 0.0)
MIXED_LANGUAGE_RATIO_THRESHOLD = _get_float_widget("mixed_language_ratio_threshold", 0.40)
MIXED_LANGUAGE_MIN_SECONDARY_SEGMENTS = _get_int_widget("mixed_language_min_secondary_segments", 2)
SECONDARY_LABEL_VOTE_WEIGHT = _get_float_widget("secondary_label_vote_weight", 0.35)
USE_LID_LENGTH_WEIGHTING = _get_bool_widget("use_lid_length_weighting", True)
SOURCE_UPDATE_FORMAT = _get_widget("source_update_format", "label").strip().lower()
UPDATE_SOURCE_DETECTED_LANGUAGE = _get_bool_widget("update_source_detected_language", False)

if SOURCE_UPDATE_FORMAT not in {"label", "iso639_3", "scriptless_label"}:
    raise ValueError("source_update_format must be one of: label, iso639_3, scriptless_label")

CHANNEL_ID_COLUMN = _get_widget("channel_id_column", "channel_id")
VIDEO_ID_COLUMN = _get_widget("video_id_column", "video_id")
CHANNEL_NAME_COLUMN_OVERRIDE = _get_widget("channel_name_column", "").strip()
CHANNEL_DESCRIPTION_COLUMN_OVERRIDE = _get_widget("channel_description_column", "").strip()
VIDEO_TITLE_COLUMN_OVERRIDE = _get_widget("video_title_column", "").strip()
VIDEO_DESCRIPTION_COLUMN_OVERRIDE = _get_widget("video_description_column", "").strip()
VIDEO_TAGS_COLUMN_OVERRIDE = _get_widget("video_tags_column", "").strip()

# COMMAND ----------
def fqtn(table: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{table}`"


def fqtn_out(table: str) -> str:
    return f"`{OUTPUT_CATALOG}`.`{OUTPUT_SCHEMA}`.`{table}`"


def local_dir_for(path: str) -> str:
    return os.path.dirname(path.replace("dbfs:/", "/dbfs/"))


channels_full = fqtn(CHANNELS_TABLE)
videos_full = fqtn(VIDEOS_TABLE)
segment_output_full = fqtn_out(OUTPUT_SEGMENT_TABLE)
votes_output_full = fqtn_out(OUTPUT_VOTES_TABLE)
channel_output_full = fqtn_out(OUTPUT_CHANNEL_TABLE)

print("Source channels table:", channels_full)
print("Source videos table:", videos_full)
print("Segment output table:", segment_output_full)
print("Channel output table:", channel_output_full)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Download or validate the model binary
# MAGIC
# MAGIC If the Databricks cluster does not have internet access, manually upload the binary and set `download_model_if_missing=false`.

# COMMAND ----------
def ensure_hf_fasttext_model(repo_id: str, filename: str, local_path: str, download_if_missing: bool) -> str:
    """Ensure a fastText .bin model exists at local_path and return the local filesystem path.

    HPLT/OpenLID-v3's model-card snippets have not always used the same filename as the
    repository file list. The repository currently contains `openlid-v3.bin`, while some
    autogenerated Hugging Face snippets refer to `model.bin`. This function tries the
    requested filename first and then common fallbacks before failing.
    """
    local_path = local_path.replace("dbfs:/", "/dbfs/")
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        print(f"Model already present: {local_path} ({os.path.getsize(local_path):,} bytes)")
        return local_path

    if not download_if_missing:
        raise FileNotFoundError(
            f"Model file not found at {local_path}. Upload it there or set download_model_if_missing=true."
        )

    target_dir = local_dir_for(local_path)
    os.makedirs(target_dir, exist_ok=True)

    candidate_filenames = []
    for f in [filename, "openlid-v3.bin", "model.bin"]:
        if f and f not in candidate_filenames:
            candidate_filenames.append(f)

    last_error = None
    for candidate in candidate_filenames:
        try:
            print(f"Downloading {repo_id}/{candidate} to {target_dir} ...")
            downloaded = hf_hub_download(
                repo_id=repo_id,
                filename=candidate,
                local_dir=target_dir,
            )
            if downloaded != local_path:
                shutil.copyfile(downloaded, local_path)
            print(f"Model ready: {local_path} ({os.path.getsize(local_path):,} bytes); source filename={candidate}")
            return local_path
        except Exception as e:
            last_error = e
            print(f"Could not download {repo_id}/{candidate}: {repr(e)[:300]}")

    raise RuntimeError(f"Could not download a usable fastText model from {repo_id}. Last error: {last_error!r}")


MODEL_LOCAL_PATH = ensure_hf_fasttext_model(
    MODEL_REPO, MODEL_FILENAME, MODEL_LOCAL_PATH, DOWNLOAD_MODEL_IF_MISSING
)

# COMMENTED OUT FOR FIRST CUT: GlotLID fallback model. Enable via widget if needed.
# if ENABLE_GLOTLID_FALLBACK:
#     GLOTLID_LOCAL_PATH = ensure_hf_fasttext_model(
#         GLOTLID_REPO, GLOTLID_FILENAME, GLOTLID_LOCAL_PATH, DOWNLOAD_MODEL_IF_MISSING
#     )

# COMMAND ----------
# Optional distribution via SparkFiles. This can be useful if workers cannot read /dbfs or /Volumes paths directly.
# For very large models, direct_path or a cluster init script is usually faster than SparkFiles.
from pyspark import SparkFiles

if MODEL_DISTRIBUTION_MODE == "sparkfiles":
    spark.sparkContext.addFile("file://" + MODEL_LOCAL_PATH if MODEL_LOCAL_PATH.startswith("/") else MODEL_LOCAL_PATH)
    WORKER_MODEL_PATH = SparkFiles.get(os.path.basename(MODEL_LOCAL_PATH))
else:
    WORKER_MODEL_PATH = MODEL_LOCAL_PATH

print("Worker model path:", WORKER_MODEL_PATH)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Build segment-level input table
# MAGIC
# MAGIC We classify channel and video text fields separately, then aggregate across segments. This avoids the common error where an English channel name or boilerplate description swamps the actual language of the videos.

# COMMAND ----------
def columns_lower_map(df) -> Dict[str, str]:
    return {c.lower(): c for c in df.columns}


def first_existing_column(df, candidates: Iterable[str], override: str = "") -> Optional[str]:
    cmap = columns_lower_map(df)
    if override:
        if override.lower() in cmap:
            return cmap[override.lower()]
        raise ValueError(f"Requested override column `{override}` not found. Available columns: {df.columns}")
    for c in candidates:
        if c.lower() in cmap:
            return cmap[c.lower()]
    return None


def add_null_if_missing(df, name: str, dtype: str = "string"):
    return df.withColumn(name, F.lit(None).cast(dtype))


def truncate_col(col_name: str, max_chars: int):
    return F.substring(F.col(col_name).cast("string"), 1, max_chars)


channels_raw = spark.table(channels_full).localCheckpoint(eager=True)
videos_raw = spark.table(videos_full).localCheckpoint(eager=True)

if CHANNEL_ID_COLUMN not in channels_raw.columns:
    raise ValueError(f"Channel ID column `{CHANNEL_ID_COLUMN}` not found in {channels_full}")
if CHANNEL_ID_COLUMN not in videos_raw.columns:
    raise ValueError(f"Channel ID column `{CHANNEL_ID_COLUMN}` not found in {videos_full}")

# Optional limited run for smoke testing.
if LIMIT_CHANNELS > 0:
    print(f"Limiting run to {LIMIT_CHANNELS:,} channels for test mode.")
    sampled_channels = channels_raw.select(CHANNEL_ID_COLUMN).distinct().limit(LIMIT_CHANNELS)
    channels_raw = channels_raw.join(sampled_channels, on=CHANNEL_ID_COLUMN, how="inner")
    videos_raw = videos_raw.join(sampled_channels, on=CHANNEL_ID_COLUMN, how="inner")

channel_name_col = first_existing_column(
    channels_raw,
    ["channel_name", "title", "name", "display_name"],
    CHANNEL_NAME_COLUMN_OVERRIDE,
)
channel_description_col = first_existing_column(
    channels_raw,
    [
        "channel_description",
        "description",
        "about",
        "bio",
        "channel_about",
        "profile_description",
        "channel_text",
    ],
    CHANNEL_DESCRIPTION_COLUMN_OVERRIDE,
)
video_id_col = first_existing_column(videos_raw, [VIDEO_ID_COLUMN, "id", "video"], VIDEO_ID_COLUMN)
video_title_col = first_existing_column(
    videos_raw,
    ["video_title", "title", "name"],
    VIDEO_TITLE_COLUMN_OVERRIDE,
)
video_description_col = first_existing_column(
    videos_raw,
    ["description", "video_description", "caption", "text", "body"],
    VIDEO_DESCRIPTION_COLUMN_OVERRIDE,
)
video_tags_col = first_existing_column(
    videos_raw,
    ["tags", "keywords", "video_tags"],
    VIDEO_TAGS_COLUMN_OVERRIDE,
)

print("Detected channel_name_col:", channel_name_col)
print("Detected channel_description_col:", channel_description_col)
print("Detected video_id_col:", video_id_col)
print("Detected video_title_col:", video_title_col)
print("Detected video_description_col:", video_description_col)
print("Detected video_tags_col:", video_tags_col)

# COMMAND ----------
# Optionally keep only the N most recent videos if a date/time column is available.
rank_candidates = [
    "published_at",
    "publish_time",
    "published_time",
    "upload_date",
    "created_time",
    "created_at",
    "first_capture_time",
    "ingestion_timestamp",
    "capture_date",
]
if VIDEO_RANK_COLUMN:
    if VIDEO_RANK_COLUMN not in videos_raw.columns:
        raise ValueError(f"video_rank_column `{VIDEO_RANK_COLUMN}` was specified but is not present.")
    rank_col = VIDEO_RANK_COLUMN
else:
    rank_col = first_existing_column(videos_raw, rank_candidates)

videos_for_segments = videos_raw
if VIDEOS_PER_CHANNEL > 0:
    if rank_col:
        print(f"Restricting to {VIDEOS_PER_CHANNEL} videos/channel using rank column `{rank_col}`.")
        order_cols = [F.col(rank_col).desc_nulls_last()]
        if video_id_col:
            order_cols.append(F.col(video_id_col).asc_nulls_last())
        w = Window.partitionBy(CHANNEL_ID_COLUMN).orderBy(*order_cols)
        videos_for_segments = (
            videos_for_segments
            .withColumn("_video_rank_for_lid", F.row_number().over(w))
            .where(F.col("_video_rank_for_lid") <= VIDEOS_PER_CHANNEL)
        )
    elif CAP_VIDEOS_WITHOUT_RANK_COLUMN and video_id_col:
        print(
            f"No rank column found. Restricting to {VIDEOS_PER_CHANNEL} deterministic videos/channel by video_id hash."
        )
        w = Window.partitionBy(CHANNEL_ID_COLUMN).orderBy(F.xxhash64(F.col(video_id_col)).asc())
        videos_for_segments = (
            videos_for_segments
            .withColumn("_video_rank_for_lid", F.row_number().over(w))
            .where(F.col("_video_rank_for_lid") <= VIDEOS_PER_CHANNEL)
        )
    else:
        print("No video rank column found; using all rows present in yt_sl_videos. If this table is not already capped to recent videos, set video_rank_column or cap_videos_without_rank_column=true.")

# COMMAND ----------
segment_dfs = []

base_channel_cols = [
    F.col(CHANNEL_ID_COLUMN).cast("string").alias("channel_id"),
    F.lit(None).cast("string").alias("video_id"),
]

if channel_name_col:
    segment_dfs.append(
        channels_raw.select(
            *base_channel_cols,
            F.lit("channel_name").alias("segment_type"),
            truncate_col(channel_name_col, MAX_SEGMENT_CHARS).alias("text"),
        )
    )

if channel_description_col:
    segment_dfs.append(
        channels_raw.select(
            *base_channel_cols,
            F.lit("channel_description").alias("segment_type"),
            truncate_col(channel_description_col, MAX_SEGMENT_CHARS).alias("text"),
        )
    )

base_video_cols = [
    F.col(CHANNEL_ID_COLUMN).cast("string").alias("channel_id"),
    F.col(video_id_col).cast("string").alias("video_id") if video_id_col else F.lit(None).cast("string").alias("video_id"),
]

if video_title_col:
    segment_dfs.append(
        videos_for_segments.select(
            *base_video_cols,
            F.lit("video_title").alias("segment_type"),
            truncate_col(video_title_col, MAX_SEGMENT_CHARS).alias("text"),
        )
    )

if video_description_col:
    segment_dfs.append(
        videos_for_segments.select(
            *base_video_cols,
            F.lit("video_description").alias("segment_type"),
            truncate_col(video_description_col, MAX_SEGMENT_CHARS).alias("text"),
        )
    )

if video_tags_col:
    segment_dfs.append(
        videos_for_segments.select(
            *base_video_cols,
            F.lit("video_tags").alias("segment_type"),
            truncate_col(video_tags_col, MAX_SEGMENT_CHARS).alias("text"),
        )
    )

if not segment_dfs:
    raise ValueError("No usable text segment columns were found. Set column override widgets manually.")

segments = segment_dfs[0]
for s in segment_dfs[1:]:
    segments = segments.unionByName(s)

segments = (
    segments
    .where(F.col("channel_id").isNotNull())
    .where(F.col("text").isNotNull() & (F.length(F.trim(F.col("text"))) > 0))
    .withColumn("text", F.substring(F.col("text"), 1, MAX_SEGMENT_CHARS))
    .withColumn(
        "segment_id",
        F.sha2(F.concat_ws("||", F.col("channel_id"), F.coalesce(F.col("video_id"), F.lit("")), F.col("segment_type"), F.col("text")), 256),
    )
    .withColumn("lid_model", F.lit("openlid-v3"))
    .repartition(NUM_PARTITIONS, "channel_id")
)

print("Segment types:")
display(segments.groupBy("segment_type").count().orderBy("segment_type"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Run OpenLID-v3 segment-level inference

# COMMAND ----------
# Precompile patterns used inside each worker.
NONWORD_REPLACE_PATTERN = regex.compile(r"[^\p{Word}\p{Zs}]|\d")
SPACE_PATTERN = regex.compile(r"\s\s+")
URL_PATTERN = regex.compile(r"https?://\S+|www\.\S+", flags=regex.IGNORECASE)


def preprocess_for_lid(text: Optional[str]) -> str:
    if text is None:
        return ""
    text = str(text).strip().replace("\n", " ").replace("\r", " ").lower()
    text = regex.sub(URL_PATTERN, " ", text)
    text = regex.sub(SPACE_PATTERN, " ", text)
    text = regex.sub(NONWORD_REPLACE_PATTERN, "", text)
    text = regex.sub(SPACE_PATTERN, " ", text).strip()
    return text


prediction_schema = StructType([
    StructField("label_1", StringType(), True),
    StructField("score_1", DoubleType(), True),
    StructField("label_2", StringType(), True),
    StructField("score_2", DoubleType(), True),
    StructField("label_3", StringType(), True),
    StructField("score_3", DoubleType(), True),
    StructField("clean_text_len", IntegerType(), True),
    StructField("is_valid_text", BooleanType(), True),
    StructField("lid_error", StringType(), True),
])

_LID_MODEL = None


def _load_lid_model_once():
    global _LID_MODEL
    if _LID_MODEL is None:
        _LID_MODEL = fasttext.load_model(WORKER_MODEL_PATH)
    return _LID_MODEL


@F.pandas_udf(prediction_schema)
def openlid_predict_udf(text_series: pd.Series) -> pd.DataFrame:
    model = _load_lid_model_once()
    rows = []
    for raw in text_series:
        clean = preprocess_for_lid(raw)
        row = {
            "label_1": None,
            "score_1": None,
            "label_2": None,
            "score_2": None,
            "label_3": None,
            "score_3": None,
            "clean_text_len": len(clean),
            "is_valid_text": len(clean) >= MIN_CLEAN_CHARS,
            "lid_error": None,
        }
        if len(clean) < MIN_CLEAN_CHARS:
            rows.append(row)
            continue
        try:
            labels, scores = model.predict(
                text=clean,
                k=max(1, TOP_K),
                threshold=SCORE_THRESHOLD,
                on_unicode_error="replace",
            )
            labels = [x.replace("__label__", "") for x in labels]
            scores = [float(x) for x in scores]
            for idx in range(min(3, len(labels))):
                row[f"label_{idx + 1}"] = labels[idx]
                row[f"score_{idx + 1}"] = scores[idx]
        except Exception as e:
            row["lid_error"] = repr(e)[:500]
        rows.append(row)
    return pd.DataFrame(rows)

# COMMAND ----------
predictions = (
    segments
    .withColumn("lid", openlid_predict_udf(F.col("text")))
    .select(
        "channel_id", "video_id", "segment_id", "segment_type", "text", "lid_model", "lid.*",
        F.split(F.col("lid.label_1"), "_").getItem(0).alias("iso639_3_1"),
        F.split(F.col("lid.label_1"), "_").getItem(1).alias("script_1"),
        F.split(F.col("lid.label_2"), "_").getItem(0).alias("iso639_3_2"),
        F.split(F.col("lid.label_2"), "_").getItem(1).alias("script_2"),
        F.split(F.col("lid.label_3"), "_").getItem(0).alias("iso639_3_3"),
        F.split(F.col("lid.label_3"), "_").getItem(1).alias("script_3"),
        F.current_timestamp().alias("prediction_timestamp"),
    )
)

(
    predictions
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(segment_output_full)
)

print("Wrote segment predictions to", segment_output_full)
display(spark.table(segment_output_full).groupBy("segment_type").count().orderBy("segment_type"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Aggregate segment predictions to channel-level language labels

# COMMAND ----------
segment_weights = spark.createDataFrame(
    [
        ("channel_name", 0.50),
        ("channel_description", 3.00),
        ("video_title", 2.00),
        ("video_description", 1.00),
        ("video_tags", 0.75),
    ],
    ["segment_type", "segment_weight"],
)

pred = spark.table(segment_output_full)

# Convert top-k segment predictions into language votes. Top-1 votes carry full
# weight; top-2 votes carry a smaller weight so bilingual channels where the
# secondary language is consistently ranked second are not invisible at channel
# aggregation time.
top1_votes = pred.select(
    "channel_id", "segment_id", "segment_type", "clean_text_len",
    F.col("label_1").alias("label_1"),
    F.col("iso639_3_1").alias("iso639_3_1"),
    F.col("script_1").alias("script_1"),
    F.col("score_1").alias("score"),
    F.lit(1).alias("prediction_rank"),
)
top2_votes = pred.select(
    "channel_id", "segment_id", "segment_type", "clean_text_len",
    F.col("label_2").alias("label_1"),
    F.col("iso639_3_2").alias("iso639_3_1"),
    F.col("script_2").alias("script_1"),
    F.col("score_2").alias("score"),
    F.lit(2).alias("prediction_rank"),
)

# Exclude noise/unknown labels and labels with missing score.
# Cached because valid_pred is consumed twice: once for weighted_votes, once for total_scores.
valid_pred = (
    top1_votes.unionByName(top2_votes)
    .where(F.col("label_1").isNotNull())
    .where(F.col("score").isNotNull())
    .withColumn("label_lower", F.lower(F.col("label_1")))
    .where(~F.col("label_lower").rlike(r"^(zxx|und|noise|null|none|unknown)"))
    .join(F.broadcast(segment_weights), on="segment_type", how="left")
    .withColumn("segment_weight", F.coalesce(F.col("segment_weight"), F.lit(1.0)))
    .withColumn("rank_weight", F.when(F.col("prediction_rank") == 1, F.lit(1.0)).otherwise(F.lit(float(SECONDARY_LABEL_VOTE_WEIGHT))))
    .withColumn(
        "length_weight",
        F.least(
            F.lit(2.0),
            F.greatest(F.lit(0.25), F.log1p(F.coalesce(F.col("clean_text_len"), F.lit(0)).cast("double")) / F.log1p(F.lit(200.0)))
        ) if USE_LID_LENGTH_WEIGHTING else F.lit(1.0)
    )
    .withColumn("weighted_score", F.col("score") * F.col("segment_weight") * F.col("rank_weight") * F.col("length_weight"))
).cache()

weighted_votes = (
    valid_pred
    .groupBy("channel_id", "label_1", "iso639_3_1", "script_1")
    .agg(
        F.sum("weighted_score").alias("weighted_score"),
        F.countDistinct("segment_id").alias("segment_count"),
        F.count("*").alias("vote_count"),
        F.avg("score").alias("mean_segment_score"),
        F.max("score").alias("max_segment_score"),
        F.avg("rank_weight").alias("mean_rank_weight"),
        F.avg("length_weight").alias("mean_length_weight"),
        F.collect_set("segment_type").alias("segment_types"),
    )
)

rank_window = Window.partitionBy("channel_id").orderBy(F.desc("weighted_score"), F.desc("segment_count"), F.desc("max_segment_score"), F.asc("label_1"))
ranked_votes = weighted_votes.withColumn("language_rank", F.row_number().over(rank_window))

(
    ranked_votes
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(votes_output_full)
)
print("Wrote channel-level language vote table to", votes_output_full)

# COMMAND ----------
top_votes = spark.table(votes_output_full).where(F.col("language_rank") <= 10)

total_scores = (
    valid_pred
    .groupBy("channel_id")
    .agg(
        F.sum("weighted_score").alias("total_language_score"),
        F.countDistinct("segment_id").alias("valid_language_segment_count"),
    )
)

pivoted = (
    top_votes
    .groupBy("channel_id")
    .agg(
        F.max(F.when(F.col("language_rank") == 1, F.col("label_1"))).alias("primary_language_label"),
        F.max(F.when(F.col("language_rank") == 1, F.col("iso639_3_1"))).alias("primary_language_iso639_3"),
        F.max(F.when(F.col("language_rank") == 1, F.col("script_1"))).alias("primary_language_script"),
        F.max(F.when(F.col("language_rank") == 1, F.col("weighted_score"))).alias("primary_language_score"),
        F.max(F.when(F.col("language_rank") == 1, F.col("segment_count"))).alias("primary_language_segment_count"),
        F.max(F.when(F.col("language_rank") == 2, F.col("label_1"))).alias("secondary_language_label"),
        F.max(F.when(F.col("language_rank") == 2, F.col("iso639_3_1"))).alias("secondary_language_iso639_3"),
        F.max(F.when(F.col("language_rank") == 2, F.col("weighted_score"))).alias("secondary_language_score"),
        F.max(F.when(F.col("language_rank") == 2, F.col("segment_count"))).alias("secondary_language_segment_count"),
        F.to_json(
            F.sort_array(
                F.collect_list(
                    F.struct(
                        "language_rank",
                        "label_1",
                        "iso639_3_1",
                        "script_1",
                        "weighted_score",
                        "segment_count",
                        "vote_count",
                        "mean_segment_score",
                        "max_segment_score",
                        "mean_rank_weight",
                        "mean_length_weight",
                        "segment_types",
                    )
                ),
                asc=True,
            )
        ).alias("language_votes_json"),
    )
)

all_channels = channels_raw.select(F.col(CHANNEL_ID_COLUMN).cast("string").alias("channel_id")).distinct()
channel_summary = (
    all_channels
    .join(pivoted, on="channel_id", how="left")
    .join(total_scores, on="channel_id", how="left")
    .withColumn(
        "primary_language_confidence",
        F.when(F.col("total_language_score") > 0, F.col("primary_language_score") / F.col("total_language_score")),
    )
    .withColumn(
        "secondary_to_primary_score_ratio",
        F.when(F.col("primary_language_score") > 0, F.col("secondary_language_score") / F.col("primary_language_score")),
    )
    .withColumn(
        "is_mixed_language_candidate",
        (F.col("secondary_to_primary_score_ratio") >= F.lit(MIXED_LANGUAGE_RATIO_THRESHOLD))
        & (F.coalesce(F.col("secondary_language_segment_count"), F.lit(0)) >= F.lit(MIXED_LANGUAGE_MIN_SECONDARY_SEGMENTS)),
    )
    .withColumn(
        "language_status",
        F.when(F.col("primary_language_label").isNull(), F.lit("insufficient_text_or_unclassified"))
        .when(F.col("is_mixed_language_candidate"), F.lit("mixed_language_candidate"))
        .otherwise(F.lit("classified")),
    )
    .withColumn("lid_model", F.lit("openlid-v3"))
    .withColumn("prediction_timestamp", F.current_timestamp())
)

# Preserve existing source language fields for audit if present.
source_audit_cols = []
for c in ["language_code", "detected_language"]:
    if c in channels_raw.columns:
        source_audit_cols.append(F.col(c).cast("string").alias(f"source_{c}"))
if source_audit_cols:
    audit = channels_raw.select(F.col(CHANNEL_ID_COLUMN).cast("string").alias("channel_id"), *source_audit_cols)
    channel_summary = channel_summary.join(audit, on="channel_id", how="left")

(
    channel_summary
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(channel_output_full)
)

print("Wrote channel-level language table to", channel_output_full)
valid_pred.unpersist()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. QA summaries

# COMMAND ----------
lang = spark.table(channel_output_full)

print("Overall classification status")
display(lang.groupBy("language_status").count().orderBy(F.desc("count")))

print("Top detected languages")
display(
    lang
    .groupBy("primary_language_label", "primary_language_iso639_3", "primary_language_script")
    .agg(
        F.count("*").alias("n_channels"),
        F.avg("primary_language_confidence").alias("mean_confidence"),
        F.expr("percentile(primary_language_confidence, 0.5)").alias("median_confidence"),
    )
    .orderBy(F.desc("n_channels"))
    .limit(100)
)

print("Potential mixed-language channels")
display(
    lang
    .where(F.col("is_mixed_language_candidate"))
    .select(
        "channel_id",
        "primary_language_label",
        "secondary_language_label",
        "primary_language_confidence",
        "secondary_to_primary_score_ratio",
        "language_votes_json",
    )
    .limit(100)
)

# COMMAND ----------
# Optional comparison against existing source fields.
if "source_language_code" in lang.columns:
    print("Source language_code coverage")
    display(
        lang
        .withColumn("has_source_language_code", F.col("source_language_code").isNotNull() & (F.length(F.trim(F.col("source_language_code"))) > 0))
        .groupBy("has_source_language_code")
        .count()
    )

if "source_detected_language" in lang.columns:
    print("Existing source detected_language coverage")
    display(
        lang
        .withColumn("has_source_detected_language", F.col("source_detected_language").isNotNull() & (F.length(F.trim(F.col("source_detected_language"))) > 0))
        .groupBy("has_source_detected_language")
        .count()
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Optional: update `yt_sl_channels.detected_language`
# MAGIC
# MAGIC This is disabled by default. Prefer keeping the prediction table separate until validation is complete. If enabled, `source_update_format` controls whether the source `detected_language` column receives the full OpenLID label such as `eng_Latn`, the ISO-639-3 code such as `eng`, or the scriptless full label.

# COMMAND ----------
if UPDATE_SOURCE_DETECTED_LANGUAGE:
    if "detected_language" not in channels_raw.columns:
        raise ValueError("Source table does not have a detected_language column to update.")

    if SOURCE_UPDATE_FORMAT == "iso639_3":
        update_expr = "primary_language_iso639_3"
    elif SOURCE_UPDATE_FORMAT == "scriptless_label":
        update_expr = "regexp_replace(primary_language_label, '_[A-Za-z]+$', '')"
    else:
        update_expr = "primary_language_label"

    _view_name = f"_lid_channel_updates_{OUTPUT_CATALOG}_{OUTPUT_SCHEMA}".replace("-", "_")
    spark.table(channel_output_full).createOrReplaceTempView(_view_name)
    _channel_id_col_quoted = f"`{CHANNEL_ID_COLUMN.replace('`', '')}`"
    merge_sql = f"""
    MERGE INTO {channels_full} AS t
    USING (
      SELECT channel_id, {update_expr} AS detected_language_update
      FROM {_view_name}
      WHERE primary_language_label IS NOT NULL
    ) AS s
    ON t.{_channel_id_col_quoted} = s.channel_id
    WHEN MATCHED THEN UPDATE SET t.detected_language = s.detected_language_update
    """
    spark.sql(merge_sql)
    spark.catalog.dropTempView(_view_name)
    print(f"Updated {channels_full}.detected_language from {channel_output_full} using source_update_format={SOURCE_UPDATE_FORMAT}")
else:
    print("Source table update skipped. Set update_source_detected_language=true to enable MERGE.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. GlotLID fallback scaffold — intentionally disabled
# MAGIC
# MAGIC For follow-up runs, enable `enable_glotlid_fallback=true` and adapt the same UDF pattern above. GlotLID uses `cis-lmu/glotlid` and `model.bin`.

# COMMAND ----------
# COMMENTED OUT FOR FIRST CUT
#
# GLOTLID_MODEL = None
# def _load_glotlid_model_once():
#     global GLOTLID_MODEL
#     if GLOTLID_MODEL is None:
#         GLOTLID_MODEL = fasttext.load_model(GLOTLID_LOCAL_PATH)
#     return GLOTLID_MODEL
#
# @F.pandas_udf(prediction_schema)
# def glotlid_predict_udf(text_series: pd.Series) -> pd.DataFrame:
#     model = _load_glotlid_model_once()
#     rows = []
#     for raw in text_series:
#         clean = preprocess_for_lid(raw)
#         row = {
#             "label_1": None, "score_1": None,
#             "label_2": None, "score_2": None,
#             "label_3": None, "score_3": None,
#             "clean_text_len": len(clean),
#             "is_valid_text": len(clean) >= MIN_CLEAN_CHARS,
#             "lid_error": None,
#         }
#         if len(clean) < MIN_CLEAN_CHARS:
#             rows.append(row)
#             continue
#         try:
#             labels, scores = model.predict(clean, k=max(1, TOP_K), threshold=SCORE_THRESHOLD)
#             labels = [x.replace("__label__", "") for x in labels]
#             scores = [float(x) for x in scores]
#             for idx in range(min(3, len(labels))):
#                 row[f"label_{idx + 1}"] = labels[idx]
#                 row[f"score_{idx + 1}"] = scores[idx]
#         except Exception as e:
#             row["lid_error"] = repr(e)[:500]
#         rows.append(row)
#     return pd.DataFrame(rows)
