# Databricks notebook source
# MAGIC %md
# MAGIC # India Hindi vs English - IndicLID pilot, Extended Data panel aggregates, validation sample
# MAGIC
# MAGIC Many Hindi / Hinglish channels with English-written titles are labelled `eng` by the LID v3 pipeline.
# MAGIC This notebook runs AI4Bharat **IndicLID** (English vs romanised and native-script Indic text) on India-declared
# MAGIC `eng` channels and writes the aggregates behind the Extended Data panel.
# MAGIC
# MAGIC Stages (widget `stage`; `all` = prep, pilot, aggregate, panel, validation; `full` is never part of `all`):
# MAGIC `probe` (egress, model download, speed benchmark; writes nothing) | `prep` (frame, stratified sample, texts) |
# MAGIC `pilot` (IndicLID on the sample) | `full` (IndicLID on every IN x eng channel) | `aggregate` (channel table,
# MAGIC label shares, agreement) | `panel` (category mix by group) | `validation` (300-channel coding sample).
# MAGIC
# MAGIC Channel-level tables stay in Databricks. Only aggregates are exported by `scripts/render_india_lid_local.py`
# MAGIC (plus the 300-row validation sample, public channel IDs and titles only).

# COMMAND ----------
# MAGIC %pip install "numpy<2" fasttext==0.9.3 pandas pyarrow requests
# MAGIC %pip install torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu
# MAGIC %pip install transformers==4.46.3 sentencepiece

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import hashlib
import json
import os
import re
import time
import zipfile
from datetime import datetime, timezone

import numpy as np
import pandas as pd


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


DEFAULTS = {
    "stage": "probe",
    "frame_table": "dev_sean.matt.fig2_map_channels_v2",
    "created_at_table": "dev_sean.matt.yt_channel_created_at",
    "stats_table": "dev_sean.default.yt_channel_stats_full",
    "videos_table": "dev_sean.matt.yt_channel_videos",
    "lid_segments_table": "dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_segments_input",
    "lid_silver_table": "dev_sean.matt.yt_lid_v3_channel_crawl_full_20260623_channel_language_silver_current",
    "snapshot_start": "2026-08-31",
    "snapshot_end": "2026-10-05",
    "country": "IN",
    "per_decile": "1000",
    "seed": "20261016",
    "recent_videos": "10",
    "desc_chars": "300",
    "channel_text_desc_chars": "600",
    "bert_batch": "32",
    "chunk_units": "20000",
    "bert_kinds": "recent_title,lid_title,channel_text",
    "full_use_bert": "false",
    "lex_min_hits": "3",
    "lex_min_rate": "0.05",
    "valid_n": "300",
    "model_dir": "/local_disk0/indiclid",
    "output_prefix": "dev_sean.matt.india_lid_pilot",
    "version": "v1",
    "overwrite": "false",
}
for k, v in DEFAULTS.items():
    _create_text_widget(k, v)
P = {k: _get_widget(k, v) for k, v in DEFAULTS.items()}
STAGE = P["stage"]
OVERWRITE = P["overwrite"].lower() == "true"
SEED = int(P["seed"])
START, END = P["snapshot_start"], P["snapshot_end"]
CLASSES = ["english", "roman_hindi", "roman_other_indic", "native_script", "other"]


def out(name: str) -> str:
    return f"{P['output_prefix']}_{name}_{P['version']}"


STAGE_OUTPUTS = {
    "prep": ["frame", "sample", "texts"],
    "pilot": ["preds", "runlog_pilot"],
    "full": ["fullpreds", "runlog_full"],
    "aggregate": ["channels", "full_channels", "label_shares", "agreement", "lexicon"],
    "panel": ["panel", "panel_sources"],
    "validation": ["validation"],
}
print({**P, "outputs": {s: [out(n) for n in v] for s, v in STAGE_OUTPUTS.items()}})
stages = list(STAGE_OUTPUTS) if STAGE == "all" else [STAGE]
if STAGE == "all":
    stages.remove("full")

# COMMAND ----------
# Pre-flight: refuse to overwrite existing tables unless overwrite=true. Only this notebook's own
# india_lid_pilot_* tables are ever created or replaced.
schema, prefix = P["output_prefix"].rsplit(".", 1)
existing = {r.tableName for r in spark.sql(f"SHOW TABLES IN {schema} LIKE '{prefix}_*'").collect()}
wanted = [out(n) for s in stages if s in STAGE_OUTPUTS for n in STAGE_OUTPUTS[s]]
clash = [t for t in wanted if t.rsplit(".", 1)[1] in existing]
if clash and not OVERWRITE:
    # aggregate / panel stages are re-runnable only with overwrite; tables a stage will not touch are fine
    raise RuntimeError(f"Output tables already exist: {clash}. Set overwrite=true or bump version.")
mode = "CREATE OR REPLACE TABLE" if OVERWRITE else "CREATE TABLE"
if OVERWRITE:
    for t in clash:
        assert t.startswith(P["output_prefix"] + "_"), t
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    mode = "CREATE TABLE"


def log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage prep - frame, stratified sample, texts

# COMMAND ----------
if "prep" in stages:
    FRAME, CREATED, STATS = P["frame_table"], P["created_at_table"], P["stats_table"]
    # Weekly views: the Figure 5 SQL (latest collection per channel and date; status missing_start /
    # missing_end / total_fell / zero / positive; try_divide guards a zero elapsed time). Missing-start and
    # total-fell channels get no wv and are not sampled; the panel excludes them as well.
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW il_wv AS
    WITH snap AS (
      SELECT canonical_id, collected_date,
             max_by(total_view_count, collected_at) AS views,
             max(collected_at)                      AS collected_at
      FROM {STATS}
      WHERE collected_date IN (DATE'{START}', DATE'{END}')
        AND canonical_id IN (SELECT canonical_id FROM {FRAME})
      GROUP BY canonical_id, collected_date
    ),
    e AS (SELECT * FROM snap WHERE collected_date = DATE'{END}'),
    s AS (SELECT * FROM snap WHERE collected_date = DATE'{START}')
    SELECT u.canonical_id,
           CASE WHEN e.canonical_id IS NULL THEN 'missing_end'
                WHEN s.canonical_id IS NULL THEN 'missing_start'
                WHEN e.views < s.views THEN 'total_fell'
                WHEN e.views = s.views THEN 'zero'
                ELSE 'positive' END AS status,
           CASE WHEN e.canonical_id IS NOT NULL AND s.canonical_id IS NOT NULL AND e.views >= s.views
                THEN try_divide(e.views - s.views,
                                (unix_timestamp(e.collected_at) - unix_timestamp(s.collected_at)) / 86400.0) * 7
           END AS wv
    FROM (SELECT DISTINCT canonical_id FROM {FRAME}) u
    LEFT JOIN e USING (canonical_id)
    LEFT JOIN s USING (canonical_id)
    """)
    # Self-declared country: latest status='ok' row per channel (null when the channel declares none).
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW il_country AS
    SELECT canonical_id, max_by(country, collected_at) AS country
    FROM {CREATED} WHERE status = 'ok' GROUP BY canonical_id
    """)
    # Frame: non-topic channels labelled hin (any country) or eng declaring the country. One row per channel.
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW il_frame0 AS
    SELECT f.canonical_id, max(f.channel_name) AS channel_name, max(f.language) AS language,
           max(f.category) AS category, max(c.country) AS country,
           max(w.status) AS wv_status, max(w.wv) AS wv
    FROM {FRAME} f
    LEFT JOIN il_country c USING (canonical_id)
    LEFT JOIN il_wv w USING (canonical_id)
    WHERE NOT coalesce(f.is_topic, false) AND f.language IN ('hin', 'eng')
      AND (f.language = 'hin' OR c.country = '{P['country']}')
    GROUP BY f.canonical_id
    """)
    spark.sql(f"""
    {mode} {out('frame')} AS
    SELECT *,
           CASE WHEN language = 'eng' THEN 'eng_IN'
                WHEN country = '{P['country']}' THEN 'hin_IN'
                WHEN country IS NULL THEN 'hin_no_country' ELSE 'hin_other_country' END AS grp,
           CASE WHEN language = 'eng' AND wv_status IN ('zero', 'positive')
                THEN ntile(10) OVER (PARTITION BY language, wv_status IN ('zero', 'positive')
                                     ORDER BY wv, canonical_id) END AS decile
    FROM il_frame0
    """)
    display(spark.sql(f"""SELECT grp, wv_status, count(*) n, sum(wv) wv_sum FROM {out('frame')}
                          GROUP BY 1, 2 ORDER BY 1, 2"""))
    # Sample: per_decile channels per views decile within IN x eng; deterministic order by seeded hash.
    n_per = int(P["per_decile"])
    spark.sql(f"""
    {mode} {out('sample')} AS
    WITH r AS (
      SELECT canonical_id, channel_name, language, wv_status, decile, wv, category, country,
             row_number() OVER (PARTITION BY decile ORDER BY xxhash64(canonical_id, {SEED}), canonical_id) AS rk,
             count(*) OVER (PARTITION BY decile) AS n_decile
      FROM {out('frame')} WHERE grp = 'eng_IN' AND decile IS NOT NULL)
    SELECT canonical_id, channel_name, language, wv_status, decile, wv, category, country, n_decile,
           n_decile / {n_per} AS weight
    FROM r WHERE rk <= {n_per}
    """)
    display(spark.sql(f"""SELECT decile, count(*) n, max(n_decile) n_decile, min(wv) wv_min, max(wv) wv_max
                          FROM {out('sample')} GROUP BY 1 ORDER BY 1"""))

    # Texts. Scope: the sample, or (full stage) the whole IN x eng frame; built for both so the full run
    # needs no extra prep. Units: channel_text = LID v3 channel_name + channel_description (the metadata text
    # the LID v3 pipeline classified: segments_input.clean_text), lid_title = LID v3 video-title segments (June
    # crawl), recent_title / recent_desc = latest yt_channel_videos snapshot per channel, newest 10 by published_at.
    N_RECENT = int(P["recent_videos"])
    DESC, CH_DESC = int(P["desc_chars"]), int(P["channel_text_desc_chars"])
    URL, WS = r"https?://\\S+", r"\\s+"

    def clean(expr: str) -> str:
        return f"trim(regexp_replace(regexp_replace({expr}, '{URL}', ' '), '{WS}', ' '))"

    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW il_recent AS
    WITH latest AS (
      SELECT canonical_id, max(collected_date) AS d FROM {P['videos_table']}
      WHERE canonical_id IN (SELECT canonical_id FROM {out('frame')} WHERE grp = 'eng_IN')
      GROUP BY canonical_id),
    v AS (
      SELECT v.canonical_id, v.video_id, v.video_title, v.video_description, v.published_at, l.d AS snapshot_date,
             row_number() OVER (PARTITION BY v.canonical_id, v.video_id ORDER BY v.collected_at DESC) AS dup
      FROM {P['videos_table']} v JOIN latest l ON v.canonical_id = l.canonical_id AND v.collected_date = l.d),
    r AS (
      SELECT *, row_number() OVER (PARTITION BY canonical_id ORDER BY published_at DESC, video_id) AS rn
      FROM v WHERE dup = 1)
    SELECT * FROM r WHERE rn <= {N_RECENT}
    """)
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW il_lidseg AS
    SELECT channel_id AS canonical_id, segment_type, clean_text
    FROM {P['lid_segments_table']}
    WHERE channel_id IN (SELECT canonical_id FROM {out('frame')} WHERE grp = 'eng_IN')
      AND segment_type IN ('channel_name', 'channel_description', 'video_title')
    """)
    spark.sql(f"""
    {mode} {out('texts')} AS
    WITH recent_t AS (
      SELECT canonical_id, 'recent_title' AS kind, rn AS pos, {clean('video_title')} AS text FROM il_recent),
    recent_d AS (
      SELECT canonical_id, 'recent_desc' AS kind, rn AS pos, left({clean('video_description')}, {DESC}) AS text
      FROM il_recent),
    lid_t AS (
      SELECT canonical_id, 'lid_title' AS kind,
             row_number() OVER (PARTITION BY canonical_id ORDER BY clean_text) AS pos, {clean('clean_text')} AS text
      FROM il_lidseg WHERE segment_type = 'video_title'),
    chan AS (
      SELECT canonical_id,
             max(CASE WHEN segment_type = 'channel_name' THEN clean_text END) AS nm,
             max(CASE WHEN segment_type = 'channel_description' THEN clean_text END) AS ds
      FROM il_lidseg GROUP BY canonical_id),
    chan_t AS (
      SELECT canonical_id, 'channel_text' AS kind, 1 AS pos,
             {clean("concat_ws('. ', nm, left(ds, " + str(CH_DESC) + "))")} AS text FROM chan),
    allt AS (
      SELECT * FROM recent_t UNION ALL SELECT * FROM recent_d UNION ALL SELECT * FROM lid_t
      UNION ALL SELECT * FROM chan_t)
    SELECT a.* FROM allt a
    WHERE length(a.text) >= 2
      AND NOT (a.kind = 'lid_title' AND EXISTS (
            SELECT 1 FROM recent_t r WHERE r.canonical_id = a.canonical_id AND lower(r.text) = lower(a.text)))
    """)
    display(spark.sql(f"""
      SELECT kind, count(*) units, count(DISTINCT canonical_id) channels, avg(length(text)) avg_len
      FROM {out('texts')} t WHERE canonical_id IN (SELECT canonical_id FROM {out('sample')})
      GROUP BY 1 ORDER BY 1"""))
    display(spark.sql(f"""
      SELECT count(*) sample_channels,
             sum(CASE WHEN EXISTS (SELECT 1 FROM {out('texts')} t WHERE t.canonical_id = s.canonical_id
                                   AND t.kind = 'recent_title') THEN 1 ELSE 0 END) with_recent_titles,
             sum(CASE WHEN EXISTS (SELECT 1 FROM {out('texts')} t WHERE t.canonical_id = s.canonical_id
                                   AND t.kind IN ('recent_title', 'lid_title')) THEN 1 ELSE 0 END) with_any_titles,
             sum(CASE WHEN EXISTS (SELECT 1 FROM {out('texts')} t WHERE t.canonical_id = s.canonical_id
                                   AND t.kind = 'channel_text') THEN 1 ELSE 0 END) with_channel_text
      FROM {out('sample')} s"""))
    display(spark.sql(f"""
      SELECT year(snapshot_date) * 100 + month(snapshot_date) AS snapshot_month, count(DISTINCT canonical_id) channels
      FROM il_recent WHERE canonical_id IN (SELECT canonical_id FROM {out('sample')}) GROUP BY 1 ORDER BY 1"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## IndicLID: model download, loading, inference helpers
# MAGIC
# MAGIC Models come only from AI4Bharat's official GitHub release `v1.0` (https://github.com/AI4Bharat/IndicLID/releases/tag/v1.0):
# MAGIC `indiclid-ftn.zip` (native-script fastText), `indiclid-ftr.zip` (roman fastText), `indiclid-bert.zip` (IndicBERT,
# MAGIC second stage for roman text the fastText model scores <= 0.6). The BERT tokenizer is `ai4bharat/IndicBERTv2-MLM-only`
# MAGIC on AI4Bharat's Hugging Face org, which the official inference code loads. The routing and thresholds below
# MAGIC reimplement `Inference/ai4bharat/IndicLID.py` (repo commit recorded in the run log): text with more than 50%
# MAGIC Latin letters/digits (after removing spaces and punctuation) is roman, else native; roman text goes to FTR and,
# MAGIC if FTR confidence <= 0.6, to IndicBERT.

# COMMAND ----------
RELEASE = "https://github.com/AI4Bharat/IndicLID/releases/download/v1.0"
ZIPS = {"indiclid-ftn": "indiclid-ftn.zip", "indiclid-ftr": "indiclid-ftr.zip", "indiclid-bert": "indiclid-bert.zip"}
MODEL_PATHS = {
    "ftn": "indiclid-ftn/model_baseline_roman.bin",
    "ftr": "indiclid-ftr/model_baseline_roman.bin",
    "bert": "indiclid-bert/basline_nn_simple.pt",
}
ROMAN_INPUT_THRESHOLD, ROMAN_MODEL_THRESHOLD = 0.5, 0.6
TOKENIZER_REPO = "ai4bharat/IndicBERTv2-MLM-only"
RUN_INFO = {}


def download_models(model_dir: str) -> dict:
    import requests
    os.makedirs(model_dir, exist_ok=True)
    info = {}
    for key, fn in ZIPS.items():
        dest = os.path.join(model_dir, fn)
        if not os.path.exists(dest):
            t0 = time.time()
            with requests.get(f"{RELEASE}/{fn}", stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in r.iter_content(1 << 20):
                        fh.write(chunk)
            log(f"downloaded {fn} ({os.path.getsize(dest) / 1e6:.0f} MB) in {time.time() - t0:.0f}s")
        h = hashlib.sha256()
        with open(dest, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        info[fn] = {"bytes": os.path.getsize(dest), "sha256": h.hexdigest(), "url": f"{RELEASE}/{fn}"}
        if not os.path.exists(os.path.join(model_dir, "models", key)):
            with zipfile.ZipFile(dest) as z:
                z.extractall(os.path.join(model_dir, "models"))
    return info


class IndicLIDModels:
    def __init__(self, model_dir: str, load_bert: bool = True):
        import fasttext
        base = os.path.join(model_dir, "models")
        self.ftn = fasttext.load_model(os.path.join(base, MODEL_PATHS["ftn"]))
        self.ftr = fasttext.load_model(os.path.join(base, MODEL_PATHS["ftr"]))
        self.bert = self.tok = None
        if load_bert:
            import torch
            from transformers import AutoTokenizer
            torch.set_num_threads(os.cpu_count() or 4)
            self.torch = torch
            # The release stores a whole pickled model (official AI4Bharat file; sha256 recorded in the run log).
            self.bert = torch.load(os.path.join(base, MODEL_PATHS["bert"]), map_location="cpu", weights_only=False)
            # The pickle predates transformers >= 4.36: add the attention-implementation attributes the new
            # forward code reads (eager attention is what the pickled BertSelfAttention modules implement anyway).
            for mod in self.bert.modules():
                cfg = getattr(mod, "config", None)
                if cfg is not None and not hasattr(cfg, "_attn_implementation_internal"):
                    cfg._attn_implementation = "eager"
                if mod.__class__.__name__ == "BertModel" and not hasattr(mod, "attn_implementation"):
                    mod.attn_implementation = "eager"
            self.bert.eval()
            self.tok = AutoTokenizer.from_pretrained(TOKENIZER_REPO)
        self.idx2lab = {  # IndicLID_lang_code_dict_reverse in the official code
            0: "asm_Latn", 1: "ben_Latn", 2: "brx_Latn", 3: "guj_Latn", 4: "hin_Latn", 5: "kan_Latn", 6: "kas_Latn",
            7: "kok_Latn", 8: "mai_Latn", 9: "mal_Latn", 10: "mni_Latn", 11: "mar_Latn", 12: "nep_Latn",
            13: "ori_Latn", 14: "pan_Latn", 15: "san_Latn", 16: "snd_Latn", 17: "tam_Latn", 18: "tel_Latn",
            19: "urd_Latn", 20: "eng_Latn", 21: "other", 22: "asm_Beng", 23: "ben_Beng", 24: "brx_Deva",
            25: "doi_Deva", 26: "guj_Gujr", 27: "hin_Deva", 28: "kan_Knda", 29: "kas_Arab", 30: "kas_Deva",
            31: "kok_Deva", 32: "mai_Deva", 33: "mal_Mlym", 34: "mni_Beng", 35: "mni_Meti", 36: "mar_Deva",
            37: "nep_Deva", 38: "ori_Orya", 39: "pan_Guru", 40: "san_Deva", 41: "sat_Olch", 42: "snd_Arab",
            43: "tam_Tamil", 44: "tel_Telu", 45: "urd_Arab"}


_SPECIAL = re.compile(r"[@_!#$%^&*()<>?/\\|}{~:]")
_EN = re.compile(r"[a-zA-Z0-9]")


def roman_share(s: str) -> float:
    """Share of Latin letters/digits among non-space, non-special characters (official char_percent_check)."""
    total = len(s) - len(_SPECIAL.findall(s)) - len(re.findall(r"\s", s))
    return 0.0 if total <= 0 else len(_EN.findall(s)) / total


def predict_units(m: IndicLIDModels, texts, use_bert_flags, bert_batch: int):
    """IndicLID on a list of texts. Returns a DataFrame with the first-stage label (FTN for native text, FTR for
    roman text), its score, and, for roman text with FTR score <= 0.6 and use_bert_flags True, the IndicBERT label.
    label_ftr = first-stage label always; label_2s = IndicBERT label when it ran, else the first-stage label."""
    n = len(texts)
    route = np.empty(n, dtype=object)
    lab1, sc1 = [None] * n, np.zeros(n)
    ro_idx = [i for i, t in enumerate(texts) if roman_share(t) > ROMAN_INPUT_THRESHOLD]
    ro_set = set(ro_idx)
    na_idx = [i for i in range(n) if i not in ro_set]
    t0 = time.time()
    if na_idx:
        labs, probs = m.ftn.predict([texts[i] for i in na_idx])
        for i, l, p in zip(na_idx, labs, probs):
            lab1[i], sc1[i], route[i] = l[0][9:], float(p[0]), "native"
    if ro_idx:
        labs, probs = m.ftr.predict([texts[i] for i in ro_idx])
        for i, l, p in zip(ro_idx, labs, probs):
            lab1[i], sc1[i], route[i] = l[0][9:], float(p[0]), "roman"
    t_ft = time.time() - t0
    lab2, sc2 = [None] * n, np.full(n, np.nan)
    low = [i for i in ro_idx if sc1[i] <= ROMAN_MODEL_THRESHOLD and use_bert_flags[i]]
    t0 = time.time()
    if low and m.bert is not None:
        low.sort(key=lambda i: len(texts[i]))
        with m.torch.no_grad():
            for s in range(0, len(low), bert_batch):
                b = low[s:s + bert_batch]
                enc = m.tok([texts[i] for i in b], return_tensors="pt", padding=True, truncation=True, max_length=512)
                o = m.bert(enc["input_ids"], token_type_ids=enc.get("token_type_ids"),
                           attention_mask=enc["attention_mask"]).logits
                best = o.argmax(1)
                for i, k, lg in zip(b, best.tolist(), o):
                    lab2[i], sc2[i] = m.idx2lab[k], float(lg[k])
    t_bert = time.time() - t0
    df = pd.DataFrame({"route": route, "label_ftr": lab1, "score_ftr": sc1, "bert_label": lab2, "bert_score": sc2})
    df["label_2s"] = df["bert_label"].where(df["bert_label"].notna(), df["label_ftr"])
    return df, t_ft, t_bert, len(low)


# Latin-script Hindi function words / high-frequency colloquialisms. Deliberately excludes strings that are also common
# English or other-language words (main, se, par, ab, to, is, in, us, no, the, an, a, he, me, so, do, am, bar, pal).
HINDI_FW = set("""hai hain nahi nahin nai kya aur mein ke ki ka ko ne bhai bhi kaise kaisa kaisi kyun kyu kab kahan kaun
kaunsa ye yeh wo woh tum aap hum mera meri mere tera teri tere apna apni apne uska uski uske unka unki unke iska iski
iske liye lekin magar toh tha thi hoga hogi honge raha rahi rahe gaya gayi gaye kiya kiye kare karo karna karke
karenge kuch sab bahut bohot zyada jyada sirf abhi phir fir wala wali wale naya nayi naye accha acha achha
dekho dekhiye dekhna dekh chalo bolo sunao suno batao jao aao aaj kal sath saath tak waise vaise
isliye kyunki kyonki agar jab yahan wahan ghar paisa paise logon ek paanch
""".split())  # excluded as English collisions: the, teen, char, log, kar, chal, bol, bata, tab, desi, do, main, par
_TOK = re.compile(r"[a-z]+")


def lexicon_counts(text: str):
    toks = _TOK.findall(text.lower())
    return len(toks), sum(1 for t in toks if t in HINDI_FW)


def schema_pred():
    from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField, StructType)
    return StructType([
        StructField("canonical_id", StringType()), StructField("kind", StringType()),
        StructField("pos", IntegerType()), StructField("route", StringType()),
        StructField("label_ftr", StringType()), StructField("score_ftr", DoubleType()),
        StructField("bert_label", StringType()), StructField("bert_score", DoubleType()),
        StructField("label_2s", StringType()), StructField("lex_tokens", IntegerType()),
        StructField("lex_hits", IntegerType())])


def run_inference(models, texts_sql: str, target_table: str, use_bert_kinds: set, label: str):
    """Stream text units from Spark to the driver in chunks, classify, append to target_table. Returns timing dict."""
    chunk, bert_batch = int(P["chunk_units"]), int(P["bert_batch"])
    stats = dict(units=0, bert_units=0, t_ft=0.0, t_bert=0.0)
    t_start = time.time()
    buf = []

    def flush():
        if not buf:
            return
        b = pd.DataFrame(buf, columns=["canonical_id", "kind", "pos", "text"])
        flags = [k in use_bert_kinds for k in b["kind"]]
        pr, t_ft, t_bert, n_low = predict_units(models, b["text"].tolist(), flags, bert_batch)
        lex = [lexicon_counts(t) for t in b["text"]]
        res = pd.concat([b[["canonical_id", "kind", "pos"]].reset_index(drop=True), pr], axis=1)
        res["lex_tokens"] = [x[0] for x in lex]
        res["lex_hits"] = [x[1] for x in lex]
        res["pos"] = res["pos"].astype("int32")
        res = res.astype({"lex_tokens": "int32", "lex_hits": "int32"})
        res = res.astype(object).where(res.notna(), None)
        sdf = spark.createDataFrame(res.values.tolist(), schema=schema_pred())
        sdf.write.mode("append").saveAsTable(target_table)
        stats["units"] += len(b)
        stats["bert_units"] += n_low
        stats["t_ft"] += t_ft
        stats["t_bert"] += t_bert
        el = time.time() - t_start
        log(f"{label}: {stats['units']:,} units, bert {stats['bert_units']:,}, {el:.0f}s elapsed, "
            f"ft {stats['t_ft']:.0f}s, bert {stats['t_bert']:.0f}s")
        buf.clear()

    for row in spark.sql(texts_sql).toLocalIterator():
        buf.append((row.canonical_id, row.kind, int(row.pos), row.text))
        if len(buf) >= chunk:
            flush()
    flush()
    stats["seconds"] = time.time() - t_start
    return stats


def setup_models(load_bert: bool):
    t0 = time.time()
    RUN_INFO["files"] = download_models(P["model_dir"])
    RUN_INFO["download_s"] = round(time.time() - t0, 1)
    t0 = time.time()
    models = IndicLIDModels(P["model_dir"], load_bert=load_bert)
    RUN_INFO["load_s"] = round(time.time() - t0, 1)
    import importlib.metadata as md
    RUN_INFO["pkgs"] = {p: md.version(p) for p in ("fasttext", "torch", "transformers", "numpy", "pandas")}
    RUN_INFO["release"] = "AI4Bharat/IndicLID v1.0 (2023-05-22)"
    RUN_INFO["tokenizer"] = TOKENIZER_REPO
    try:
        from huggingface_hub import model_info
        RUN_INFO["tokenizer_revision"] = model_info(TOKENIZER_REPO).sha
    except Exception as e:  # noqa: BLE001
        RUN_INFO["tokenizer_revision"] = f"unavailable: {e!r}"[:200]
    RUN_INFO["cpus"] = os.cpu_count()
    return models


# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage probe - egress, model download, speed benchmark (writes nothing)

# COMMAND ----------
if "probe" in stages:
    import requests
    for u in ["https://github.com/AI4Bharat/IndicLID/releases/download/v1.0/indiclid-ftn.zip",
              f"https://huggingface.co/{TOKENIZER_REPO}/resolve/main/config.json"]:
        try:
            r = requests.head(u, allow_redirects=True, timeout=30)
            log(f"egress {u.split('/')[2]}: HTTP {r.status_code}")
        except Exception as e:  # noqa: BLE001
            log(f"egress {u.split('/')[2]}: FAILED {e!r}"[:300])
            raise
    models = setup_models(load_bert=True)
    print(json.dumps(RUN_INFO, indent=1))
    en = ["How to make perfect pasta at home", "Top 10 gaming moments of the year", "Best budget phones under 15000",
          "Daily news bulletin for today", "Learn python programming for beginners full course"]
    hi = ["Taarak Mehta Ka Ooltah Chashmah - Episode 3421", "Bhai ne kya kar diya dekho", "Aaj ka mausam kaisa rahega",
          "Desi Comedy 2.0 | Sabse mazedaar video", "Ghar pe kaise banaye aasan recipe",
          "हिंदी समाचार आज की खबर",
          "Mera naya vlog dekho aur subscribe karo", "tamil movie review super hit"]
    texts = (en + hi) * 250
    t0 = time.time()
    pr, t_ft, t_b, nlow = predict_units(models, texts, [False] * len(texts), 32)
    log(f"first-stage: {len(texts)} units in {time.time() - t0:.1f}s ({len(texts) / (time.time() - t0):.0f}/s), "
        f"{nlow} low-conf")
    print(pr.head(len(en + hi)).assign(text=(en + hi)).to_string())
    low_t = [t for t, s, r in zip(texts, pr["score_ftr"], pr["route"]) if r == "roman" and s <= 0.6][:200] or texts[:200]
    t0 = time.time()
    pr2, _, tb, nl = predict_units(models, low_t, [True] * len(low_t), 32)
    log(f"bert benchmark: {len(low_t)} low-conf texts -> {nl} through BERT in {tb:.1f}s ({nl / max(tb, 1e-9):.1f}/s)")
    # BERT on 200 forced roman texts regardless of confidence
    forced = [t for t in texts if roman_share(t) > 0.5][:200]
    t0 = time.time()
    n = 0
    with models.torch.no_grad():
        for s in range(0, len(forced), 32):
            enc = models.tok(forced[s:s + 32], return_tensors="pt", padding=True, truncation=True, max_length=512)
            models.bert(enc["input_ids"], token_type_ids=enc.get("token_type_ids"), attention_mask=enc["attention_mask"])
            n += len(forced[s:s + 32])
    log(f"bert forced benchmark: {n} texts in {time.time() - t0:.1f}s ({n / (time.time() - t0):.1f}/s)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage pilot - IndicLID on the sample

# COMMAND ----------
if "pilot" in stages:
    models = setup_models(load_bert=True)
    kinds = {k.strip() for k in P["bert_kinds"].split(",") if k.strip()}
    sql = f"""SELECT t.canonical_id, t.kind, t.pos, t.text FROM {out('texts')} t
              JOIN {out('sample')} s ON t.canonical_id = s.canonical_id ORDER BY t.canonical_id, t.kind, t.pos"""
    st = run_inference(models, sql, out("preds"), kinds, "pilot")
    st.update(run_info=json.dumps(RUN_INFO), stage="pilot", bert_kinds=",".join(sorted(kinds)))
    spark.createDataFrame([(json.dumps(st, default=str),)], "log STRING").write.saveAsTable(out("runlog_pilot"))
    display(spark.sql(f"SELECT kind, route, count(*) n FROM {out('preds')} GROUP BY 1, 2 ORDER BY 1, 2"))
    print(json.dumps(st, indent=1, default=str)[:3000])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage full - IndicLID on every IN x eng channel (first stage only unless `full_use_bert=true`)

# COMMAND ----------
if "full" in stages:
    use_bert = P["full_use_bert"].lower() == "true"
    models = setup_models(load_bert=use_bert)
    kinds = {k.strip() for k in P["bert_kinds"].split(",") if k.strip()} if use_bert else set()
    sql = f"""SELECT t.canonical_id, t.kind, t.pos, t.text FROM {out('texts')} t
              WHERE t.kind <> 'recent_desc' ORDER BY t.canonical_id, t.kind, t.pos"""
    st = run_inference(models, sql, out("fullpreds"), kinds, "full")
    st.update(run_info=json.dumps(RUN_INFO), stage="full", use_bert=use_bert)
    spark.createDataFrame([(json.dumps(st, default=str),)], "log STRING").write.saveAsTable(out("runlog_full"))
    print(json.dumps(st, indent=1, default=str)[:3000])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage aggregate - channel table, label shares, agreement
# MAGIC
# MAGIC Unit class from the IndicLID label: `eng_Latn` -> english; `hin_Latn` -> roman_hindi; any other `*_Latn` ->
# MAGIC roman_other_indic; `other` -> other; every non-Latin label -> native_script. Channel label = plurality class over
# MAGIC the channel's title units (`recent_title` + `lid_title`); ties or no titles fall back to the `channel_text` unit's
# MAGIC class; else `undetermined`. Two variants: `2s` (two-stage, IndicBERT where it ran) and `ftr` (first stage only).
# MAGIC Lexicon score = Hindi function-word tokens / Latin word tokens, pooled over title units and the channel text;
# MAGIC `lex_flag` = at least `lex_min_hits` hits and rate >= `lex_min_rate`.

# COMMAND ----------
def unit_class(col: str) -> str:
    return (f"CASE WHEN {col} IS NULL THEN NULL WHEN {col} = 'eng_Latn' THEN 'english' "
            f"WHEN {col} IN ('hin_Latn', 'urd_Latn') THEN 'roman_hindi' WHEN {col} LIKE '%\\\\_Latn' THEN 'roman_other_indic' "
            f"WHEN {col} = 'other' THEN 'other' ELSE 'native_script' END")


def channel_label_sql(preds: str, variant_col: str, suffix: str) -> str:
    """Per-channel plurality label for one variant (label_2s / label_ftr)."""
    cnt = ",\n".join(
        f"sum(CASE WHEN kind IN ('recent_title', 'lid_title') AND cls = '{c}' THEN 1 ELSE 0 END) AS c_{c}"
        for c in CLASSES)
    return f"""
    SELECT canonical_id, n_titles, {', '.join('c_' + c for c in CLASSES)}, ct_class,
           CASE WHEN n_titles = 0 OR n_max > 1 THEN coalesce(ct_class, 'undetermined')
                {' '.join(f"WHEN c_{c} = mx THEN '{c}'" for c in CLASSES)} END AS label_{suffix}
    FROM (
      SELECT *, greatest({', '.join('c_' + c for c in CLASSES)}) AS mx,
             {' + '.join(f"(CASE WHEN c_{c} = greatest({', '.join('c_' + k for k in CLASSES)}) THEN 1 ELSE 0 END)" for c in CLASSES)} AS n_max
      FROM (
        SELECT canonical_id,
               sum(CASE WHEN kind IN ('recent_title', 'lid_title') THEN 1 ELSE 0 END) AS n_titles,
               {cnt},
               max(CASE WHEN kind = 'channel_text' THEN cls END) AS ct_class
        FROM (SELECT canonical_id, kind, {unit_class(variant_col)} AS cls FROM {preds})
        GROUP BY canonical_id))"""


def build_channel_table(preds: str, target: str, base: str, with_weight: bool):
    lex_hits, lex_rate = int(P["lex_min_hits"]), float(P["lex_min_rate"])
    texts = out("texts")
    # Lexicon is computed here, in SQL, from the texts (the per-unit lex_* columns written at inference time used an
    # earlier word list and are not read).
    FW_SQL = ", ".join(f"'{w}'" for w in sorted(HINDI_FW))
    spark.sql(f"""
    {mode} {target} AS
    WITH two AS ({channel_label_sql(preds, 'label_2s', '2s')}),
    one AS (SELECT canonical_id, label_ftr AS label_ftr_only, ct_class AS ct_class_ftr,
                   {', '.join(f'c_{c} AS f_{c}' for c in CLASSES)}
            FROM ({channel_label_sql(preds, 'label_ftr', 'ftr')})),
    lex AS (
      SELECT canonical_id, sum(size(toks)) AS lex_tokens,
             sum(size(filter(toks, x -> array_contains(array({FW_SQL}), x)))) AS lex_hits,
             try_divide(sum(size(filter(toks, x -> array_contains(array({FW_SQL}), x)))), sum(size(toks))) AS lex_rate
      FROM (SELECT canonical_id, regexp_extract_all(lower(text), '[a-z]+', 0) AS toks
            FROM {texts} WHERE kind IN ('recent_title', 'lid_title', 'channel_text')
              AND canonical_id IN (SELECT canonical_id FROM {preds}))
      GROUP BY canonical_id),
    sil AS (SELECT channel_id AS canonical_id, max(channel_language_script_label) AS lid_script_label,
                   max(is_romanized) AS lid_is_romanized, max(is_mixed_language) AS lid_is_mixed
            FROM {P['lid_silver_table']} GROUP BY channel_id)
    SELECT b.canonical_id, b.channel_name, b.country, b.language, b.category, b.wv_status, b.wv, b.decile,
           {'b.weight,' if with_weight else '1.0 AS weight,'}
           two.n_titles, {', '.join(f'two.c_{c}' for c in CLASSES)}, two.ct_class AS ct_class_2s,
           coalesce(two.label_2s, 'undetermined') AS indiclid_label,
           coalesce(one.label_ftr_only, 'undetermined') AS indiclid_label_ftr, {', '.join(f'one.f_{c}' for c in CLASSES)},
           lex.lex_tokens, lex.lex_hits, lex.lex_rate,
           coalesce(lex.lex_hits >= {lex_hits} AND lex.lex_rate >= {lex_rate}, false) AS lex_flag,
           sil.lid_script_label, sil.lid_is_romanized, sil.lid_is_mixed
    FROM {base} b
    LEFT JOIN two ON two.canonical_id = b.canonical_id
    LEFT JOIN one ON one.canonical_id = b.canonical_id
    LEFT JOIN lex ON lex.canonical_id = b.canonical_id
    LEFT JOIN sil ON sil.canonical_id = b.canonical_id
    """)


if "aggregate" in stages:
    build_channel_table(out("preds"), out("channels"), out("sample"), with_weight=True)
    # runlog_full is written only when the full inference loop has finished, so it marks a complete fullpreds table.
    has_full = out("runlog_full").rsplit(".", 1)[1] in {r.tableName for r in spark.sql(
        f"SHOW TABLES IN {schema} LIKE '{prefix}_*'").collect()}
    if has_full:
        build_channel_table(out("fullpreds"), out("full_channels"),
                            f"(SELECT * FROM {out('frame')} WHERE grp = 'eng_IN')", with_weight=False)
    ch = out("channels")
    # label shares: overall (decile 0) and by decile; channels (weighted to the population), views (weighted views)
    parts = []
    for variant, col in (("2s", "indiclid_label"), ("ftr", "indiclid_label_ftr")):
        for dec_sel, grp_by in (("decile", f"decile, {col}"), ("0", col)):  # decile 0 = overall
            parts.append(f"""
            SELECT '{variant}' AS variant, {dec_sel} AS decile, {col} AS label, count(*) AS n_sample,
                   sum(weight) AS n_est, sum(weight * wv) AS wv_est
            FROM {ch} GROUP BY {grp_by}""")
    spark.sql(f"{mode} {out('label_shares')} AS " + " UNION ALL ".join(parts))
    display(spark.sql(f"""
      SELECT variant, label, n_sample, n_est / sum(n_est) OVER (PARTITION BY variant) AS share_channels,
             wv_est / sum(wv_est) OVER (PARTITION BY variant) AS share_views
      FROM {out('label_shares')} WHERE decile = 0 ORDER BY variant, n_sample DESC"""))
    # agreement with the lexicon signal (weighted), and lexicon rate by IndicLID label
    spark.sql(f"""
    {mode} {out('agreement')} AS
    SELECT indiclid_label AS label, lex_flag, count(*) AS n_sample, sum(weight) AS n_est, sum(weight * wv) AS wv_est
    FROM {ch} GROUP BY 1, 2""")
    spark.sql(f"""
    {mode} {out('lexicon')} AS
    SELECT indiclid_label AS label, count(*) AS n_sample, avg(lex_rate) AS mean_lex_rate,
           percentile(lex_rate, 0.5) AS median_lex_rate, avg(CAST(lex_flag AS INT)) AS lex_flag_rate,
           avg(try_divide(c_roman_hindi, n_titles)) AS mean_title_share_roman_hindi,
           corr(lex_rate, try_divide(c_roman_hindi, n_titles)) AS corr_lex_title_share_hindi
    FROM {ch} GROUP BY 1""")
    display(spark.table(out("agreement")).orderBy("label", "lex_flag"))
    display(spark.table(out("lexicon")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage panel - category mix by group (channels and weekly views)
# MAGIC
# MAGIC Groups: `hin_all` (Hindi-labelled, any country), `hin_IN` (IN-declared Hindi, sensitivity), and the IN x eng
# MAGIC channels split by IndicLID label: `eng_IN_english`, `eng_IN_romanised_indic` (roman_hindi + roman_other_indic),
# MAGIC `eng_IN_native` (native script), `eng_IN_other` (other / undetermined), `eng_IN_all` (as labelled). Only channels
# MAGIC with measurable weekly views (zero or positive) count. IN x eng groups use the full run when `source=full`,
# MAGIC else the weighted pilot sample (`weight` = stratum size / sample size).

# COMMAND ----------
if "panel" in stages:
    fr = out("frame")
    full_exists = out("full_channels").rsplit(".", 1)[1] in {r.tableName for r in spark.sql(
        f"SHOW TABLES IN {schema} LIKE '{prefix}_*'").collect()}
    sources = [("pilot_sample_weighted", out("channels"))] + ([("full_run", out("full_channels"))] if full_exists else [])
    spark.createDataFrame([(a, b) for a, b in sources], "source STRING, table STRING").write.saveAsTable(out("panel_sources"))
    meas = "wv_status IN ('zero', 'positive')"
    grp_case = """CASE WHEN indiclid_label = 'english' THEN 'eng_IN_english'
                       WHEN indiclid_label IN ('roman_hindi', 'roman_other_indic') THEN 'eng_IN_romanised_indic'
                       WHEN indiclid_label = 'native_script' THEN 'eng_IN_native' ELSE 'eng_IN_other' END"""
    parts = [f"""
      SELECT 'frame' AS source, 'hin_all' AS grp, category, count(*) AS n_est, count(*) AS n_sample, sum(wv) AS wv_est
      FROM {fr} WHERE language = 'hin' AND {meas} GROUP BY category
      UNION ALL
      SELECT 'frame', 'hin_IN', category, count(*), count(*), sum(wv)
      FROM {fr} WHERE grp = 'hin_IN' AND {meas} GROUP BY category
      UNION ALL
      SELECT 'frame', 'eng_IN_all', category, count(*), count(*), sum(wv)
      FROM {fr} WHERE grp = 'eng_IN' AND {meas} GROUP BY category"""]
    for src, tbl in sources:
        for lcol, tag in (("indiclid_label", "2s"), ("indiclid_label_ftr", "ftr")):
            gc = grp_case.replace("indiclid_label", lcol)
            parts.append(f"""
      SELECT '{src}:{tag}', {gc} AS grp, category, sum(weight), count(*), sum(weight * wv)
      FROM {tbl} WHERE {meas} GROUP BY 2, category""")
        parts.append(f"""
      SELECT '{src}:lex', CASE WHEN lex_flag THEN 'eng_IN_lex_hindi' ELSE 'eng_IN_lex_other' END AS grp,
             category, sum(weight), count(*), sum(weight * wv)
      FROM {tbl} WHERE {meas} GROUP BY 2, category""")
    spark.sql(f"{mode} {out('panel')} AS " + " UNION ALL ".join(parts))
    display(spark.sql(f"""
      SELECT source, grp, category, n_est / sum(n_est) OVER (PARTITION BY source, grp) AS share_channels,
             wv_est / sum(wv_est) OVER (PARTITION BY source, grp) AS share_views, n_sample
      FROM {out('panel')} ORDER BY source, grp, category"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stage validation - 300-channel coding sample (public channel IDs, titles and labels only)
# MAGIC
# MAGIC Strata = IndicLID label (english, roman_hindi, roman_other_indic, native_script; `other` and `undetermined`
# MAGIC pooled into a fifth stratum only if they exist) x views tercile (ntile 3 of weekly views over the pilot sample).
# MAGIC Equal allocation per stratum with shortfalls from small strata redistributed over the others; rows are shuffled
# MAGIC with the fixed seed so the file order does not reveal the stratum.

# COMMAND ----------
if "validation" in stages:
    N_VAL = int(P["valid_n"])
    ch = spark.table(out("channels")).where("wv IS NOT NULL").toPandas()
    ch["tercile"] = pd.qcut(ch["wv"].rank(method="first"), 3, labels=["low", "mid", "high"]).astype(str)
    ch["label_grp"] = ch["indiclid_label"].where(ch["indiclid_label"].isin(CLASSES[:4]), "other_or_undetermined")
    ch = ch[ch["n_titles"].fillna(0) > 0]  # coders need a recent video to watch
    sizes = ch.groupby(["label_grp", "tercile"]).size()
    alloc = {k: 0 for k in sizes.index}
    remaining = N_VAL
    while remaining > 0:
        open_k = [k for k in alloc if alloc[k] < sizes[k]]
        if not open_k:
            break
        per = max(1, remaining // len(open_k))
        for k in open_k:
            add = min(per, sizes[k] - alloc[k], remaining)
            alloc[k] += add
            remaining -= add
            if remaining == 0:
                break
    rng = np.random.default_rng(SEED)
    picked = []
    for k, n in alloc.items():
        pool = ch[(ch["label_grp"] == k[0]) & (ch["tercile"] == k[1])]
        picked.append(pool.iloc[rng.permutation(len(pool))[:n]])
    val = pd.concat(picked).sample(frac=1.0, random_state=SEED).reset_index(drop=True)
    ids = val["canonical_id"].tolist()
    ids_sql = ",".join(f"'{i}'" for i in ids)
    titles = spark.sql(f"""
      SELECT canonical_id, text, kind, pos FROM {out('texts')}
      WHERE canonical_id IN ({ids_sql}) AND kind IN ('recent_title', 'lid_title')""").toPandas()
    titles["k"] = (titles["kind"] != "recent_title").astype(int)
    titles = titles.sort_values(["canonical_id", "k", "pos"])
    top3 = titles.groupby("canonical_id")["text"].apply(lambda s: s.head(3).tolist()).to_dict()
    for i in range(3):
        val[f"recent_video_title_{i + 1}"] = [(top3.get(c, []) + ["", "", ""])[i] for c in val["canonical_id"]]
    val["channel_url"] = "https://www.youtube.com/channel/" + val["canonical_id"]
    val["original_lid_label"] = val["language"].fillna("") + ":" + val["lid_script_label"].fillna("")
    keep = ["canonical_id", "channel_url", "channel_name", "recent_video_title_1", "recent_video_title_2",
            "recent_video_title_3", "indiclid_label", "lex_rate", "original_lid_label", "label_grp", "tercile"]
    v = val[keep].rename(columns={"canonical_id": "channel_id", "channel_name": "channel_title",
                                  "lex_rate": "lexicon_score", "label_grp": "stratum_label",
                                  "tercile": "stratum_views_tercile"})
    spark.createDataFrame(v.astype(object).where(v.notna(), None)).write.saveAsTable(out("validation"))
    print(v.groupby(["stratum_label", "stratum_views_tercile"]).size())
