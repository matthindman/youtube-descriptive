# Databricks notebook source
"""Fail before prompt construction when the registered DeepSeek account is unavailable."""

from __future__ import annotations

import json

import requests


try:
    dbutils.widgets.text(
        "design_config_path",
        "dbfs:/FileStore/youtube_descriptive/full_corpus_dual_sample_20260717_v1.json",
    )
except Exception:
    pass

CONFIG_PATH = dbutils.widgets.get("design_config_path").strip()
CONFIG = json.loads(dbutils.fs.head(CONFIG_PATH, 1024 * 1024))
LANGUAGE = CONFIG["language"]
API_KEY = dbutils.secrets.get(
    scope=LANGUAGE["secret_scope"], key=LANGUAGE["deepseek_secret_key"]
)

response = requests.get(
    "https://api.deepseek.com/user/balance",
    headers={
        "Accept": "application/json",
        "Authorization": f"Bearer {API_KEY}",
    },
    timeout=30,
)
try:
    payload = response.json()
except Exception:
    payload = {"raw_response": response.text[:500]}

result = {
    "status_code": response.status_code,
    "is_available": payload.get("is_available") if isinstance(payload, dict) else None,
    "balance_infos": payload.get("balance_infos", []) if isinstance(payload, dict) else [],
    "error": payload.get("error") if isinstance(payload, dict) else None,
}
print("DEEPSEEK BALANCE PREFLIGHT:", json.dumps(result, sort_keys=True))
if response.status_code != 200 or result["is_available"] is not True:
    raise RuntimeError(
        "DeepSeek account is unavailable; stopping before prompt construction: "
        + json.dumps(result, sort_keys=True)
    )

print("DEEPSEEK BALANCE PREFLIGHT: PASS")
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
