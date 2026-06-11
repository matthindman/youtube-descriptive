# Databricks notebook source
# MAGIC %md
# MAGIC # Terminate Databricks cluster
# MAGIC
# MAGIC Final cleanup task for long-running jobs. This notebook calls the Databricks Clusters API to terminate
# MAGIC the configured all-purpose cluster. It should run as an `ALL_DONE` task after the analysis task.

# COMMAND ----------
import json
import os
import urllib.request

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


_create_text_widget("cluster_id", "0601-203643-bkxsqffg")
_create_text_widget("reason", "full_too_lid_job_cleanup")

CLUSTER_ID = _get_widget("cluster_id", "0601-203643-bkxsqffg").strip()
REASON = _get_widget("reason", "full_too_lid_job_cleanup").strip()

if not CLUSTER_ID:
    raise ValueError("cluster_id is required.")

# COMMAND ----------
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_url = context.apiUrl().get().rstrip("/")
api_token = context.apiToken().get()

payload = json.dumps({"cluster_id": CLUSTER_ID}).encode("utf-8")
request = urllib.request.Request(
    f"{api_url}/api/2.0/clusters/delete",
    data=payload,
    headers={
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    },
    method="POST",
)

print(f"Requesting async termination for cluster {CLUSTER_ID}; reason={REASON}")
with urllib.request.urlopen(request, timeout=30) as response:
    body = response.read().decode("utf-8")
    status = response.status

result = {
    "cluster_id": CLUSTER_ID,
    "reason": REASON,
    "api_status": status,
    "api_response": body,
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
