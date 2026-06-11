import json
import re
import sys

schema = sys.argv[1]
tables = json.load(sys.stdin)

pattern = re.compile(
    r"topic|categor|genre|archetype|primary_topic|topic_top_k_json|ai_label|all_labels|classif|llm|raw_json|json",
    re.IGNORECASE,
)
ignored = re.compile(
    r"language|sentiment|toxicity|lid_|label_raw|label_[0-9]|iso639|script",
    re.IGNORECASE,
)
channel_keys = {"channel_id", "canonical_id", "youtube_channel_id", "yt_channel_id", "channelid"}

for table in tables:
    full_name = table.get("full_name") or f"dev_sean.{schema}.{table.get('name', '')}"
    columns = [column.get("name", "") for column in table.get("columns", [])]
    matches = [column for column in columns if pattern.search(column) and not ignored.search(column)]
    keys = [column for column in columns if column.lower() in channel_keys]
    if matches or pattern.search(full_name):
        print(f"{full_name}\tcolumns={', '.join(matches)}\tkeys={', '.join(keys)}")
