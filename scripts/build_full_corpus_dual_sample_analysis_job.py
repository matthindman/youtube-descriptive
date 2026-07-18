#!/usr/bin/env python3
"""Build the ordered post-enrichment analysis job payload."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


STAGES = ("allocate", "estimate", "qa")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cluster-id", required=True)
    parser.add_argument("--notebook-path", required=True)
    parser.add_argument("--dbfs-config-path", required=True)
    parser.add_argument("--hierarchy-config-path", required=True)
    parser.add_argument("--topic-remap-path", required=True)
    return parser.parse_args()


def build_payload(args: argparse.Namespace) -> dict[str, object]:
    config = json.loads(args.config.read_text(encoding="utf-8"))
    if args.cluster_id != config["execution"]["existing_cluster_id"]:
        raise ValueError("cluster-id does not match the registered existing cluster")
    tasks: list[dict[str, object]] = []
    for index, stage in enumerate(STAGES):
        task: dict[str, object] = {
            "task_key": f"analysis_{stage}",
            "existing_cluster_id": args.cluster_id,
            "timeout_seconds": 0,
            "notebook_task": {
                "notebook_path": args.notebook_path,
                "base_parameters": {
                    "stage": stage,
                    "design_config_path": args.dbfs_config_path,
                    "hierarchy_config_path": args.hierarchy_config_path,
                    "topic_remap_path": args.topic_remap_path,
                },
            },
        }
        if index:
            task["depends_on"] = [{"task_key": f"analysis_{STAGES[index - 1]}"}]
        tasks.append(task)
    return {"run_name": f"{config['design_version']}_analysis", "tasks": tasks}


def main() -> None:
    args = parse_args()
    args.output.write_text(json.dumps(build_payload(args), indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
