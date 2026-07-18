#!/usr/bin/env python3
"""Build a Databricks Jobs submit payload for the registered dual sample."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


STAGES = ("build_frame", "simulate_design", "draw_samples", "stage_enrichment")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cluster-id", required=True)
    parser.add_argument("--notebook-path", required=True)
    parser.add_argument("--dbfs-config-path", required=True)
    parser.add_argument("--start-stage", choices=STAGES, default=STAGES[0])
    return parser.parse_args()


def build_payload(args: argparse.Namespace) -> dict[str, object]:
    config = json.loads(args.config.read_text(encoding="utf-8"))
    if args.cluster_id != config["execution"]["existing_cluster_id"]:
        raise ValueError("cluster-id does not match the registered existing cluster")
    start_stage = getattr(args, "start_stage", STAGES[0])
    selected_stages = STAGES[STAGES.index(start_stage) :]
    tasks: list[dict[str, object]] = []
    for index, stage in enumerate(selected_stages):
        task: dict[str, object] = {
            "task_key": stage,
            "existing_cluster_id": args.cluster_id,
            "timeout_seconds": 0,
            "notebook_task": {
                "notebook_path": args.notebook_path,
                "base_parameters": {
                    "stage": stage,
                    "design_config_path": args.dbfs_config_path,
                },
            },
        }
        if index:
            task["depends_on"] = [{"task_key": selected_stages[index - 1]}]
        tasks.append(task)
    return {
        "run_name": f"{config['design_version']}_from_{start_stage}",
        "tasks": tasks,
    }


def main() -> None:
    args = parse_args()
    payload = build_payload(args)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
