#!/usr/bin/env python3
"""Build the human-validation topic-calibration job payload."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cluster-id", required=True)
    parser.add_argument("--notebook-path", required=True)
    parser.add_argument("--dbfs-config-path", required=True)
    return parser.parse_args()


def build_payload(args: argparse.Namespace) -> dict[str, object]:
    config = json.loads(args.config.read_text(encoding="utf-8"))
    if args.cluster_id != config["execution"]["existing_cluster_id"]:
        raise ValueError("cluster-id does not match the registered existing cluster")
    return {
        "run_name": f"{config['design_version']}_topic_calibration",
        "tasks": [
            {
                "task_key": "topic_calibration",
                "existing_cluster_id": args.cluster_id,
                "timeout_seconds": 0,
                "notebook_task": {
                    "notebook_path": args.notebook_path,
                    "base_parameters": {"design_config_path": args.dbfs_config_path},
                },
            }
        ],
    }


def main() -> None:
    args = parse_args()
    args.output.write_text(json.dumps(build_payload(args), indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
