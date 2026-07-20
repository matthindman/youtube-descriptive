#!/usr/bin/env python3
"""Build the paired recent-video LID cutoff experiment job payload."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cluster-id", required=True)
    parser.add_argument("--experiment-path", required=True)
    parser.add_argument("--lid-path", required=True)
    parser.add_argument("--dbfs-config-path", required=True)
    return parser.parse_args()


def build_payload(args: argparse.Namespace) -> dict[str, object]:
    config = json.loads(args.config.read_text(encoding="utf-8"))
    if args.cluster_id != config["execution"]["existing_cluster_id"]:
        raise ValueError("cluster-id does not match the registered existing cluster")
    catalog = config["output_catalog"]
    schema = config["output_schema"]
    prefix = config["output_prefix"]
    language = config["language"]
    experiment_prefix = f"{prefix}_lid_video_cutoff"
    lid_prefix = f"{experiment_prefix}_lid"
    run_id = f"{language['lid_run_id']}_video_cutoff_20260720_v1"
    buckets = int(language["inference_hash_buckets"])
    output_suffixes = {
        "segments_input": "segments_input",
        "openlid_segments": "openlid_segments",
        "glotlid_segments": "glotlid_segments",
        "glotlid_native_segments": "glotlid_native_segments",
        "openlid_compact": "openlid_predictions_compact",
        "glotlid_compact": "glotlid_predictions_compact",
        "glotlid_native_compact": "glotlid_native_predictions_compact",
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
        "hindi_indic_audit": "hindi_indic_audit_candidates",
        "suspect_tail_audit": "suspect_tail_audit_sample",
        "high_risk_redirect": "high_risk_redirect_diagnostic",
        "manual_validation_sample": "manual_validation_sample",
        "unclassified_audit": "unclassified_audit",
        "source_language_confusion": "source_language_confusion",
        "dedupe_qa": "dedupe_qa",
        "preflight_estimate": "preflight_estimate",
        "ablation_summary": "ablation_summary",
        "run_progress": "run_progress",
    }
    lid_parameters = {
        "catalog": catalog,
        "schema": schema,
        "channels_table": f"{experiment_prefix}_sample_channels",
        "videos_table": f"{experiment_prefix}_sample_videos",
        "output_catalog": catalog,
        "output_schema": schema,
        "run_id": run_id,
        "inference_hash_buckets": str(buckets),
        "bucket_start": "0",
        "bucket_end": str(buckets - 1),
        "channel_id_column": "channel_id",
        "video_id_column": "video_id",
        "channel_name_column": "channel_name",
        "channel_description_column": "channel_description",
        "video_title_column": "video_title",
        "video_description_column": "video_description",
        "video_rank_column": "position",
        "video_rank_ascending": "true",
        "videos_per_channel": str(max(language["video_cutoff_candidates"])),
        "enable_openlid": "true",
        "enable_glotlid": "true",
        "glotlid_mode": "all_valid_segments",
        "glotlid_preprocessing_mode": "match_openlid",
        "prediction_output_mode": "compact",
        "production_mode": "true",
        "run_heavy_qa": "false",
        "enable_notebook_displays": "false",
        "create_validation_samples": "false",
        "run_ablation_aggregations": "false",
        "optimize_after_write": "false",
        "download_model_if_missing": "false",
        "min_num_partitions": "256",
        "max_num_partitions": "4096",
        "target_segments_per_partition": "25000",
        "checkpoint_dir": f"dbfs:/tmp/yt_lid_v3/{run_id}/checkpoints",
        "update_source_detected_language": "false",
    }
    for widget_suffix, table_suffix in output_suffixes.items():
        lid_parameters[f"output_{widget_suffix}_table"] = f"{lid_prefix}_{table_suffix}"
    common = {"design_config_path": args.dbfs_config_path}
    return {
        "run_name": f"{config['design_version']}_lid_video_cutoff",
        "tasks": [
            {
                "task_key": "prepare_cutoff_sample",
                "existing_cluster_id": args.cluster_id,
                "timeout_seconds": 0,
                "notebook_task": {
                    "notebook_path": args.experiment_path,
                    "base_parameters": {**common, "stage": "prepare"},
                },
            },
            {
                "task_key": "dual_lid_50_videos",
                "depends_on": [{"task_key": "prepare_cutoff_sample"}],
                "existing_cluster_id": args.cluster_id,
                "timeout_seconds": 0,
                "notebook_task": {"notebook_path": args.lid_path, "base_parameters": lid_parameters},
            },
            {
                "task_key": "analyze_cutoffs",
                "depends_on": [{"task_key": "dual_lid_50_videos"}],
                "existing_cluster_id": args.cluster_id,
                "timeout_seconds": 0,
                "notebook_task": {
                    "notebook_path": args.experiment_path,
                    "base_parameters": {**common, "stage": "analyze"},
                },
            },
        ],
    }


def main() -> None:
    args = parse_args()
    args.output.write_text(json.dumps(build_payload(args), indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
