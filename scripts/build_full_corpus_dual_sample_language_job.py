#!/usr/bin/env python3
"""Build the gated dual-LID and DeepSeek language job payload."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cluster-id", required=True)
    parser.add_argument("--orchestrator-path", required=True)
    parser.add_argument("--lid-path", required=True)
    parser.add_argument("--llm-path", required=True)
    parser.add_argument("--dbfs-config-path", required=True)
    parser.add_argument("--sample-phase", choices=("all", "pps", "remainder", "combine"), default="all")
    return parser.parse_args()


def build_payload(args: argparse.Namespace) -> dict[str, object]:
    config = json.loads(args.config.read_text(encoding="utf-8"))
    if args.cluster_id != config["execution"]["existing_cluster_id"]:
        raise ValueError("cluster-id does not match the registered existing cluster")
    catalog = config["output_catalog"]
    schema = config["output_schema"]
    prefix = config["output_prefix"]
    language = config["language"]
    recent_videos = int(config["collection"]["recent_videos_per_channel"])
    sample_phase = getattr(args, "sample_phase", "all")
    if sample_phase == "combine":
        return {
            "run_name": f"{config['design_version']}_language_combine",
            "tasks": [
                {
                    "task_key": "publish_combined_language",
                    "existing_cluster_id": args.cluster_id,
                    "timeout_seconds": 0,
                    "notebook_task": {
                        "notebook_path": args.orchestrator_path,
                        "base_parameters": {
                            "design_config_path": args.dbfs_config_path,
                            "sample_phase": "all",
                            "stage": "publish_combined",
                        },
                    },
                }
            ],
        }
    phase_suffix = "" if sample_phase == "all" else f"_{sample_phase}"
    buckets = str(language["inference_hash_buckets"])
    lid_prefix = f"{prefix}_lid{phase_suffix}"
    llm_prefix = f"{prefix}_deepseek_flash{phase_suffix}"
    lid_run_id = f"{language['lid_run_id']}{phase_suffix}"
    llm_run_id = f"{language['llm_run_id']}{phase_suffix}"
    source_channels = f"{prefix}_lid_source_channels{phase_suffix}"
    source_videos = f"{prefix}_lid_source_videos{phase_suffix}"
    routing_table = f"{prefix}_language_routing_comparison{phase_suffix}"

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
        "channels_table": source_channels,
        "videos_table": source_videos,
        "output_catalog": catalog,
        "output_schema": schema,
        "run_id": lid_run_id,
        "inference_hash_buckets": buckets,
        "bucket_start": "0",
        "bucket_end": str(int(buckets) - 1),
        "channel_id_column": "channel_id",
        "video_id_column": "video_id",
        "channel_name_column": "channel_name",
        "channel_description_column": "channel_description",
        "video_title_column": "video_title",
        "video_description_column": "video_description",
        "video_rank_column": "position",
        "video_rank_ascending": "true",
        "videos_per_channel": str(recent_videos),
        "enable_openlid": "true",
        "enable_glotlid": "true",
        "glotlid_mode": "all_valid_segments",
        "glotlid_preprocessing_mode": "match_openlid",
        "prediction_output_mode": "compact",
        "production_mode": "true",
        "run_heavy_qa": "true",
        "enable_notebook_displays": "false",
        "create_validation_samples": "true",
        "run_ablation_aggregations": "false",
        "optimize_after_write": "false",
        "download_model_if_missing": "false",
        "min_num_partitions": "256",
        "max_num_partitions": "4096",
        "target_segments_per_partition": "25000",
        "checkpoint_dir": f"dbfs:/tmp/yt_lid_v3/{lid_run_id}/checkpoints",
        "update_source_detected_language": "false",
    }
    for widget_suffix, table_suffix in output_suffixes.items():
        lid_parameters[f"output_{widget_suffix}_table"] = f"{lid_prefix}_{table_suffix}"

    llm_parameters = {
        "catalog": catalog,
        "schema": schema,
        "comparison_table": routing_table,
        "segments_input_table": f"{lid_prefix}_segments_input",
        "channels_table": f"{lid_prefix}_channels",
        "channel_text_features_table": f"{lid_prefix}_channel_text_features",
        "hindi_indic_audit_table": f"{lid_prefix}_hindi_indic_audit_candidates",
        "source_channels_table": source_channels,
        "source_videos_table": source_videos,
        "run_id": llm_run_id,
        "source_run_id": lid_run_id,
        "inference_hash_buckets": buckets,
        "panel_requests_table": f"{llm_prefix}_llm_requests",
        "panel_batch_jobs_table": f"{llm_prefix}_llm_batch_jobs",
        "panel_raw_results_table": f"{llm_prefix}_llm_raw_results",
        "panel_verdicts_table": f"{llm_prefix}_llm_verdicts",
        "panel_model_agreement_table": f"{llm_prefix}_llm_model_agreement",
        "panel_run_progress_table": f"{llm_prefix}_llm_run_progress",
        "routing_mode": "residual_panel",
        "route_disagreement": "true",
        "route_unresolved_tail": "false",
        "route_shared_bias_english_indic": "false",
        "route_unclassified": "true",
        "route_agreement_audit": "false",
        "exclude_arabic_family_pairs": "false",
        "max_routed_channels": "0",
        "models_json": json.dumps(
            [{"provider": "deepseek", "model": language["llm_model"], "tier": "small"}]
        ),
        "max_output_tokens": "2000",
        "temperature": "",
        "prompt_version": language["llm_prompt_version"],
        "apply_llm_calibration": "true",
        "deepseek_thinking_type": "disabled",
        "deepseek_reasoning_effort": "",
        "deepseek_max_output_tokens": "600",
        "deepseek_max_workers": "32",
        "deepseek_request_timeout_seconds": "60",
        "deepseek_max_retries": "2",
        "deepseek_pending_batch_size": "500",
        "deepseek_direct_streaming": "true",
        "deepseek_delete_request_jsonl_after_submit": "true",
        "deepseek_direct_submit_from_requests_table": "true",
        "submit_batches": "true",
        "submit_provider_filter": "deepseek",
        "submit_model_filter": language["llm_model"],
        "skip_existing_submitted_batches": "true",
        "import_results": "true",
        "reuse_existing_requests_on_submit": "true",
        "reuse_existing_requests_on_import": "true",
        "panel_majority_mode": "reached_models",
        "min_panel_votes_for_majority": "1",
        "panel_majority_vote_basis": "normalized_base_iso",
        "secret_scope": language["secret_scope"],
        "deepseek_secret_key": language["deepseek_secret_key"],
    }
    common = {"design_config_path": args.dbfs_config_path, "sample_phase": sample_phase}
    return {
        "run_name": f"{config['design_version']}_language_{sample_phase}",
        "tasks": [
            {
                "task_key": "language_preflight",
                "existing_cluster_id": args.cluster_id,
                "timeout_seconds": 0,
                "notebook_task": {
                    "notebook_path": args.orchestrator_path,
                    "base_parameters": {**common, "stage": "preflight"},
                },
            },
            {
                "task_key": "dual_lid",
                "depends_on": [{"task_key": "language_preflight"}],
                "existing_cluster_id": args.cluster_id,
                "timeout_seconds": 0,
                "notebook_task": {"notebook_path": args.lid_path, "base_parameters": lid_parameters},
            },
            {
                "task_key": "prepare_routing",
                "depends_on": [{"task_key": "dual_lid"}],
                "existing_cluster_id": args.cluster_id,
                "timeout_seconds": 0,
                "notebook_task": {
                    "notebook_path": args.orchestrator_path,
                    "base_parameters": {**common, "stage": "prepare_routing"},
                },
            },
            {
                "task_key": "deepseek_fallback",
                "depends_on": [{"task_key": "prepare_routing"}],
                "existing_cluster_id": args.cluster_id,
                "timeout_seconds": 0,
                "notebook_task": {"notebook_path": args.llm_path, "base_parameters": llm_parameters},
            },
            {
                "task_key": "publish_language",
                "depends_on": [{"task_key": "deepseek_fallback"}],
                "existing_cluster_id": args.cluster_id,
                "timeout_seconds": 0,
                "notebook_task": {
                    "notebook_path": args.orchestrator_path,
                    "base_parameters": {**common, "stage": "publish"},
                },
            },
        ],
    }


def main() -> None:
    args = parse_args()
    args.output.write_text(json.dumps(build_payload(args), indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
