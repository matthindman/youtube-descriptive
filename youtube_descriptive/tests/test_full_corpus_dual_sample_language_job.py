from __future__ import annotations

import argparse
import json
import unittest
from pathlib import Path

from scripts.build_full_corpus_dual_sample_language_job import build_payload


ROOT = Path(__file__).resolve().parents[2]


class FullCorpusDualSampleLanguageJobTests(unittest.TestCase):
    def test_language_payload_is_gated_and_uses_registered_compute(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="0601-203643-bkxsqffg",
            orchestrator_path="/Users/test/orchestrator",
            lid_path="/Users/test/lid",
            llm_path="/Users/test/llm",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
            sample_phase="all",
            start_at="preflight",
            run_through="full",
        )
        payload = build_payload(args)
        tasks = payload["tasks"]
        self.assertEqual(
            [task["task_key"] for task in tasks],
            ["language_preflight", "dual_lid", "prepare_routing", "deepseek_fallback", "publish_language"],
        )
        self.assertTrue(all(task["existing_cluster_id"] == args.cluster_id for task in tasks))
        self.assertNotIn("new_cluster", json.dumps(payload))
        self.assertEqual(tasks[1]["depends_on"], [{"task_key": "language_preflight"}])
        llm_parameters = tasks[3]["notebook_task"]["base_parameters"]
        self.assertEqual(llm_parameters["submit_provider_filter"], "deepseek")
        self.assertEqual(llm_parameters["secret_scope"], "youtube-llm-keys")
        self.assertEqual(llm_parameters["prompt_max_chars"], "6000")
        self.assertEqual(llm_parameters["max_video_titles"], "12")
        self.assertEqual(llm_parameters["max_video_descriptions"], "4")
        lid_parameters = tasks[1]["notebook_task"]["base_parameters"]
        self.assertEqual(lid_parameters["video_rank_column"], "position")
        self.assertEqual(lid_parameters["video_rank_ascending"], "true")
        self.assertEqual(lid_parameters["videos_per_channel"], "50")

    def test_pps_phase_has_distinct_sources_runs_and_outputs(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="0601-203643-bkxsqffg",
            orchestrator_path="/Users/test/orchestrator",
            lid_path="/Users/test/lid",
            llm_path="/Users/test/llm",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
            sample_phase="pps",
            start_at="preflight",
            run_through="full",
        )
        tasks = build_payload(args)["tasks"]
        lid = tasks[1]["notebook_task"]["base_parameters"]
        llm = tasks[3]["notebook_task"]["base_parameters"]
        self.assertEqual(lid["channels_table"], "yt_dual_sample_20260717_v1_lid_source_channels_pps")
        self.assertEqual(lid["run_id"], "dual_sample_tail_20260717_v1_pps")
        self.assertEqual(
            llm["comparison_table"],
            "yt_dual_sample_20260717_v1_language_routing_comparison_pps",
        )
        self.assertEqual(llm["run_id"], "dual_sample_tail_20260717_v1_deepseek_flash_pps")
        self.assertTrue(
            all(
                task["notebook_task"]["base_parameters"].get("sample_phase") == "pps"
                for task in (tasks[0], tasks[2], tasks[4])
            )
        )

    def test_combine_phase_only_publishes_the_nonoverlapping_union(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="0601-203643-bkxsqffg",
            orchestrator_path="/Users/test/orchestrator",
            lid_path="/Users/test/lid",
            llm_path="/Users/test/llm",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
            sample_phase="combine",
            start_at="preflight",
            run_through="full",
        )
        payload = build_payload(args)
        self.assertEqual(len(payload["tasks"]), 1)
        task = payload["tasks"][0]
        self.assertEqual(task["task_key"], "publish_combined_language")
        self.assertEqual(task["notebook_task"]["base_parameters"]["stage"], "publish_combined")
        self.assertNotIn("new_cluster", json.dumps(payload))

    def test_lid_only_mode_stops_before_routing_and_deepseek(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="0601-203643-bkxsqffg",
            orchestrator_path="/Users/test/orchestrator",
            lid_path="/Users/test/lid",
            llm_path="/Users/test/llm",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
            sample_phase="remainder",
            start_at="preflight",
            run_through="lid",
        )
        payload = build_payload(args)
        self.assertEqual(
            [task["task_key"] for task in payload["tasks"]],
            ["language_preflight", "dual_lid"],
        )
        self.assertEqual(
            payload["tasks"][1]["notebook_task"]["base_parameters"]["run_id"],
            "dual_sample_tail_20260717_v1_remainder",
        )
        self.assertNotIn("deepseek_fallback", json.dumps(payload))

    def test_routing_start_resumes_completed_lid_without_rerunning_inference(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="0601-203643-bkxsqffg",
            orchestrator_path="/Users/test/orchestrator",
            lid_path="/Users/test/lid",
            llm_path="/Users/test/llm",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
            sample_phase="pps",
            start_at="routing",
            run_through="full",
        )
        payload = build_payload(args)
        self.assertEqual(
            [task["task_key"] for task in payload["tasks"]],
            ["prepare_routing", "deepseek_fallback", "publish_language"],
        )
        self.assertNotIn("depends_on", payload["tasks"][0])
        self.assertNotIn("dual_lid", json.dumps(payload))

    def test_llm_request_cache_is_bound_to_the_prompt_payload(self) -> None:
        source = (ROOT / "youtube_descriptive" / "src" / "03_language_llm_panel_databricks.py").read_text(
            encoding="utf-8"
        )
        self.assertIn('"prompt_fingerprint"', source)
        self.assertIn('F.col("prompt_fingerprint")', source)
        self.assertIn("request_cache_matches", source)
        self.assertIn("current_identity.join", source)

    def test_deepseek_account_errors_stop_remaining_chunks_and_retry_cleanly(self) -> None:
        source = (ROOT / "youtube_descriptive" / "src" / "03_language_llm_panel_databricks.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("class DeepSeekFatalProviderError", source)
        self.assertIn("fatal_status_counts = {401: 0, 402: 0, 403: 0}", source)
        self.assertIn("if isinstance(e, DeepSeekFatalProviderError):", source)
        self.assertIn("os.path.exists(result_path) and pending_lines", source)


if __name__ == "__main__":
    unittest.main()
