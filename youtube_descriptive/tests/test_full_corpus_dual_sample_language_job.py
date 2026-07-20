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
        )
        payload = build_payload(args)
        self.assertEqual(len(payload["tasks"]), 1)
        task = payload["tasks"][0]
        self.assertEqual(task["task_key"], "publish_combined_language")
        self.assertEqual(task["notebook_task"]["base_parameters"]["stage"], "publish_combined")
        self.assertNotIn("new_cluster", json.dumps(payload))


if __name__ == "__main__":
    unittest.main()
