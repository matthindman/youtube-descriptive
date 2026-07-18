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


if __name__ == "__main__":
    unittest.main()
