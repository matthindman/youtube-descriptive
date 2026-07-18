from __future__ import annotations

import argparse
import json
import unittest
from pathlib import Path

from scripts.build_full_corpus_dual_sample_analysis_job import STAGES, build_payload


ROOT = Path(__file__).resolve().parents[2]


class FullCorpusDualSampleAnalysisJobTests(unittest.TestCase):
    def test_analysis_payload_is_ordered_and_uses_registered_compute(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="0601-203643-bkxsqffg",
            notebook_path="/Users/test/analysis",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
            hierarchy_config_path="dbfs:/FileStore/test/hierarchy.yaml",
            topic_remap_path="dbfs:/FileStore/test/remap.yaml",
        )
        payload = build_payload(args)
        tasks = payload["tasks"]
        self.assertEqual(
            [task["task_key"] for task in tasks],
            [f"analysis_{stage}" for stage in STAGES],
        )
        self.assertTrue(all(task["existing_cluster_id"] == args.cluster_id for task in tasks))
        self.assertNotIn("new_cluster", json.dumps(payload))
        self.assertEqual(tasks[1]["depends_on"], [{"task_key": "analysis_allocate"}])
        self.assertEqual(
            tasks[-1]["depends_on"], [{"task_key": "analysis_qa"}]
        )


if __name__ == "__main__":
    unittest.main()
