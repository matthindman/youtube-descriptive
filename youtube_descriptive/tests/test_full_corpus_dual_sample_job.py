from __future__ import annotations

import argparse
import json
import unittest
from pathlib import Path

from scripts.build_full_corpus_dual_sample_job import STAGES, build_payload


ROOT = Path(__file__).resolve().parents[2]


class FullCorpusDualSampleJobTests(unittest.TestCase):
    def test_payload_uses_only_registered_cluster_and_ordered_stages(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="0601-203643-bkxsqffg",
            notebook_path="/Users/test/dual_sample",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
            start_stage=STAGES[0],
        )
        payload = build_payload(args)
        tasks = payload["tasks"]
        self.assertEqual([task["task_key"] for task in tasks], list(STAGES))
        self.assertTrue(all(task["existing_cluster_id"] == args.cluster_id for task in tasks))
        self.assertNotIn("new_cluster", json.dumps(payload))
        for index, task in enumerate(tasks):
            if index == 0:
                self.assertNotIn("depends_on", task)
            else:
                self.assertEqual(task["depends_on"], [{"task_key": STAGES[index - 1]}])

    def test_payload_can_resume_from_draw_without_rebuilding_frame(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="0601-203643-bkxsqffg",
            notebook_path="/Users/test/dual_sample",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
            start_stage="draw_samples",
        )
        tasks = build_payload(args)["tasks"]
        self.assertEqual([task["task_key"] for task in tasks], ["draw_samples", "stage_enrichment"])
        self.assertNotIn("depends_on", tasks[0])
        self.assertEqual(tasks[1]["depends_on"], [{"task_key": "draw_samples"}])

    def test_payload_rejects_unregistered_compute(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="new-cluster",
            notebook_path="/Users/test/dual_sample",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
            start_stage=STAGES[0],
        )
        with self.assertRaisesRegex(ValueError, "registered existing cluster"):
            build_payload(args)


if __name__ == "__main__":
    unittest.main()
