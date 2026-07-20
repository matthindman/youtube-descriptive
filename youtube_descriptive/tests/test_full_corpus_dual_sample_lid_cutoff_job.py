from __future__ import annotations

import argparse
import json
import unittest
from pathlib import Path

from scripts.build_full_corpus_dual_sample_lid_cutoff_job import build_payload


ROOT = Path(__file__).resolve().parents[2]


class FullCorpusDualSampleLidCutoffJobTests(unittest.TestCase):
    def test_payload_uses_registered_cluster_and_all_50_recent_videos(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="0601-203643-bkxsqffg",
            experiment_path="/Users/test/cutoff",
            lid_path="/Users/test/lid",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
        )
        payload = build_payload(args)
        tasks = payload["tasks"]
        self.assertEqual(
            [task["task_key"] for task in tasks],
            ["prepare_cutoff_sample", "dual_lid_50_videos", "analyze_cutoffs"],
        )
        self.assertTrue(all(task["existing_cluster_id"] == args.cluster_id for task in tasks))
        self.assertNotIn("new_cluster", json.dumps(payload))
        lid = tasks[1]["notebook_task"]["base_parameters"]
        self.assertEqual(lid["videos_per_channel"], "50")
        self.assertEqual(lid["video_rank_column"], "position")
        self.assertEqual(lid["video_rank_ascending"], "true")
        self.assertEqual(lid["glotlid_mode"], "all_valid_segments")


if __name__ == "__main__":
    unittest.main()
