from __future__ import annotations

import argparse
import json
import unittest
from pathlib import Path

from scripts.build_full_corpus_dual_sample_collection_job import STAGES, build_payload


ROOT = Path(__file__).resolve().parents[2]


class FullCorpusDualSampleCollectionJobTests(unittest.TestCase):
    def test_collection_payload_is_ordered_and_never_embeds_a_secret_value(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="0601-203643-bkxsqffg",
            notebook_path="/Users/test/collection",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
            youtube_api_secret_scope="scope-name",
            youtube_api_secret_key="key-name",
        )
        payload = build_payload(args)
        tasks = payload["tasks"]
        self.assertEqual([task["task_key"] for task in tasks], list(STAGES))
        self.assertTrue(all(task["existing_cluster_id"] == args.cluster_id for task in tasks))
        self.assertNotIn("new_cluster", json.dumps(payload))
        self.assertNotIn("secret-value", json.dumps(payload))
        self.assertEqual(tasks[1]["depends_on"], [{"task_key": "collect_channels"}])


if __name__ == "__main__":
    unittest.main()
