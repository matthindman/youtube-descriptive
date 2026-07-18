from __future__ import annotations

import argparse
import json
import unittest
from pathlib import Path

from scripts.build_full_corpus_dual_sample_simulation_job import build_payload


ROOT = Path(__file__).resolve().parents[2]


class FullCorpusDualSampleSimulationJobTests(unittest.TestCase):
    def test_simulation_payload_uses_registered_existing_compute(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="0601-203643-bkxsqffg",
            notebook_path="/Users/test/simulation",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
        )
        payload = build_payload(args)
        self.assertEqual(len(payload["tasks"]), 1)
        self.assertEqual(payload["tasks"][0]["existing_cluster_id"], args.cluster_id)
        self.assertNotIn("new_cluster", json.dumps(payload))


if __name__ == "__main__":
    unittest.main()
