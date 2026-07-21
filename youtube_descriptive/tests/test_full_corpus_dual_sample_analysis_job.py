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

    def test_analysis_launcher_uses_portable_mktemp_template(self) -> None:
        launcher = (ROOT / "scripts" / "run_full_corpus_dual_sample_analysis.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("full_corpus_dual_sample_analysis.XXXXXX\")", launcher)
        self.assertNotIn("full_corpus_dual_sample_analysis.XXXXXX.json", launcher)

    def test_attention_pps_mode_is_forwarded_to_every_stage(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="0601-203643-bkxsqffg",
            notebook_path="/Users/test/analysis",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
            hierarchy_config_path="dbfs:/FileStore/test/hierarchy.yaml",
            topic_remap_path="dbfs:/FileStore/test/remap.yaml",
            analysis_mode="attention_pps",
        )
        payload = build_payload(args)
        self.assertTrue(
            payload["run_name"].endswith("_analysis_attention_pps_from_allocate")
        )
        self.assertTrue(
            all(
                task["notebook_task"]["base_parameters"]["analysis_mode"]
                == "attention_pps"
                for task in payload["tasks"]
            )
        )

        launcher = (
            ROOT / "scripts" / "run_full_corpus_dual_sample_analysis.sh"
        ).read_text(encoding="utf-8")
        self.assertIn('ANALYSIS_MODE="${ANALYSIS_MODE:-full}"', launcher)
        self.assertIn('--analysis-mode "${ANALYSIS_MODE}"', launcher)

    def test_restart_from_estimate_preserves_order_without_missing_dependency(self) -> None:
        args = argparse.Namespace(
            config=ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json",
            output=Path("unused.json"),
            cluster_id="0601-203643-bkxsqffg",
            notebook_path="/Users/test/analysis",
            dbfs_config_path="dbfs:/FileStore/test/config.json",
            hierarchy_config_path="dbfs:/FileStore/test/hierarchy.yaml",
            topic_remap_path="dbfs:/FileStore/test/remap.yaml",
            analysis_mode="attention_pps",
            start_at="estimate",
        )
        tasks = build_payload(args)["tasks"]
        self.assertEqual(
            [task["task_key"] for task in tasks],
            ["analysis_estimate", "analysis_qa", "analysis_publish_treemap"],
        )
        self.assertNotIn("depends_on", tasks[0])
        self.assertEqual(
            tasks[1]["depends_on"], [{"task_key": "analysis_estimate"}]
        )

    def test_exact_topic_margins_use_companion_topic_table(self) -> None:
        notebook = (
            ROOT
            / "youtube_descriptive"
            / "src"
            / "14_full_corpus_dual_sample_analysis_databricks.py"
        ).read_text(encoding="utf-8")
        self.assertIn('"platform_topics": f"{PREFIX}_platform_topics"', notebook)
        self.assertIn('spark.table(TABLES["platform_topics"])', notebook)
        self.assertNotIn(
            'frame.select("channel_id", "raw_topic_categories")', notebook
        )
        self.assertIn('F.col("weighted_sum2") > 0', notebook)
        self.assertIn('F.col("view_language_total") > 0', notebook)
        self.assertIn('F.col("view_language_family_total") > 0', notebook)
        self.assertIn("primary_equal_channel_census_ge10k_share", notebook)
        self.assertIn("Equal-channel frame-stratum calibration failed", notebook)


if __name__ == "__main__":
    unittest.main()
