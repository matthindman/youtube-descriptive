from __future__ import annotations

import json
import math
import unittest
from pathlib import Path

from youtube_descriptive.src.full_corpus_dual_sample_design import (
    allocate_stratified_counts,
    expected_kish_effective_n,
    poisson_ht_total_variance,
    sha256_order_key,
    sha256_uniform,
    solve_capped_probabilities,
    srs_ht_total_variance,
    validate_design_config,
)


ROOT = Path(__file__).resolve().parents[2]


class FullCorpusDualSampleDesignTests(unittest.TestCase):
    def test_frozen_config_is_valid(self) -> None:
        path = ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json"
        config = json.loads(path.read_text(encoding="utf-8"))
        validate_design_config(config)
        self.assertEqual(config["samples"]["selected_alpha"], 0.1)
        self.assertEqual(config["execution"]["existing_cluster_id"], "0601-203643-bkxsqffg")
        self.assertEqual(config["treemap"]["packing"], "squarify")
        self.assertEqual(config["treemap"]["static_cell_cap"], 200)

    def test_config_rejects_wrong_databricks_profile(self) -> None:
        path = ROOT / "config" / "full_corpus_dual_sample_20260717_v1.json"
        config = json.loads(path.read_text(encoding="utf-8"))
        config["execution"]["databricks_profile"] = "default"
        with self.assertRaisesRegex(ValueError, "execution settings changed"):
            validate_design_config(config)

    def test_sha256_mapping_is_deterministic_and_seeded(self) -> None:
        args = ("UC_x", "frame_v1", "seed_a")
        self.assertEqual(sha256_order_key(*args), sha256_order_key(*args))
        self.assertEqual(sha256_uniform(*args), sha256_uniform(*args))
        self.assertNotEqual(sha256_order_key(*args), sha256_order_key("UC_x", "frame_v1", "seed_b"))
        self.assertGreater(sha256_uniform(*args), 0.0)
        self.assertLess(sha256_uniform(*args), 1.0)

    def test_capped_probability_solver_conserves_expected_n(self) -> None:
        q_values = [0.7, 0.1, 0.1, 0.1]
        _, probabilities = solve_capped_probabilities(q_values, 2.0)
        self.assertTrue(math.isclose(sum(probabilities), 2.0, rel_tol=1e-12))
        self.assertEqual(probabilities[0], 1.0)
        self.assertTrue(all(0.0 < value <= 1.0 for value in probabilities))

    def test_stratified_allocation_is_exact_and_respects_capacity(self) -> None:
        strata = [
            {"stratum": "zero", "population_n": 80, "size_total": 0.0},
            {"stratum": "small", "population_n": 15, "size_total": 20.0},
            {"stratum": "large", "population_n": 5, "size_total": 980.0},
        ]
        allocation = allocate_stratified_counts(strata, target_n=20, alpha=0.1)
        self.assertEqual(sum(allocation.values()), 20)
        self.assertLessEqual(allocation["large"], 5)
        self.assertGreater(allocation["zero"], 0)

    def test_expected_kish_n_is_exact_for_equal_probabilities(self) -> None:
        probabilities = [0.1] * 100
        self.assertTrue(math.isclose(expected_kish_effective_n(probabilities, 100), 10.0))

    def test_srs_total_and_variance_include_fpc(self) -> None:
        total, variance = srs_ht_total_variance([0.0, 1.0, 1.0, 0.0], population_n=8)
        self.assertEqual(total, 4.0)
        self.assertTrue(math.isclose(variance, 8.0 / 3.0))

    def test_poisson_total_and_variance(self) -> None:
        total, variance = poisson_ht_total_variance([2.0, 3.0], [0.5, 1.0])
        self.assertEqual(total, 7.0)
        self.assertEqual(variance, 8.0)


if __name__ == "__main__":
    unittest.main()
