from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from scripts import render_full_corpus_weighted_treemaps as renderer


class FullCorpusWeightedTreemapRendererTests(unittest.TestCase):
    def test_language_catalog_keeps_undetermined_explicit(self) -> None:
        frame = pd.DataFrame({"language": ["eng", "spa", "und", "zza"]})
        named = renderer.add_language_names(frame)
        self.assertEqual(
            named[renderer.base.DISPLAY_COL].tolist(),
            ["English", "Spanish", "Undetermined", "Zaza"],
        )

    def test_renderer_rows_use_selected_calibrated_geometry(self) -> None:
        cells = pd.DataFrame(
            {
                "language": ["eng"],
                renderer.base.DISPLAY_COL: ["English"],
                "family": ["Music"],
                "leaf": ["Rock music"],
                "view_geometry_total": [125.0],
                "channel_geometry_total": [50.0],
            }
        )
        attention = renderer.renderer_rows(cells, "attention")
        channels = renderer.renderer_rows(cells, "channels")
        self.assertEqual(attention.loc[0, renderer.base.VALUE_COL], 125.0)
        self.assertEqual(channels.loc[0, renderer.base.VALUE_COL], 50.0)
        self.assertFalse(bool(attention.loc[0, "is_placement_override"]))

    def test_attention_only_publication_does_not_require_srs_columns(self) -> None:
        cells = pd.DataFrame(
            {
                "allocation_variant": ["platform_only"],
                "population_scope": ["all_retrievable"],
                "language": ["eng"],
                "family": ["Music"],
                "leaf": ["Rock music"],
                "view_geometry_total": [100.0],
                "view_raw_share": [1.0],
                "view_standard_error": [0.0],
                "view_ci95_lower": [1.0],
                "view_ci95_upper": [1.0],
                "view_effective_contributing_n": [1000.0],
                "view_largest_weighted_contribution": [0.01],
                "view_headline_reliable": [True],
                "view_geometry_calibration_basis": ["exact margin"],
            }
        )
        publication = pd.DataFrame({"taxonomy_level": ["language"]})
        renderer.validate_inputs(cells, publication, ("attention",))
        with self.assertRaises(RuntimeError):
            renderer.validate_inputs(cells, publication, ("attention", "channels"))

    def test_static_configuration_keeps_subtopics(self) -> None:
        with TemporaryDirectory() as directory:
            renderer.configure_static(
                Path(directory),
                "test",
                "attention",
                {
                    "population_scope": "all_retrievable",
                    "treemap": {
                        "static_top_languages": 12,
                        "static_cell_cap": 250,
                        "static_leaf_min_frac": 0.003,
                    },
                },
            )
            self.assertTrue(renderer.base.STATIC_INCLUDE_SUBTOPICS)
            self.assertEqual(renderer.base.STATIC_CELL_CAP, 250)
            self.assertEqual(renderer.base.LEAF_MIN, 0.003)

    def test_static_cell_cap_can_override_export_manifest(self) -> None:
        with TemporaryDirectory() as directory:
            renderer.configure_static(
                Path(directory),
                "test",
                "attention",
                {"treemap": {"static_cell_cap": 200}},
                static_cell_cap=250,
                leaf_min_frac=0.003,
            )
            self.assertEqual(renderer.base.STATIC_CELL_CAP, 250)
            self.assertEqual(renderer.base.LEAF_MIN, 0.003)


if __name__ == "__main__":
    unittest.main()
