from __future__ import annotations

import unittest

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


if __name__ == "__main__":
    unittest.main()
