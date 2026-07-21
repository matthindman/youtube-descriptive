from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from scripts.render_full_corpus_expansion_changes import (
    build_changes,
    render_change_figure,
    write_markdown_summary,
)


class FullCorpusExpansionChangeTests(unittest.TestCase):
    def test_census_to_platform_changes_conserve_and_keep_intersections(self) -> None:
        cells = pd.DataFrame(
            [
                ("platform_only", "known_subscriber", "eng", "A", "a", 60.0),
                ("platform_only", "known_subscriber", "eng", "B", "b", 10.0),
                ("platform_only", "known_subscriber", "spa", "A", "a", 20.0),
                ("platform_only", "known_subscriber", "spa", "B", "b", 10.0),
            ],
            columns=[
                "allocation_variant",
                "population_scope",
                "language",
                "family",
                "leaf",
                "view_geometry_total",
            ],
        )
        base = {
            "allocation_variant": "platform_only",
            "population_scope": "known_subscriber",
            "view_standard_error": 0.001,
            "view_headline_reliable": True,
            "view_effective_contributing_n": 100.0,
        }
        rows = [
            {
                **base,
                "taxonomy_level": "language",
                "language": "eng",
                "family": "",
                "leaf": "",
                "view_head_total": 85.0,
                "view_raw_share": 0.70,
            },
            {
                **base,
                "taxonomy_level": "language",
                "language": "spa",
                "family": "",
                "leaf": "",
                "view_head_total": 15.0,
                "view_raw_share": 0.30,
            },
            {
                **base,
                "taxonomy_level": "family",
                "language": "",
                "family": "A",
                "leaf": "",
                "view_head_total": 90.0,
                "view_raw_share": 0.80,
            },
            {
                **base,
                "taxonomy_level": "family",
                "language": "",
                "family": "B",
                "leaf": "",
                "view_head_total": 10.0,
                "view_raw_share": 0.20,
            },
            {
                **base,
                "taxonomy_level": "leaf",
                "language": "",
                "family": "A",
                "leaf": "a",
                "view_head_total": 90.0,
                "view_raw_share": 0.80,
            },
            {
                **base,
                "taxonomy_level": "leaf",
                "language": "",
                "family": "B",
                "leaf": "b",
                "view_head_total": 10.0,
                "view_raw_share": 0.20,
            },
        ]
        for language, family, leaf, head, share in (
            ("eng", "A", "", 80.0, 0.60),
            ("eng", "B", "", 5.0, 0.10),
            ("spa", "A", "", 10.0, 0.20),
            ("spa", "B", "", 5.0, 0.10),
        ):
            rows.append(
                {
                    **base,
                    "taxonomy_level": "language_family",
                    "language": language,
                    "family": family,
                    "leaf": leaf,
                    "view_head_total": head,
                    "view_raw_share": share,
                }
            )
        for language, family, leaf, head, share in (
            ("eng", "A", "a", 80.0, 0.60),
            ("eng", "B", "b", 5.0, 0.10),
            ("spa", "A", "a", 10.0, 0.20),
            ("spa", "B", "b", 5.0, 0.10),
        ):
            rows.append(
                {
                    **base,
                    "taxonomy_level": "language_family_leaf",
                    "language": language,
                    "family": family,
                    "leaf": leaf,
                    "view_head_total": head,
                    "view_raw_share": share,
                }
            )
        changes = build_changes(cells, pd.DataFrame(rows))

        for level, group in changes.groupby("taxonomy_level", observed=True):
            self.assertAlmostEqual(group["census_share"].sum(), 1.0, places=12, msg=level)
            self.assertAlmostEqual(group["platform_share"].sum(), 1.0, places=12, msg=level)
            self.assertAlmostEqual(group["absolute_change"].sum(), 0.0, places=12, msg=level)
        spanish = changes.loc[(changes["taxonomy_level"] == "language") & (changes["language"] == "spa")].iloc[0]
        self.assertAlmostEqual(spanish["absolute_change"], 0.15)
        self.assertAlmostEqual(spanish["proportional_change"], 1.0)
        self.assertTrue(spanish["proportional_ranking_eligible"])
        family = changes.loc[changes["taxonomy_level"] == "family"]
        self.assertTrue((family["change_standard_error"] == 0).all())
        self.assertTrue(
            family["sampling_inference_basis"].str.startswith("exact frozen-frame").all()
        )
        intersections = changes.loc[
            changes["taxonomy_level"] == "language_family"
        ]
        self.assertTrue((intersections["change_standard_error"] == 0.001).all())
        self.assertEqual(
            len(changes.loc[changes["taxonomy_level"] == "language_family_leaf"]),
            4,
        )
        with TemporaryDirectory() as directory:
            artifacts = render_change_figure(
                changes,
                ["language", "family", "leaf"],
                "absolute_change",
                Path(directory) / "marginal_absolute",
            )
            for path in artifacts.values():
                self.assertGreater(Path(path).stat().st_size, 1_000)
            summary_path = Path(directory) / "summary.md"
            write_markdown_summary(changes, summary_path)
            summary = summary_path.read_text(encoding="utf-8")
            family_section = summary.split("## Topic families", 1)[1].split(
                "## Subtopics", 1
            )[0]
            growth = family_section.split(
                "### Largest percentage-point growth", 1
            )[1].split("### Largest percentage-point decline", 1)[0]
            decline = family_section.split(
                "### Largest percentage-point decline", 1
            )[1].split("### Largest proportional growth", 1)[0]
            self.assertIn("| B |", growth)
            self.assertNotIn("| A |", growth)
            self.assertIn("| A |", decline)
            self.assertNotIn("| B |", decline)


if __name__ == "__main__":
    unittest.main()
