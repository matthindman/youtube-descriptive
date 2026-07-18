#!/usr/bin/env python3
"""Build the treemap's deterministic ISO 639 display-name lookup.

The generated CSV contains ISO 639-3 languages, legacy bibliographic aliases,
and ISO 639-5 language-family collection codes exposed by ``pycountry``. The
small, editorial language merges in ``config/language_normalization.yaml``
remain authoritative at render time (for example, cmn/yue -> Chinese).
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import pycountry


def build_rows() -> list[dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}

    for language in pycountry.languages:
        alpha_3 = getattr(language, "alpha_3", "")
        name = getattr(language, "name", "")
        if alpha_3 and name:
            rows[alpha_3] = {
                "language_code": alpha_3,
                "canonical_iso639_3": alpha_3,
                "display_name": name,
                "mapping_source": "iso639_3",
            }
        bibliographic = getattr(language, "bibliographic", "")
        if bibliographic and name:
            rows.setdefault(
                bibliographic,
                {
                    "language_code": bibliographic,
                    "canonical_iso639_3": alpha_3,
                    "display_name": name,
                    "mapping_source": "iso639_2_bibliographic_alias",
                },
            )

    for family in pycountry.language_families:
        alpha_3 = getattr(family, "alpha_3", "")
        name = getattr(family, "name", "")
        if alpha_3 and name:
            rows.setdefault(
                alpha_3,
                {
                    "language_code": alpha_3,
                    "canonical_iso639_3": alpha_3,
                    "display_name": name,
                    "mapping_source": "iso639_5_collection",
                },
            )

    return [rows[code] for code in sorted(rows)]


def observed_codes(path: Path) -> dict[str, int]:
    payload = json.loads(path.read_text())
    data = payload.get("result", {}).get("data_array", [])
    return {str(code): int(count) for code, count in data}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("config/iso639_language_names.csv"),
    )
    parser.add_argument(
        "--observed-json",
        type=Path,
        help="Optional SQL statement JSON used to audit observed-code coverage.",
    )
    args = parser.parse_args()

    rows = build_rows()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(rows[0]),
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"WROTE: {args.output.resolve()}")
    print(f"MAPPING ROWS: {len(rows):,}")
    if args.observed_json:
        observed = observed_codes(args.observed_json)
        mapped = {row["language_code"] for row in rows}
        classified = {code: count for code, count in observed.items() if code != "und"}
        residual = {code: count for code, count in classified.items() if code not in mapped}
        print(f"OBSERVED CLASSIFIED CODES: {len(classified):,}")
        print(f"CATALOG-MAPPED OBSERVED CODES: {len(classified) - len(residual):,}")
        print(f"UNREGISTERED OBSERVED CODES: {len(residual):,}")
        print(f"UNREGISTERED OBSERVED CHANNELS: {sum(residual.values()):,}")
        if residual:
            print("UNREGISTERED DETAIL: " + ", ".join(
                f"{code}={count}" for code, count in sorted(residual.items())
            ))


if __name__ == "__main__":
    main()
