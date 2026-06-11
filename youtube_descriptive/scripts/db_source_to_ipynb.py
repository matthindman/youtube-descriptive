#!/usr/bin/env python3
"""Convert a Databricks source-format notebook (.py) into a Jupyter .ipynb.

Zero third-party dependencies (stdlib json only). Parses the Databricks cell
conventions used across this repo:

  * ``# Databricks notebook source``      -> file header (ignored)
  * ``# COMMAND ----------``               -> cell boundary
  * ``# MAGIC %md`` / ``# MAGIC <text>``   -> markdown cell
  * ``# MAGIC %sql`` / ``%sh`` / ``%fs``   -> code cell carrying the magic
  * anything else                          -> Python code cell

The resulting .ipynb opens natively in Jupyter/VS Code and imports directly
into Databricks (one notebook cell per .ipynb cell).

Usage:
    python scripts/db_source_to_ipynb.py SRC.py DEST.ipynb
"""
from __future__ import annotations

import json
import sys
from typing import List


def split_cells(lines: List[str]) -> List[List[str]]:
    cells: List[List[str]] = [[]]
    for line in lines:
        if line.strip() == "# COMMAND ----------":
            cells.append([])
        else:
            cells[-1].append(line)
    # drop the leading "# Databricks notebook source" header line from the first cell
    if cells and cells[0]:
        cells[0] = [ln for ln in cells[0] if ln.strip() != "# Databricks notebook source"]
    return [c for c in cells if any(ln.strip() for ln in c)]


def to_nb_cell(raw: List[str]) -> dict:
    stripped = [ln for ln in raw if ln.strip()]
    is_magic = stripped and stripped[0].lstrip().startswith("# MAGIC")
    if is_magic:
        body: List[str] = []
        for ln in raw:
            s = ln.lstrip()
            if s.startswith("# MAGIC"):
                body.append(s[len("# MAGIC"):].lstrip("\n").removeprefix(" "))
            elif not ln.strip():
                body.append("")
        first = body[0].strip() if body else ""
        if first.startswith("%md"):
            md = body[1:] if body and body[0].strip() == "%md" else [body[0].replace("%md", "", 1)] + body[1:]
            text = "\n".join(md).strip("\n")
            return {"cell_type": "markdown", "metadata": {}, "source": _src(text)}
        # non-md magic (%sql/%sh/...) -> keep as a code cell verbatim
        return {"cell_type": "code", "metadata": {}, "execution_count": None,
                "outputs": [], "source": _src("\n".join(body).strip("\n"))}
    text = "\n".join(raw).strip("\n")
    return {"cell_type": "code", "metadata": {}, "execution_count": None,
            "outputs": [], "source": _src(text)}


def _src(text: str) -> List[str]:
    """nbformat stores source as a list of lines, each (except the last) ending in \\n."""
    parts = text.split("\n")
    return [p + "\n" for p in parts[:-1]] + [parts[-1]] if parts else []


def convert(src_path: str, dest_path: str) -> None:
    with open(src_path, "r", encoding="utf-8") as fh:
        lines = fh.read().split("\n")
    cells = [to_nb_cell(c) for c in split_cells(lines)]
    # nbformat 4.5+ requires a unique, stable cell id per cell.
    for i, cell in enumerate(cells):
        cell["id"] = f"cell{i:03d}"
    nb = {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
            "authorship": "Claude (Anthropic Opus 4.8)",
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    with open(dest_path, "w", encoding="utf-8") as fh:
        json.dump(nb, fh, indent=1)
        fh.write("\n")
    print(f"Wrote {dest_path} with {len(cells)} cells "
          f"({sum(c['cell_type']=='markdown' for c in cells)} markdown, "
          f"{sum(c['cell_type']=='code' for c in cells)} code).")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    convert(sys.argv[1], sys.argv[2])
