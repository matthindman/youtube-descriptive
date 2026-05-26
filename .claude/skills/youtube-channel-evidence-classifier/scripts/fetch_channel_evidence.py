#!/usr/bin/env python3
"""Fetch public YouTube channel evidence snippets for validation work.

The script is read-only. It prints one JSON object per channel ID.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path


USER_AGENT = "Mozilla/5.0 (compatible; channel-evidence-validator/1.0)"


def fetch(url: str, timeout: float) -> tuple[str | None, str | None]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            return resp.read().decode(charset, errors="replace"), None
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return None, str(exc)


def unique(items: list[str], limit: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        item = html.unescape(re.sub(r"\s+", " ", item)).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
        if len(out) >= limit:
            break
    return out


def extract_page(html_text: str | None) -> dict[str, object]:
    if not html_text:
        return {"titles": [], "descriptions": [], "unavailable": False}
    titles = re.findall(r'"title"\s*:\s*"([^"]{1,180})"', html_text)
    titles += re.findall(r"<title>(.*?)</title>", html_text, flags=re.I | re.S)
    descriptions = re.findall(r'"description"\s*:\s*"([^"]{1,300})"', html_text)
    descriptions += re.findall(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
        html_text,
        flags=re.I,
    )
    unavailable = bool(
        re.search(r"channel does not exist|not available|404", html_text, flags=re.I)
    )
    return {
        "titles": unique(titles, 8),
        "descriptions": unique(descriptions, 5),
        "unavailable": unavailable,
    }


def extract_rss(xml_text: str | None) -> dict[str, object]:
    if not xml_text:
        return {"feed_title": None, "video_titles": []}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return {"feed_title": None, "video_titles": []}
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    feed_title_el = root.find("atom:title", ns)
    video_titles = [
        el.text or "" for el in root.findall("atom:entry/atom:title", ns)
    ]
    return {
        "feed_title": feed_title_el.text if feed_title_el is not None else None,
        "video_titles": unique(video_titles, 10),
    }


def ids_from_csv(path: Path, id_column: str, rows: str | None) -> list[str]:
    wanted: set[int] | None = None
    if rows:
        wanted = set()
        for part in rows.split(","):
            if "-" in part:
                start, end = [int(x) for x in part.split("-", 1)]
                wanted.update(range(start, end + 1))
            else:
                wanted.add(int(part))
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        out = []
        for idx, row in enumerate(reader, start=1):
            if wanted is not None and idx not in wanted:
                continue
            channel_id = (row.get(id_column) or "").strip()
            if channel_id:
                out.append(channel_id)
        return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("channel_ids", nargs="*", help="YouTube UC... channel IDs")
    parser.add_argument("--csv", type=Path, help="CSV containing channel IDs")
    parser.add_argument("--id-column", default="channel_id")
    parser.add_argument("--rows", help="1-based CSV data rows, e.g. 11-20,24")
    parser.add_argument("--timeout", type=float, default=12.0)
    args = parser.parse_args()

    channel_ids = list(args.channel_ids)
    if args.csv:
        channel_ids.extend(ids_from_csv(args.csv, args.id_column, args.rows))
    if not channel_ids:
        parser.error("provide channel IDs or --csv")

    for channel_id in channel_ids:
        page_url = f"https://www.youtube.com/channel/{channel_id}"
        rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        page_text, page_error = fetch(page_url, args.timeout)
        rss_text, rss_error = fetch(rss_url, args.timeout)
        record = {
            "channel_id": channel_id,
            "page_url": page_url,
            "rss_url": rss_url,
            "page_error": page_error,
            "rss_error": rss_error,
            "page": extract_page(page_text),
            "rss": extract_rss(rss_text),
        }
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
