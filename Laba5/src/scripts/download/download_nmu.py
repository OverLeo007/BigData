from __future__ import annotations

import argparse
import csv
import html
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests


NMU_URL = "https://www.admkrsk.ru/citytoday/ecology/Pages/NMU.aspx"
DEFAULT_OUTPUT_DIR = Path("data/raw/nmu")
DEFAULT_OUTPUT_BASENAME = "nmu_announcements"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
}

CSV_COLUMNS = [
    "published",
    "regime",
    "regime_level",
    "period_start",
    "period_end",
    "announcement_text",
    "source",
]

MONTHS = {
    "января": 1, "январь": 1,
    "февраля": 2, "февраль": 2,
    "марта": 3, "март": 3,
    "апреля": 4, "апрель": 4,
    "мая": 5, "май": 5,
    "июня": 6, "июнь": 6,
    "июля": 7, "июль": 7,
    "августа": 8, "август": 8,
    "сентября": 9, "сентябрь": 9,
    "октября": 10, "октябрь": 10,
    "ноября": 11, "ноябрь": 11,
    "декабря": 12, "декабрь": 12,
}

# "19.00 27 мая 2026" / "19 часов 30 апреля 2026" / "00 часов 02 марта 2026"
TIME_DAY = r"(\d{1,2})(?:[.:](\d{2}))?\s*(?:час\w*\s+)?(\d{1,2})\s+([а-яё]+)\s+(\d{4})"
PERIOD_RE = re.compile(r"с\s+" + TIME_DAY + r".*?\bдо\s+" + TIME_DAY, re.IGNORECASE)
REGIME_RE = re.compile(r"(перв|втор|трет)\w*\s+режим", re.IGNORECASE)
PUBLISHED_RE = re.compile(r"(?<!\d)(\d{2})\.(\d{2})\.(\d{4})\b")

REGIME_LEVELS = {"перв": 1, "втор": 2, "трет": 3}
REGIME_LABELS = {0: "Общий прогноз", 1: "Первый режим", 2: "Второй режим", 3: "Третий режим"}


# Real April-2026 (and adjacent) announcements scraped from admkrsk.ru. Used only when the live
# page cannot be parsed (the manual-CSV fallback explicitly allowed by the project white paper).
FALLBACK_ANNOUNCEMENTS: list[dict[str, Any]] = [
    {"published": "31.03.2026", "regime": "Общий прогноз", "regime_level": 0,
     "period_start": "2026-03-31 19:00:00", "period_end": "2026-04-01 19:00:00",
     "announcement_text": "31.03.2026 Общий прогноз: НМУ ожидаются с 19 часов 31 марта 2026 года до 19 часов 01 апреля 2026 года."},
    {"published": "07.04.2026", "regime": "Общий прогноз", "regime_level": 0,
     "period_start": "2026-04-07 19:00:00", "period_end": "2026-04-08 13:00:00",
     "announcement_text": "07.04.2026 Общий прогноз: НМУ ожидаются с 19 часов 07 апреля 2026 года до 13 часов 08 апреля 2026 года."},
    {"published": "22.04.2026", "regime": "Общий прогноз", "regime_level": 0,
     "period_start": "2026-04-22 19:00:00", "period_end": "2026-04-23 07:00:00",
     "announcement_text": "22.04.2026 Общий прогноз: НМУ ожидаются с 19 часов 22 апреля 2026 года до 07 часов 23 апреля 2026 года."},
    {"published": "25.04.2026", "regime": "Общий прогноз", "regime_level": 0,
     "period_start": "2026-04-25 19:00:00", "period_end": "2026-04-26 10:00:00",
     "announcement_text": "25.04.2026 Общий прогноз: НМУ ожидаются с 19 часов 25 апреля 2026 года до 10 часов 26 апреля 2026 года."},
    {"published": "26.04.2026", "regime": "Общий прогноз", "regime_level": 0,
     "period_start": "2026-04-26 19:00:00", "period_end": "2026-04-27 07:00:00",
     "announcement_text": "26.04.2026 Общий прогноз: НМУ ожидаются с 19 часов 26 апреля 2026 года до 07 часов 27 апреля 2026 года."},
    {"published": "30.04.2026", "regime": "Общий прогноз", "regime_level": 0,
     "period_start": "2026-04-30 19:00:00", "period_end": "2026-05-01 19:00:00",
     "announcement_text": "30.04.2026 Общий прогноз: НМУ ожидаются с 19 часов 30 апреля 2026 года до 19 часов 01 мая 2026 года."},
]


def fetch_html() -> str:
    response = requests.get(NMU_URL, headers=HEADERS, timeout=90)
    response.raise_for_status()
    return response.text


def html_to_text(raw_html: str) -> str:
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", raw_html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[\s​\xa0]+", " ", text)
    return text.strip()


def build_datetime(hour: str, minute: str | None, day: str, month_name: str, year: str) -> datetime | None:
    month = MONTHS.get(month_name.lower())
    if month is None:
        return None

    hour_value = int(hour)
    minute_value = int(minute) if minute else 0
    base = datetime(int(year), month, int(day))

    # "24 часов" means end of the day -> next day 00:00.
    if hour_value >= 24:
        return base + timedelta(days=1)
    return base.replace(hour=hour_value, minute=minute_value)


def parse_announcements(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    chunks = re.split(r"(?=(?<!\d)\d{2}\.\d{2}\.\d{4}\b)", text)

    for chunk in chunks:
        chunk = chunk.strip()
        published_match = PUBLISHED_RE.match(chunk)
        if not published_match:
            continue
        if "НМУ" not in chunk and "режим" not in chunk.lower():
            continue

        period_match = PERIOD_RE.search(chunk)
        if not period_match:
            continue

        groups = period_match.groups()
        start = build_datetime(groups[0], groups[1], groups[2], groups[3], groups[4])
        end = build_datetime(groups[5], groups[6], groups[7], groups[8], groups[9])
        if start is None or end is None or end <= start:
            continue

        regime_match = REGIME_RE.search(chunk)
        regime_level = REGIME_LEVELS.get(regime_match.group(1).lower(), 0) if regime_match else 0

        announcement_text = chunk[: period_match.end()].strip()
        announcement_text = re.sub(r"\s+", " ", announcement_text)[:400]

        rows.append(
            {
                "published": f"{published_match.group(1)}.{published_match.group(2)}.{published_match.group(3)}",
                "regime": REGIME_LABELS[regime_level],
                "regime_level": regime_level,
                "period_start": start.strftime("%Y-%m-%d %H:%M:%S"),
                "period_end": end.strftime("%Y-%m-%d %H:%M:%S"),
                "announcement_text": announcement_text,
                "source": "parsed",
            }
        )

    # Deduplicate identical periods (the page repeats announcements in several blocks).
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        unique[(row["period_start"], row["period_end"])] = row
    return sorted(unique.values(), key=lambda r: r["period_start"])


def save_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def download_nmu(*, output_dir: Path, output_basename: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"{output_basename}.csv"
    html_path = output_dir / "nmu_raw.html"

    try:
        raw_html = fetch_html()
        html_path.write_text(raw_html, encoding="utf-8")
        rows = parse_announcements(html_to_text(raw_html))
        print(f"Parsed NMU announcements: {len(rows)}")
    except Exception as error:  # noqa: BLE001 - network/markup issues fall back to the seed CSV
        print(f"NMU download/parse failed ({error!r}); using fallback announcements.")
        rows = []

    if not rows:
        rows = [dict(item) for item in FALLBACK_ANNOUNCEMENTS]
        for row in rows:
            row["source"] = "fallback"
        print(f"Using fallback NMU announcements: {len(rows)}")

    save_csv(rows, csv_path)
    print(f"Saved CSV: {csv_path}")
    return csv_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Download and parse Krasnoyarsk NMU announcements.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--output-basename", default=DEFAULT_OUTPUT_BASENAME)
    args = parser.parse_args()

    download_nmu(output_dir=Path(args.output_dir), output_basename=args.output_basename)


if __name__ == "__main__":
    main()
