from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any

import requests


BASE_URL = "https://air.krasn.ru/api/2.0"
DEFAULT_OUTPUT_DIR = Path("data/raw/air")
DEFAULT_OUTPUT_BASENAME = "air_measurements"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
}

CSV_COLUMNS = [
    "site_id",
    "time",
    "aqi",
    "iaqi",
    "pm25",
    "pm10",
    "pm25_mcp",
    "temperature",
    "humidity",
    "pressure",
    "_raw",
]


def parse_site_ids(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def get_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    response = requests.get(url, params=params, headers=HEADERS, timeout=90)

    if response.status_code != 200:
        print("Request failed")
        print("URL:", response.url)
        print("Status:", response.status_code)
        print("Body:", response.text[:1000])

    response.raise_for_status()
    return response.json()


def extract_data(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", [])

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("items", "rows", "measurements", "values"):
            value = data.get(key)
            if isinstance(value, list):
                return value

    return []


def load_site_measurements(
    site_ids: list[int],
    start: str,
    end: str,
    time_interval: str,
) -> tuple[list[dict[str, Any]], str]:
    params = {
        "time_interval": time_interval,
        "sites": ",".join(map(str, site_ids)),
        "time_begin": start,
        "time_end": end,
    }

    payload = get_json(f"{BASE_URL}/data", params=params)
    rows = extract_data(payload)

    request = requests.Request("GET", f"{BASE_URL}/data", params=params).prepare()
    return rows, request.url or ""


def normalize_row(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "site_id": item.get("site") or item.get("site_id"),
        "time": item.get("time") or item.get("date") or item.get("datetime"),
        "aqi": item.get("aqi"),
        "iaqi": item.get("iaqi"),
        "pm25": item.get("pm25"),
        "pm10": item.get("pm10"),
        "pm25_mcp": item.get("pm25_mcp"),
        "temperature": item.get("t") or item.get("temperature"),
        "humidity": item.get("h") or item.get("humidity"),
        "pressure": item.get("p") or item.get("pressure"),
        "_raw": json.dumps(item, ensure_ascii=False),
    }


def save_json(data: Any, path: Path) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def save_csv(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        raise ValueError("No rows to save. Check date range and site ids.")

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def download_air_measurements(
    *,
    start: str,
    end: str,
    site_ids: list[int],
    time_interval: str,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    output_basename: str = DEFAULT_OUTPUT_BASENAME,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    rows, used_url = load_site_measurements(
        site_ids=site_ids,
        start=start,
        end=end,
        time_interval=time_interval,
    )

    payload = {
        "site_ids": site_ids,
        "time_begin": start,
        "time_end": end,
        "time_interval": time_interval,
        "rows_count": len(rows),
        "used_url": used_url,
        "rows": rows,
    }

    json_path = output_dir / f"{output_basename}.json"
    csv_path = output_dir / f"{output_basename}.csv"

    save_json(payload, json_path)
    save_csv([normalize_row(row) for row in rows], csv_path)

    print(f"Downloaded rows: {len(rows)}")
    print(f"Request URL: {used_url}")

    return json_path, csv_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=os.getenv("AIR_START"))
    parser.add_argument("--end", default=os.getenv("AIR_END"))
    parser.add_argument("--sites", default=os.getenv("AIR_SITES"))
    parser.add_argument("--time-interval", default=os.getenv("AIR_TIME_INTERVAL", "hour"))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--output-basename", default=DEFAULT_OUTPUT_BASENAME)
    args = parser.parse_args()

    if not args.start or not args.end or not args.sites:
        parser.error("--start, --end and --sites are required")

    json_path, csv_path = download_air_measurements(
        start=args.start,
        end=args.end,
        site_ids=parse_site_ids(args.sites),
        time_interval=args.time_interval,
        output_dir=Path(args.output_dir),
        output_basename=args.output_basename,
    )

    print(f"Saved JSON: {json_path}")
    print(f"Saved CSV: {csv_path}")


if __name__ == "__main__":
    main()
