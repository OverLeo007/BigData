from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests


BASE_URL = "https://air.krasn.ru/api/2.0"
DEFAULT_OUTPUT_DIR = Path("data/raw/air")
DEFAULT_OUTPUT_BASENAME = "air_measurements"
DEFAULT_CHUNK_DAYS = 15
DEFAULT_PAGE_SIZE = 10000

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


def parse_datetime(value: str) -> datetime:
    normalized = value.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported datetime format: {value}")


def format_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def iter_time_chunks(start: str, end: str, chunk_days: int) -> list[tuple[str, str]]:
    if chunk_days < 1:
        raise ValueError("--chunk-days must be greater than 0")

    current = parse_datetime(start)
    finish = parse_datetime(end)
    if finish <= current:
        raise ValueError("--end must be greater than --start")

    chunks: list[tuple[str, str]] = []
    step = timedelta(days=chunk_days)
    while current < finish:
        chunk_end = min(current + step, finish)
        chunks.append((format_datetime(current), format_datetime(chunk_end)))
        current = chunk_end
    return chunks


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


def load_site_measurement_page(
    *,
    site_ids: list[int],
    start: str,
    end: str,
    time_interval: str,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], str]:
    params = {
        "time_interval": time_interval,
        "sites": ",".join(map(str, site_ids)),
        "time_begin": start,
        "time_end": end,
        "limit": limit,
        "offset": offset,
    }

    payload = get_json(f"{BASE_URL}/data", params=params)
    rows = extract_data(payload)

    request = requests.Request("GET", f"{BASE_URL}/data", params=params).prepare()
    return rows, request.url or ""


def row_key(item: dict[str, Any]) -> tuple[Any, Any]:
    return (
        item.get("site") or item.get("site_id"),
        item.get("time") or item.get("date") or item.get("datetime"),
    )


def load_site_measurements_chunked(
    *,
    site_ids: list[int],
    start: str,
    end: str,
    time_interval: str,
    chunk_days: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if page_size < 1 or page_size > DEFAULT_PAGE_SIZE:
        raise ValueError(f"--page-size must be in range 1..{DEFAULT_PAGE_SIZE}")

    rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[Any, Any]] = set()
    request_log: list[dict[str, Any]] = []

    for chunk_start, chunk_end in iter_time_chunks(start, end, chunk_days):
        offset = 0
        while True:
            page_rows, used_url = load_site_measurement_page(
                site_ids=site_ids,
                start=chunk_start,
                end=chunk_end,
                time_interval=time_interval,
                limit=page_size,
                offset=offset,
            )

            added_rows = 0
            for row in page_rows:
                key = row_key(row)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                rows.append(row)
                added_rows += 1

            request_log.append(
                {
                    "time_begin": chunk_start,
                    "time_end": chunk_end,
                    "limit": page_size,
                    "offset": offset,
                    "rows_count": len(page_rows),
                    "added_rows_count": added_rows,
                    "used_url": used_url,
                }
            )

            print(
                "Air chunk "
                f"{chunk_start} - {chunk_end}, offset {offset}: "
                f"received {len(page_rows)}, added {added_rows}"
            )

            if len(page_rows) < page_size:
                break

            offset += page_size

            if added_rows == 0:
                print(
                    "No new rows on the next page. "
                    "Stopping this chunk to avoid an endless pagination loop."
                )
                break

    return rows, request_log


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
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    page_size: int = DEFAULT_PAGE_SIZE,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    output_basename: str = DEFAULT_OUTPUT_BASENAME,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    rows, request_log = load_site_measurements_chunked(
        site_ids=site_ids,
        start=start,
        end=end,
        time_interval=time_interval,
        chunk_days=chunk_days,
        page_size=page_size,
    )

    payload = {
        "site_ids": site_ids,
        "time_begin": start,
        "time_end": end,
        "time_interval": time_interval,
        "chunk_days": chunk_days,
        "page_size": page_size,
        "rows_count": len(rows),
        "requests_count": len(request_log),
        "requests": request_log,
        "rows": rows,
    }

    json_path = output_dir / f"{output_basename}.json"
    csv_path = output_dir / f"{output_basename}.csv"

    save_json(payload, json_path)
    save_csv([normalize_row(row) for row in rows], csv_path)

    print(f"Downloaded rows: {len(rows)}")
    print(f"Requests: {len(request_log)}")

    return json_path, csv_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=os.getenv("AIR_START"))
    parser.add_argument("--end", default=os.getenv("AIR_END"))
    parser.add_argument("--sites", default=os.getenv("AIR_SITES"))
    parser.add_argument("--time-interval", default=os.getenv("AIR_TIME_INTERVAL", "hour"))
    parser.add_argument("--chunk-days", type=int, default=int(os.getenv("AIR_CHUNK_DAYS", DEFAULT_CHUNK_DAYS)))
    parser.add_argument("--page-size", type=int, default=int(os.getenv("AIR_PAGE_SIZE", DEFAULT_PAGE_SIZE)))
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
        chunk_days=args.chunk_days,
        page_size=args.page_size,
        output_dir=Path(args.output_dir),
        output_basename=args.output_basename,
    )

    print(f"Saved JSON: {json_path}")
    print(f"Saved CSV: {csv_path}")


if __name__ == "__main__":
    main()
