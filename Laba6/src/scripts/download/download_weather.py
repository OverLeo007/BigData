from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests


BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
DEFAULT_OUTPUT_DIR = Path("data/raw/weather")
DEFAULT_OUTPUT_BASENAME = "weather_hourly"
DEFAULT_LATITUDE = 56.0153
DEFAULT_LONGITUDE = 92.8932
DEFAULT_TIMEZONE = "Asia/Krasnoyarsk"
DEFAULT_CHUNK_DAYS = 15

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
]

CSV_COLUMNS = [
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
]


def to_open_meteo_date(value: str) -> str:
    normalized = value.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {value}")


def parse_datetime(value: str) -> datetime:
    normalized = value.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported datetime format: {value}")


def iter_date_chunks(start: str, end: str, chunk_days: int) -> list[tuple[date, date]]:
    if chunk_days < 1:
        raise ValueError("--chunk-days must be greater than 0")

    start_dt = parse_datetime(start)
    end_dt = parse_datetime(end)
    if end_dt <= start_dt:
        raise ValueError("--end must be greater than --start")

    current = start_dt.date()
    last_date = (end_dt - timedelta(seconds=1)).date()
    chunks: list[tuple[date, date]] = []

    while current <= last_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), last_date)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)

    return chunks


def get_json(params: dict[str, Any]) -> dict[str, Any]:
    response = requests.get(BASE_URL, params=params, timeout=90)

    if response.status_code != 200:
        print("Request failed")
        print("URL:", response.url)
        print("Status:", response.status_code)
        print("Body:", response.text[:1000])

    response.raise_for_status()
    return response.json()


def rows_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        return []

    times = hourly.get("time")
    if not isinstance(times, list):
        return []

    rows: list[dict[str, Any]] = []
    for index, time_value in enumerate(times):
        row = {"time": time_value}
        for column in CSV_COLUMNS:
            if column == "time":
                continue
            values = hourly.get(column)
            row[column] = values[index] if isinstance(values, list) and index < len(values) else None
        rows.append(row)

    return rows


def save_json(payload: dict[str, Any], path: Path) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def save_csv(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        raise ValueError("No weather rows to save. Check date range and coordinates.")

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def download_weather(
    *,
    start: str,
    end: str,
    latitude: float,
    longitude: float,
    timezone: str,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    output_basename: str = DEFAULT_OUTPUT_BASENAME,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    seen_times: set[Any] = set()
    request_log: list[dict[str, Any]] = []

    for chunk_start, chunk_end in iter_date_chunks(start, end, chunk_days):
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": chunk_start.isoformat(),
            "end_date": chunk_end.isoformat(),
            "hourly": ",".join(HOURLY_VARIABLES),
            "timezone": timezone,
        }

        payload = get_json(params)
        chunk_rows = rows_from_payload(payload)
        added_rows = 0

        for row in chunk_rows:
            time_value = row.get("time")
            if time_value in seen_times:
                continue
            seen_times.add(time_value)
            rows.append(row)
            added_rows += 1

        used_url = requests.Request("GET", BASE_URL, params=params).prepare().url
        request_log.append(
            {
                "start_date": chunk_start.isoformat(),
                "end_date": chunk_end.isoformat(),
                "rows_count": len(chunk_rows),
                "added_rows_count": added_rows,
                "used_url": used_url,
            }
        )

        print(
            "Weather chunk "
            f"{chunk_start.isoformat()} - {chunk_end.isoformat()}: "
            f"received {len(chunk_rows)}, added {added_rows}"
        )

    payload = {
        "latitude": latitude,
        "longitude": longitude,
        "time_begin": start,
        "time_end": end,
        "timezone": timezone,
        "chunk_days": chunk_days,
        "rows_count": len(rows),
        "requests_count": len(request_log),
        "requests": request_log,
        "rows": rows,
    }

    json_path = output_dir / f"{output_basename}.json"
    csv_path = output_dir / f"{output_basename}.csv"

    save_json(payload, json_path)
    save_csv(rows, csv_path)

    print(f"Downloaded weather rows: {len(rows)}")
    print(f"Requests: {len(request_log)}")

    return json_path, csv_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=os.getenv("WEATHER_START") or os.getenv("AIR_START"))
    parser.add_argument("--end", default=os.getenv("WEATHER_END") or os.getenv("AIR_END"))
    parser.add_argument("--latitude", type=float, default=float(os.getenv("WEATHER_LATITUDE", DEFAULT_LATITUDE)))
    parser.add_argument("--longitude", type=float, default=float(os.getenv("WEATHER_LONGITUDE", DEFAULT_LONGITUDE)))
    parser.add_argument("--timezone", default=os.getenv("WEATHER_TIMEZONE", DEFAULT_TIMEZONE))
    parser.add_argument("--chunk-days", type=int, default=int(os.getenv("WEATHER_CHUNK_DAYS", DEFAULT_CHUNK_DAYS)))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--output-basename", default=DEFAULT_OUTPUT_BASENAME)
    args = parser.parse_args()

    if not args.start or not args.end:
        parser.error("--start and --end are required")

    json_path, csv_path = download_weather(
        start=args.start,
        end=args.end,
        latitude=args.latitude,
        longitude=args.longitude,
        timezone=args.timezone,
        chunk_days=args.chunk_days,
        output_dir=Path(args.output_dir),
        output_basename=args.output_basename,
    )

    print(f"Saved JSON: {json_path}")
    print(f"Saved CSV: {csv_path}")


if __name__ == "__main__":
    main()
