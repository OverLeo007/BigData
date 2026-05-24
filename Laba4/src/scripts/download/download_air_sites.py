from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any

import requests

from download_air import BASE_URL, HEADERS, parse_site_ids


DEFAULT_OUTPUT_DIR = Path("data/reference")
DEFAULT_OUTPUT_BASENAME = "air_sites"

CSV_COLUMNS = [
    "site_id",
    "site_name",
    "project_id",
    "project_name",
    "project_short_name",
    "latitude",
    "longitude",
    "raw_json",
]


def get_json(url: str) -> dict[str, Any]:
    response = requests.get(url, headers=HEADERS, timeout=60)
    response.raise_for_status()
    return response.json()


def extract_projects(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", [])
    return data if isinstance(data, list) else []


def extract_sites(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", {})
    if isinstance(data, dict) and isinstance(data.get("sites"), list):
        return data["sites"]
    return []


def first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def load_air_sites(site_ids: set[int] | None = None) -> list[dict[str, Any]]:
    projects = extract_projects(get_json(f"{BASE_URL}/projects"))
    rows: list[dict[str, Any]] = []

    for project in projects:
        project_id = project.get("id")
        if project_id is None:
            continue

        for site in extract_sites(get_json(f"{BASE_URL}/projects/{project_id}")):
            site_id = site.get("id")
            if site_id is None:
                continue

            site_id = int(site_id)
            if site_ids is not None and site_id not in site_ids:
                continue

            rows.append(
                {
                    "site_id": site_id,
                    "site_name": first_not_none(site.get("name"), site.get("title")),
                    "project_id": project_id,
                    "project_name": project.get("name"),
                    "project_short_name": project.get("short_name"),
                    "latitude": first_not_none(site.get("geom_y"), site.get("lat"), site.get("latitude")),
                    "longitude": first_not_none(site.get("geom_x"), site.get("lon"), site.get("longitude")),
                    "raw_json": json.dumps(site, ensure_ascii=False),
                }
            )

    return sorted(rows, key=lambda row: row["site_id"])


def save_sites_csv(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        raise ValueError("No site rows to save. Check site ids.")

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def download_air_sites(
    *,
    site_ids: list[int],
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    output_basename: str = DEFAULT_OUTPUT_BASENAME,
) -> Path:
    rows = load_air_sites(set(site_ids))
    csv_path = output_dir / f"{output_basename}.csv"
    save_sites_csv(rows, csv_path)

    print(f"Downloaded sites: {len(rows)}")
    return csv_path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sites", default=os.getenv("AIR_SITES"))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--output-basename", default=DEFAULT_OUTPUT_BASENAME)
    args = parser.parse_args()

    if not args.sites:
        parser.error("--sites is required")

    csv_path = download_air_sites(
        site_ids=parse_site_ids(args.sites),
        output_dir=Path(args.output_dir),
        output_basename=args.output_basename,
    )
    print(f"Saved CSV: {csv_path}")


if __name__ == "__main__":
    main()
