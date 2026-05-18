from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
ENV_FILE = ROOT_DIR / ".env"

DEFAULT_START = "2026-04-01 00:00:00"
DEFAULT_END = "2026-05-01 00:00:00"
DEFAULT_SITE_IDS = "3837,3857,3849,3479,3851"
DEFAULT_TIME_INTERVAL = "hour"

RAW_AIR_CSV = ROOT_DIR / "data" / "raw" / "air" / "air_measurements.csv"
RAW_AIR_JSON = ROOT_DIR / "data" / "raw" / "air" / "air_measurements.json"
SITES_CSV = ROOT_DIR / "data" / "reference" / "air_sites.csv"
CLEANED_AIR_CSV = ROOT_DIR / "data" / "staging" / "air_cleaned.csv"

SPARK_RAW_AIR_CSV = "/opt/krasair/data/raw/air/air_measurements.csv"
SPARK_CLEANED_AIR_CSV = "/opt/krasair/data/staging/air_cleaned.csv"
SPARK_CLEAN_SCRIPT = "/opt/krasair/scripts/clean/clean_air_data.py"
DATA_DIRS = [
    ROOT_DIR / "data",
    ROOT_DIR / "data" / "raw",
    ROOT_DIR / "data" / "raw" / "air",
    ROOT_DIR / "data" / "reference",
    ROOT_DIR / "data" / "staging",
    ROOT_DIR / "data" / "marts",
]


def run(command: list[str], *, env: dict[str, str] | None = None) -> None:
    print()
    print("+", " ".join(command))
    subprocess.run(command, cwd=ROOT_DIR, env=env, check=True)


def read_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}

    if not path.exists():
        return values

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")

    return values


def build_env() -> dict[str, str]:
    env = os.environ.copy()
    dotenv = read_dotenv(ENV_FILE)

    for key, value in dotenv.items():
        env.setdefault(key, value)

    env.setdefault("POSTGRES_HOST", "localhost")
    env.setdefault("POSTGRES_PORT", "5432")
    env.setdefault("POSTGRES_DB", dotenv.get("POSTGRES_DB", "laba3"))
    env.setdefault("POSTGRES_USER", dotenv.get("POSTGRES_USER", "laba3"))
    env.setdefault("POSTGRES_PASSWORD", dotenv.get("POSTGRES_PASSWORD", "laba3"))
    return env


def ensure_data_dirs() -> None:
    for path in DATA_DIRS:
        path.mkdir(parents=True, exist_ok=True)


def clean_data_dir() -> None:
    data_dir = ROOT_DIR / "data"
    if data_dir.exists():
        shutil.rmtree(data_dir)
        print(f"Removed: {data_dir}")
    else:
        print(f"Nothing to remove: {data_dir}")


def start_infrastructure() -> None:
    run(["docker", "compose", "-f", "compose.yml", "up", "-d", "postgres", "spark-master", "spark-worker"])


def download_raw_data(args: argparse.Namespace, env: dict[str, str]) -> None:
    run(
        [
            sys.executable,
            "scripts/download/download_air.py",
            "--start",
            args.start,
            "--end",
            args.end,
            "--sites",
            args.sites,
            "--time-interval",
            args.time_interval,
            "--output-dir",
            str(RAW_AIR_CSV.parent),
            "--output-basename",
            RAW_AIR_CSV.stem,
        ],
        env=env,
    )

    run(
        [
            sys.executable,
            "scripts/download/download_air_sites.py",
            "--sites",
            args.sites,
            "--output-dir",
            str(SITES_CSV.parent),
            "--output-basename",
            SITES_CSV.stem,
        ],
        env=env,
    )


def clean_with_spark() -> None:
    run(
        [
            "docker",
            "exec",
            "laba3-spark-master",
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            SPARK_CLEAN_SCRIPT,
            "--input",
            SPARK_RAW_AIR_CSV,
            "--output",
            SPARK_CLEANED_AIR_CSV,
        ]
    )


def load_to_postgres(env: dict[str, str]) -> None:
    run(
        [
            sys.executable,
            "scripts/load/load_cleaned_to_postgres.py",
            "--cleaned-csv",
            str(CLEANED_AIR_CSV),
            "--sites-csv",
            str(SITES_CSV),
        ],
        env=env,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Laba3 air preprocessing pipeline.")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--sites", default=DEFAULT_SITE_IDS)
    parser.add_argument("--time-interval", default=DEFAULT_TIME_INTERVAL)
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove data directory and exit without running pipeline steps.",
    )
    parser.add_argument(
        "--from-step",
        type=int,
        choices=range(1, 5),
        default=1,
        help="Start from a pipeline step: 1, 2, 3 or 4.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.clean:
        clean_data_dir()
        return

    env = build_env()
    ensure_data_dirs()

    print("Pipeline config")
    print(f"  Start: {args.start}")
    print(f"  End: {args.end}")
    print(f"  Sites: {args.sites}")
    print(f"  Time interval: {args.time_interval}")
    print(f"  Raw CSV: {RAW_AIR_CSV}")
    print(f"  Raw JSON: {RAW_AIR_JSON}")
    print(f"  Sites CSV: {SITES_CSV}")
    print(f"  Cleaned CSV: {CLEANED_AIR_CSV}")
    start_step = args.from_step
    print(f"  From step: {args.from_step}")

    if start_step <= 1:
        print("\n[1/4] Starting infrastructure")
        start_infrastructure()
    else:
        print("\n[1/4] Skipping infrastructure")

    if start_step <= 2:
        print("\n[2/4] Downloading raw air data and site names")
        download_raw_data(args, env)
    else:
        print("\n[2/4] Skipping raw download")

    if start_step <= 3:
        print("\n[3/4] Cleaning data with Spark")
        clean_with_spark()
    else:
        print("\n[3/4] Skipping Spark cleaning")

    if start_step <= 4:
        print("\n[4/4] Loading cleaned data and site names into Postgres")
        load_to_postgres(env)

    print("\nPipeline finished")


if __name__ == "__main__":
    main()
