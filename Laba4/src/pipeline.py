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
DEFAULT_WEATHER_LATITUDE = "56.0153"
DEFAULT_WEATHER_LONGITUDE = "92.8932"
DEFAULT_WEATHER_TIMEZONE = "Asia/Krasnoyarsk"
DEFAULT_CHUNK_DAYS = "15"
DEFAULT_AIR_PAGE_SIZE = "10000"

RAW_AIR_CSV = ROOT_DIR / "data" / "raw" / "air" / "air_measurements.csv"
RAW_AIR_JSON = ROOT_DIR / "data" / "raw" / "air" / "air_measurements.json"
RAW_WEATHER_CSV = ROOT_DIR / "data" / "raw" / "weather" / "weather_hourly.csv"
RAW_WEATHER_JSON = ROOT_DIR / "data" / "raw" / "weather" / "weather_hourly.json"
SITES_CSV = ROOT_DIR / "data" / "reference" / "air_sites.csv"
CLEANED_AIR_CSV = ROOT_DIR / "data" / "staging" / "air_cleaned.csv"
CLEANED_WEATHER_CSV = ROOT_DIR / "data" / "staging" / "weather_cleaned.csv"
MART_AIR_WEATHER_HOURLY_CSV = ROOT_DIR / "data" / "marts" / "air_weather_hourly.csv"
MART_AIR_WEATHER_DAILY_CSV = ROOT_DIR / "data" / "marts" / "air_weather_daily.csv"
MART_POLLUTION_BY_WIND_CSV = ROOT_DIR / "data" / "marts" / "pollution_by_wind.csv"
MART_DATA_QUALITY_CSV = ROOT_DIR / "data" / "marts" / "data_quality_summary.csv"

SPARK_RAW_AIR_CSV = "/opt/krasair/data/raw/air/air_measurements.csv"
SPARK_RAW_WEATHER_CSV = "/opt/krasair/data/raw/weather/weather_hourly.csv"
SPARK_CLEANED_AIR_CSV = "/opt/krasair/data/staging/air_cleaned.csv"
SPARK_CLEANED_WEATHER_CSV = "/opt/krasair/data/staging/weather_cleaned.csv"
SPARK_MART_AIR_WEATHER_HOURLY_CSV = "/opt/krasair/data/marts/air_weather_hourly.csv"
SPARK_MART_AIR_WEATHER_DAILY_CSV = "/opt/krasair/data/marts/air_weather_daily.csv"
SPARK_MART_POLLUTION_BY_WIND_CSV = "/opt/krasair/data/marts/pollution_by_wind.csv"
SPARK_MART_DATA_QUALITY_CSV = "/opt/krasair/data/marts/data_quality_summary.csv"
SPARK_CLEAN_AIR_SCRIPT = "/opt/krasair/scripts/clean/clean_air_data.py"
SPARK_CLEAN_WEATHER_SCRIPT = "/opt/krasair/scripts/clean/clean_weather_data.py"
SPARK_MARTS_SCRIPT = "/opt/krasair/scripts/marts/build_data_marts.py"
DATA_DIRS = [
    ROOT_DIR / "data",
    ROOT_DIR / "data" / "raw",
    ROOT_DIR / "data" / "raw" / "air",
    ROOT_DIR / "data" / "raw" / "weather",
    ROOT_DIR / "data" / "reference",
    ROOT_DIR / "data" / "staging",
    ROOT_DIR / "data" / "marts",
]


def run(command: list[str], *, env: dict[str, str] | None = None) -> None:
    print()
    print("+", " ".join(command))
    subprocess.run(command, cwd=ROOT_DIR, env=env, check=True)


def capture(command: list[str], *, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(
        command,
        cwd=ROOT_DIR,
        env=env,
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout


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
    env.setdefault("POSTGRES_DB", dotenv.get("POSTGRES_DB", "laba4"))
    env.setdefault("POSTGRES_USER", dotenv.get("POSTGRES_USER", "laba4"))
    env.setdefault("POSTGRES_PASSWORD", dotenv.get("POSTGRES_PASSWORD", "laba4"))
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
    required_services = ["postgres", "spark-master", "spark-worker", "superset"]
    running_output = capture(["docker", "compose", "-f", "compose.yml", "ps", "--services", "--status", "running"])
    running_services = {line.strip() for line in running_output.splitlines() if line.strip()}
    missing_services = [service for service in required_services if service not in running_services]

    if not missing_services:
        print("Infrastructure is already running. Skipping docker compose up.")
        return

    print("Starting missing services: " + ", ".join(missing_services))
    run(["docker", "compose", "-f", "compose.yml", "up", "-d", *missing_services])


def restart_infrastructure() -> None:
    run(["docker", "compose", "-f", "compose.yml", "up", "-d", "--force-recreate", "postgres", "spark-master", "spark-worker", "superset"])


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
            "--chunk-days",
            args.chunk_days,
            "--page-size",
            args.air_page_size,
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
            "scripts/download/download_weather.py",
            "--start",
            args.start,
            "--end",
            args.end,
            "--latitude",
            args.weather_latitude,
            "--longitude",
            args.weather_longitude,
            "--timezone",
            args.weather_timezone,
            "--chunk-days",
            args.chunk_days,
            "--output-dir",
            str(RAW_WEATHER_CSV.parent),
            "--output-basename",
            RAW_WEATHER_CSV.stem,
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


def normalize_sources_with_spark() -> None:
    run(
        [
            "docker",
            "exec",
            "laba4-spark-master",
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            SPARK_CLEAN_AIR_SCRIPT,
            "--input",
            SPARK_RAW_AIR_CSV,
            "--output",
            SPARK_CLEANED_AIR_CSV,
        ]
    )

    run(
        [
            "docker",
            "exec",
            "laba4-spark-master",
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            SPARK_CLEAN_WEATHER_SCRIPT,
            "--input",
            SPARK_RAW_WEATHER_CSV,
            "--output",
            SPARK_CLEANED_WEATHER_CSV,
        ]
    )


def build_marts_with_spark() -> None:
    run(
        [
            "docker",
            "exec",
            "laba4-spark-master",
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            SPARK_MARTS_SCRIPT,
            "--air-input",
            SPARK_CLEANED_AIR_CSV,
            "--weather-input",
            SPARK_CLEANED_WEATHER_CSV,
            "--joined-output",
            SPARK_MART_AIR_WEATHER_HOURLY_CSV,
            "--daily-output",
            SPARK_MART_AIR_WEATHER_DAILY_CSV,
            "--wind-output",
            SPARK_MART_POLLUTION_BY_WIND_CSV,
            "--qa-output",
            SPARK_MART_DATA_QUALITY_CSV,
        ]
    )


def load_to_postgres(env: dict[str, str]) -> None:
    run(
        [
            sys.executable,
            "scripts/load/load_to_postgres.py",
            "--cleaned-csv",
            str(CLEANED_AIR_CSV),
            "--sites-csv",
            str(SITES_CSV),
            "--weather-csv",
            str(CLEANED_WEATHER_CSV),
            "--air-weather-hourly-csv",
            str(MART_AIR_WEATHER_HOURLY_CSV),
            "--air-weather-daily-csv",
            str(MART_AIR_WEATHER_DAILY_CSV),
            "--pollution-by-wind-csv",
            str(MART_POLLUTION_BY_WIND_CSV),
            "--data-quality-csv",
            str(MART_DATA_QUALITY_CSV),
        ],
        env=env,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Laba4 air and weather Spark pipeline.")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--sites", default=DEFAULT_SITE_IDS)
    parser.add_argument("--time-interval", default=DEFAULT_TIME_INTERVAL)
    parser.add_argument("--weather-latitude", default=DEFAULT_WEATHER_LATITUDE)
    parser.add_argument("--weather-longitude", default=DEFAULT_WEATHER_LONGITUDE)
    parser.add_argument("--weather-timezone", default=DEFAULT_WEATHER_TIMEZONE)
    parser.add_argument("--chunk-days", default=DEFAULT_CHUNK_DAYS)
    parser.add_argument("--air-page-size", default=DEFAULT_AIR_PAGE_SIZE)
    parser.add_argument(
        "--restart-services",
        action="store_true",
        help="Force recreate infrastructure containers before running data steps.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove data directory and exit without running pipeline steps.",
    )
    parser.add_argument(
        "--from-step",
        type=int,
        choices=range(1, 6),
        default=1,
        help="Start from a pipeline step: 1, 2, 3, 4 or 5.",
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
    print(f"  Weather point: {args.weather_latitude}, {args.weather_longitude}")
    print(f"  Weather timezone: {args.weather_timezone}")
    print(f"  Download chunk days: {args.chunk_days}")
    print(f"  Air page size: {args.air_page_size}")
    print(f"  Raw CSV: {RAW_AIR_CSV}")
    print(f"  Raw JSON: {RAW_AIR_JSON}")
    print(f"  Raw weather CSV: {RAW_WEATHER_CSV}")
    print(f"  Raw weather JSON: {RAW_WEATHER_JSON}")
    print(f"  Sites CSV: {SITES_CSV}")
    print(f"  Staging air CSV: {CLEANED_AIR_CSV}")
    print(f"  Staging weather CSV: {CLEANED_WEATHER_CSV}")
    print(f"  Air-weather hourly mart: {MART_AIR_WEATHER_HOURLY_CSV}")
    print(f"  Air-weather daily mart: {MART_AIR_WEATHER_DAILY_CSV}")
    start_step = args.from_step
    print(f"  From step: {args.from_step}")

    if start_step <= 1:
        print("\n[1/5] Starting infrastructure")
        if args.restart_services:
            restart_infrastructure()
        else:
            start_infrastructure()
    else:
        print("\n[1/5] Skipping infrastructure")

    if start_step <= 2:
        print("\n[2/5] Downloading raw air, weather data and site names")
        download_raw_data(args, env)
    else:
        print("\n[2/5] Skipping raw download")

    if start_step <= 3:
        print("\n[3/5] Normalizing source data to staging with Spark")
        normalize_sources_with_spark()
    else:
        print("\n[3/5] Skipping source normalization")

    if start_step <= 4:
        print("\n[4/5] Building air-weather marts with Spark")
        build_marts_with_spark()
    else:
        print("\n[4/5] Skipping Spark marts")

    if start_step <= 5:
        print("\n[5/5] Loading cleaned data, site names and marts into Postgres")
        load_to_postgres(env)
    else:
        print("\n[5/5] Skipping Postgres load")

    print("\nPipeline finished")


if __name__ == "__main__":
    main()
