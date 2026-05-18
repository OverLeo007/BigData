from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


DEFAULT_CLEANED_CSV = Path("data/staging/air_cleaned.csv")
DEFAULT_SITES_CSV = Path("data/reference/air_sites.csv")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "laba3")
POSTGRES_USER = os.getenv("POSTGRES_USER", "laba3")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "laba3")

AIR_TABLE = "air_cleaned"
SITES_TABLE = "air_sites"
VIEW_NAMES = [
    "v_air_daily_by_site",
    "v_air_hourly_profile",
    "v_air_site_summary",
]


def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")


def normalize_air_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    for column in ["site_id", "hour", "month"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

    for column in [
        "aqi",
        "iaqi",
        "pm25",
        "pm10",
        "pm25_mcp",
        "temperature",
        "humidity",
        "pressure",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    for column in ["is_pollution_peak", "has_missing_pollution_value"]:
        df[column] = df[column].astype("string").str.lower().map({"true": True, "false": False}).fillna(False)

    return df


def normalize_sites_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["site_id"] = pd.to_numeric(df["site_id"], errors="coerce").astype("Int64")
    df["project_id"] = pd.to_numeric(df["project_id"], errors="coerce").astype("Int64")

    for column in ["latitude", "longitude"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def postgres_url() -> str:
    return (
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )


def create_views(engine) -> None:
    sql = """
    create or replace view v_air_daily_by_site as
    select
        a.date,
        a.site_id,
        s.site_name,
        count(*) as rows_count,
        avg(a.aqi) as avg_aqi,
        max(a.aqi) as max_aqi,
        avg(a.pm25) as avg_pm25,
        max(a.pm25) as max_pm25,
        avg(a.pm10) as avg_pm10,
        max(a.pm10) as max_pm10,
        avg(a.temperature) as avg_temperature,
        avg(a.humidity) as avg_humidity,
        avg(a.pressure) as avg_pressure,
        sum(case when a.is_pollution_peak then 1 else 0 end) as pollution_peak_count,
        sum(case when a.has_missing_pollution_value then 1 else 0 end) as missing_pollution_count
    from air_cleaned a
    left join air_sites s on s.site_id = a.site_id
    group by a.date, a.site_id, s.site_name;

    create or replace view v_air_hourly_profile as
    select
        a.site_id,
        s.site_name,
        a.hour,
        count(*) as rows_count,
        avg(a.aqi) as avg_aqi,
        avg(a.pm25) as avg_pm25,
        avg(a.pm10) as avg_pm10,
        avg(a.temperature) as avg_temperature,
        sum(case when a.is_pollution_peak then 1 else 0 end) as pollution_peak_count
    from air_cleaned a
    left join air_sites s on s.site_id = a.site_id
    group by a.site_id, s.site_name, a.hour;

    create or replace view v_air_site_summary as
    select
        a.site_id,
        s.site_name,
        count(*) as rows_count,
        min(a.timestamp) as first_timestamp,
        max(a.timestamp) as last_timestamp,
        avg(a.aqi) as avg_aqi,
        max(a.aqi) as max_aqi,
        avg(a.pm25) as avg_pm25,
        max(a.pm25) as max_pm25,
        avg(a.pm10) as avg_pm10,
        max(a.pm10) as max_pm10,
        sum(case when a.is_pollution_peak then 1 else 0 end) as pollution_peak_count,
        sum(case when a.has_missing_pollution_value then 1 else 0 end) as missing_pollution_count
    from air_cleaned a
    left join air_sites s on s.site_id = a.site_id
    group by a.site_id, s.site_name;
    """

    with engine.begin() as connection:
        connection.execute(text(sql))


def recreate_tables(engine, air_df: pd.DataFrame, sites_df: pd.DataFrame) -> None:
    drop_sql = "\n".join(
        [f"drop view if exists {view_name} cascade;" for view_name in VIEW_NAMES]
        + [
            f"drop table if exists {AIR_TABLE} cascade;",
            f"drop table if exists {SITES_TABLE} cascade;",
        ]
    )

    with engine.begin() as connection:
        connection.execute(text(drop_sql))

    sites_df.to_sql(SITES_TABLE, engine, if_exists="fail", index=False, chunksize=1000)
    air_df.to_sql(AIR_TABLE, engine, if_exists="fail", index=False, chunksize=1000)


def load_to_postgres(cleaned_csv: Path, sites_csv: Path) -> None:
    require_file(cleaned_csv)
    require_file(sites_csv)

    air_df = normalize_air_dataframe(pd.read_csv(cleaned_csv))
    sites_df = normalize_sites_dataframe(pd.read_csv(sites_csv))
    engine = create_engine(postgres_url())

    recreate_tables(engine, air_df, sites_df)
    create_views(engine)

    print(f"Rows loaded into {AIR_TABLE}: {len(air_df)}")
    print(f"Rows loaded into {SITES_TABLE}: {len(sites_df)}")
    print("Views created: v_air_daily_by_site, v_air_hourly_profile, v_air_site_summary")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cleaned-csv", default=str(DEFAULT_CLEANED_CSV))
    parser.add_argument("--sites-csv", default=str(DEFAULT_SITES_CSV))
    args = parser.parse_args()

    load_to_postgres(Path(args.cleaned_csv), Path(args.sites_csv))


if __name__ == "__main__":
    main()
