from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


DEFAULT_CLEANED_CSV = Path("data/staging/air_cleaned.csv")
DEFAULT_SITES_CSV = Path("data/reference/air_sites.csv")
DEFAULT_WEATHER_CSV = Path("data/staging/weather_cleaned.csv")
DEFAULT_AIR_WEATHER_HOURLY_CSV = Path("data/marts/air_weather_hourly.csv")
DEFAULT_AIR_WEATHER_DAILY_CSV = Path("data/marts/air_weather_daily.csv")
DEFAULT_POLLUTION_BY_WIND_CSV = Path("data/marts/pollution_by_wind.csv")
DEFAULT_DATA_QUALITY_CSV = Path("data/marts/data_quality_summary.csv")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "laba4")
POSTGRES_USER = os.getenv("POSTGRES_USER", "laba4")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "laba4")

AIR_TABLE = "air_cleaned"
SITES_TABLE = "air_sites"
WEATHER_TABLE = "weather_cleaned"
AIR_WEATHER_HOURLY_TABLE = "mart_air_weather_hourly"
AIR_WEATHER_DAILY_TABLE = "mart_air_weather_daily"
POLLUTION_BY_WIND_TABLE = "mart_pollution_by_wind"
DATA_QUALITY_TABLE = "mart_data_quality_summary"
VIEW_NAMES = [
    "v_air_daily_by_site",
    "v_air_hourly_profile",
    "v_air_site_summary",
    "v_air_weather_daily_by_site",
    "v_pollution_by_wind",
    "v_data_quality_summary",
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


def normalize_weather_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["hour"] = pd.to_numeric(df["hour"], errors="coerce").astype("Int64")

    for column in [
        "weather_temperature",
        "weather_humidity",
        "precipitation",
        "rain",
        "snowfall",
        "surface_pressure",
        "wind_speed",
        "wind_direction",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df["has_precipitation"] = df["has_precipitation"].astype("string").str.lower().map({"true": True, "false": False}).fillna(False)
    return df


def normalize_hourly_mart_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    for column in ["site_id", "hour", "month"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

    for column in [
        "aqi",
        "pm25",
        "pm10",
        "air_temperature",
        "air_humidity",
        "air_pressure",
        "weather_temperature",
        "weather_humidity",
        "precipitation",
        "rain",
        "snowfall",
        "surface_pressure",
        "wind_speed",
        "wind_direction",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    for column in ["is_pollution_peak", "has_missing_pollution_value", "has_precipitation"]:
        df[column] = df[column].astype("string").str.lower().map({"true": True, "false": False}).fillna(False)

    return df


def normalize_daily_mart_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for column in ["site_id", "rows_count", "pollution_peak_count", "precipitation_hours"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

    for column in [
        "avg_aqi",
        "max_aqi",
        "avg_pm25",
        "max_pm25",
        "avg_pm10",
        "max_pm10",
        "avg_weather_temperature",
        "avg_weather_humidity",
        "avg_wind_speed",
        "total_precipitation",
    ]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def normalize_wind_mart_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for column in ["site_id", "rows_count", "pollution_peak_count"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

    for column in ["avg_aqi", "avg_pm25", "avg_pm10"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def normalize_quality_mart_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for column in ["rows_count", "missing_weather_rows", "missing_pollution_rows", "site_count"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

    for column in ["first_timestamp", "last_timestamp"]:
        df[column] = pd.to_datetime(df[column], errors="coerce")

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

    create or replace view v_air_weather_daily_by_site as
    select
        d.date,
        d.site_id,
        s.site_name,
        d.rows_count,
        d.avg_aqi,
        d.max_aqi,
        d.avg_pm25,
        d.max_pm25,
        d.avg_pm10,
        d.max_pm10,
        d.avg_weather_temperature,
        d.avg_weather_humidity,
        d.avg_wind_speed,
        d.total_precipitation,
        d.pollution_peak_count,
        d.precipitation_hours
    from mart_air_weather_daily d
    left join air_sites s on s.site_id = d.site_id;

    create or replace view v_pollution_by_wind as
    select
        w.site_id,
        s.site_name,
        w.wind_speed_group,
        w.rows_count,
        w.avg_aqi,
        w.avg_pm25,
        w.avg_pm10,
        w.pollution_peak_count
    from mart_pollution_by_wind w
    left join air_sites s on s.site_id = w.site_id;

    create or replace view v_data_quality_summary as
    select *
    from mart_data_quality_summary;
    """

    with engine.begin() as connection:
        connection.execute(text(sql))


def recreate_tables(
    engine,
    air_df: pd.DataFrame,
    sites_df: pd.DataFrame,
    weather_df: pd.DataFrame,
    hourly_df: pd.DataFrame,
    daily_df: pd.DataFrame,
    wind_df: pd.DataFrame,
    quality_df: pd.DataFrame,
) -> None:
    drop_sql = "\n".join(
        [f"drop view if exists {view_name} cascade;" for view_name in VIEW_NAMES]
        + [
            f"drop table if exists {AIR_TABLE} cascade;",
            f"drop table if exists {SITES_TABLE} cascade;",
            f"drop table if exists {WEATHER_TABLE} cascade;",
            f"drop table if exists {AIR_WEATHER_HOURLY_TABLE} cascade;",
            f"drop table if exists {AIR_WEATHER_DAILY_TABLE} cascade;",
            f"drop table if exists {POLLUTION_BY_WIND_TABLE} cascade;",
            f"drop table if exists {DATA_QUALITY_TABLE} cascade;",
        ]
    )

    with engine.begin() as connection:
        connection.execute(text(drop_sql))

    sites_df.to_sql(SITES_TABLE, engine, if_exists="fail", index=False, chunksize=1000)
    air_df.to_sql(AIR_TABLE, engine, if_exists="fail", index=False, chunksize=1000)
    weather_df.to_sql(WEATHER_TABLE, engine, if_exists="fail", index=False, chunksize=1000)
    hourly_df.to_sql(AIR_WEATHER_HOURLY_TABLE, engine, if_exists="fail", index=False, chunksize=1000)
    daily_df.to_sql(AIR_WEATHER_DAILY_TABLE, engine, if_exists="fail", index=False, chunksize=1000)
    wind_df.to_sql(POLLUTION_BY_WIND_TABLE, engine, if_exists="fail", index=False, chunksize=1000)
    quality_df.to_sql(DATA_QUALITY_TABLE, engine, if_exists="fail", index=False, chunksize=1000)


def load_to_postgres(
    cleaned_csv: Path,
    sites_csv: Path,
    weather_csv: Path,
    air_weather_hourly_csv: Path,
    air_weather_daily_csv: Path,
    pollution_by_wind_csv: Path,
    data_quality_csv: Path,
) -> None:
    require_file(cleaned_csv)
    require_file(sites_csv)
    require_file(weather_csv)
    require_file(air_weather_hourly_csv)
    require_file(air_weather_daily_csv)
    require_file(pollution_by_wind_csv)
    require_file(data_quality_csv)

    air_df = normalize_air_dataframe(pd.read_csv(cleaned_csv))
    sites_df = normalize_sites_dataframe(pd.read_csv(sites_csv))
    weather_df = normalize_weather_dataframe(pd.read_csv(weather_csv))
    hourly_df = normalize_hourly_mart_dataframe(pd.read_csv(air_weather_hourly_csv))
    daily_df = normalize_daily_mart_dataframe(pd.read_csv(air_weather_daily_csv))
    wind_df = normalize_wind_mart_dataframe(pd.read_csv(pollution_by_wind_csv))
    quality_df = normalize_quality_mart_dataframe(pd.read_csv(data_quality_csv))
    engine = create_engine(postgres_url())

    recreate_tables(engine, air_df, sites_df, weather_df, hourly_df, daily_df, wind_df, quality_df)
    create_views(engine)

    print(f"Rows loaded into {AIR_TABLE}: {len(air_df)}")
    print(f"Rows loaded into {SITES_TABLE}: {len(sites_df)}")
    print(f"Rows loaded into {WEATHER_TABLE}: {len(weather_df)}")
    print(f"Rows loaded into {AIR_WEATHER_HOURLY_TABLE}: {len(hourly_df)}")
    print(f"Rows loaded into {AIR_WEATHER_DAILY_TABLE}: {len(daily_df)}")
    print(f"Rows loaded into {POLLUTION_BY_WIND_TABLE}: {len(wind_df)}")
    print(f"Rows loaded into {DATA_QUALITY_TABLE}: {len(quality_df)}")
    print("Views created: " + ", ".join(VIEW_NAMES))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cleaned-csv", default=str(DEFAULT_CLEANED_CSV))
    parser.add_argument("--sites-csv", default=str(DEFAULT_SITES_CSV))
    parser.add_argument("--weather-csv", default=str(DEFAULT_WEATHER_CSV))
    parser.add_argument("--air-weather-hourly-csv", default=str(DEFAULT_AIR_WEATHER_HOURLY_CSV))
    parser.add_argument("--air-weather-daily-csv", default=str(DEFAULT_AIR_WEATHER_DAILY_CSV))
    parser.add_argument("--pollution-by-wind-csv", default=str(DEFAULT_POLLUTION_BY_WIND_CSV))
    parser.add_argument("--data-quality-csv", default=str(DEFAULT_DATA_QUALITY_CSV))
    args = parser.parse_args()

    load_to_postgres(
        Path(args.cleaned_csv),
        Path(args.sites_csv),
        Path(args.weather_csv),
        Path(args.air_weather_hourly_csv),
        Path(args.air_weather_daily_csv),
        Path(args.pollution_by_wind_csv),
        Path(args.data_quality_csv),
    )


if __name__ == "__main__":
    main()
