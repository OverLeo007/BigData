from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


DEFAULT_CLEANED_CSV = Path("data/staging/air_cleaned.csv")
DEFAULT_SITES_CSV = Path("data/reference/air_sites.csv")
DEFAULT_WEATHER_CSV = Path("data/staging/weather_cleaned.csv")
DEFAULT_NMU_CSV = Path("data/staging/nmu_hourly.csv")
DEFAULT_AIR_WEATHER_HOURLY_CSV = Path("data/marts/air_weather_hourly.csv")
DEFAULT_AIR_WEATHER_DAILY_CSV = Path("data/marts/air_weather_daily.csv")
DEFAULT_POLLUTION_BY_WIND_CSV = Path("data/marts/pollution_by_wind.csv")
DEFAULT_DATA_QUALITY_CSV = Path("data/marts/data_quality_summary.csv")
DEFAULT_ML_REGRESSION_CSV = Path("data/marts/ml_regression_predictions.csv")
DEFAULT_ML_CLASSIFICATION_CSV = Path("data/marts/ml_classification_predictions.csv")
DEFAULT_ML_METRICS_CSV = Path("data/marts/ml_metrics.csv")
DEFAULT_ML_FEATURE_IMPORTANCE_CSV = Path("data/marts/ml_feature_importance.csv")
DEFAULT_ML_CLUSTERS_CSV = Path("data/marts/ml_clusters.csv")
DEFAULT_ML_CLUSTER_PROFILE_CSV = Path("data/marts/ml_cluster_profile.csv")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "laba5")
POSTGRES_USER = os.getenv("POSTGRES_USER", "laba5")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "laba5")

AIR_TABLE = "air_cleaned"
SITES_TABLE = "air_sites"
WEATHER_TABLE = "weather_cleaned"
NMU_TABLE = "nmu_hourly"
AIR_WEATHER_HOURLY_TABLE = "mart_air_weather_hourly"
AIR_WEATHER_DAILY_TABLE = "mart_air_weather_daily"
POLLUTION_BY_WIND_TABLE = "mart_pollution_by_wind"
DATA_QUALITY_TABLE = "mart_data_quality_summary"
ML_REGRESSION_TABLE = "ml_regression_predictions"
ML_CLASSIFICATION_TABLE = "ml_classification_predictions"
ML_METRICS_TABLE = "ml_metrics"
ML_FEATURE_IMPORTANCE_TABLE = "ml_feature_importance"
ML_CLUSTERS_TABLE = "ml_clusters"
ML_CLUSTER_PROFILE_TABLE = "ml_cluster_profile"

TABLE_NAMES = [
    AIR_TABLE,
    SITES_TABLE,
    WEATHER_TABLE,
    NMU_TABLE,
    AIR_WEATHER_HOURLY_TABLE,
    AIR_WEATHER_DAILY_TABLE,
    POLLUTION_BY_WIND_TABLE,
    DATA_QUALITY_TABLE,
    ML_REGRESSION_TABLE,
    ML_CLASSIFICATION_TABLE,
    ML_METRICS_TABLE,
    ML_FEATURE_IMPORTANCE_TABLE,
    ML_CLUSTERS_TABLE,
    ML_CLUSTER_PROFILE_TABLE,
]

VIEW_NAMES = [
    "v_air_daily_by_site",
    "v_air_hourly_profile",
    "v_air_site_summary",
    "v_air_weather_daily_by_site",
    "v_pollution_by_wind",
    "v_data_quality_summary",
    "v_pollution_by_nmu",
    "v_ml_regression",
    "v_ml_classification",
    "v_ml_clusters",
    "v_ml_cluster_profile",
    "v_ml_metrics",
    "v_ml_feature_importance",
]


def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")


def to_bool(series: pd.Series) -> pd.Series:
    return series.astype("string").str.lower().map({"true": True, "false": False}).fillna(False)


def normalize_air_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    for column in ["site_id", "hour", "month"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")

    for column in ["aqi", "iaqi", "pm25", "pm10", "pm25_mcp", "temperature", "humidity", "pressure"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    for column in ["is_pollution_peak", "has_missing_pollution_value"]:
        df[column] = to_bool(df[column])

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

    df["has_precipitation"] = to_bool(df["has_precipitation"])
    return df


def normalize_nmu_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["hour"] = pd.to_numeric(df["hour"], errors="coerce").astype("Int64")
    df["regime_level"] = pd.to_numeric(df["regime_level"], errors="coerce").astype("Int64")
    df["is_nmu"] = to_bool(df["is_nmu"])
    df["regime"] = df["regime"].astype("string")
    df["announcement_text"] = df["announcement_text"].astype("string")
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
        df[column] = to_bool(df[column])

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


def normalize_ml_regression_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for column in ["site_id", "hour"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    for column in ["actual_aqi", "predicted_aqi", "abs_error"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["model"] = df["model"].astype("string")
    return df


def normalize_ml_classification_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for column in ["site_id", "hour", "actual_peak", "predicted_peak"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    df["peak_probability"] = pd.to_numeric(df["peak_probability"], errors="coerce")
    df["model"] = df["model"].astype("string")
    return df


def normalize_ml_metrics_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for column in ["task", "model", "metric"]:
        df[column] = df[column].astype("string")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def normalize_ml_feature_importance_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for column in ["task", "model", "feature"]:
        df[column] = df[column].astype("string")
    df["importance"] = pd.to_numeric(df["importance"], errors="coerce")
    return df


def normalize_ml_clusters_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for column in ["site_id", "hour", "cluster", "is_nmu"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    for column in ["aqi", "pm25", "weather_temperature", "wind_speed"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def normalize_ml_cluster_profile_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for column in ["cluster", "rows_count"]:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    for column in df.columns:
        if column not in ("cluster", "rows_count"):
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

    create or replace view v_pollution_by_nmu as
    select
        case when n.timestamp is not null then 'НМУ' else 'Обычные часы' end as nmu_status,
        count(*) as rows_count,
        avg(a.aqi) as avg_aqi,
        avg(a.pm25) as avg_pm25,
        avg(a.pm10) as avg_pm10,
        sum(case when a.is_pollution_peak then 1 else 0 end) as pollution_peak_count,
        avg(case when a.is_pollution_peak then 1.0 else 0.0 end) as pollution_peak_share
    from air_cleaned a
    left join (select distinct timestamp from nmu_hourly) n on n.timestamp = a.timestamp
    group by 1;

    create or replace view v_ml_regression as
    select
        r.site_id,
        s.site_name,
        r.timestamp,
        r.date,
        r.hour,
        r.model,
        r.actual_aqi,
        r.predicted_aqi,
        r.abs_error
    from ml_regression_predictions r
    left join air_sites s on s.site_id = r.site_id;

    create or replace view v_ml_classification as
    select
        c.site_id,
        s.site_name,
        c.timestamp,
        c.date,
        c.hour,
        c.model,
        c.actual_peak,
        c.predicted_peak,
        c.peak_probability,
        case when c.actual_peak = c.predicted_peak then 1 else 0 end as is_correct
    from ml_classification_predictions c
    left join air_sites s on s.site_id = c.site_id;

    create or replace view v_ml_clusters as
    select
        cl.site_id,
        s.site_name,
        cl.timestamp,
        cl.date,
        cl.hour,
        cl.cluster,
        cl.aqi,
        cl.pm25,
        cl.weather_temperature,
        cl.wind_speed,
        cl.is_nmu
    from ml_clusters cl
    left join air_sites s on s.site_id = cl.site_id;

    create or replace view v_ml_cluster_profile as
    select * from ml_cluster_profile;

    create or replace view v_ml_metrics as
    select * from ml_metrics;

    create or replace view v_ml_feature_importance as
    select * from ml_feature_importance;
    """

    with engine.begin() as connection:
        connection.execute(text(sql))


def recreate_tables(engine, tables: dict[str, pd.DataFrame]) -> None:
    drop_sql = "\n".join(
        [f"drop view if exists {view_name} cascade;" for view_name in VIEW_NAMES]
        + [f"drop table if exists {table_name} cascade;" for table_name in TABLE_NAMES]
    )

    with engine.begin() as connection:
        connection.execute(text(drop_sql))

    for table_name, df in tables.items():
        df.to_sql(table_name, engine, if_exists="fail", index=False, chunksize=1000)


def load_to_postgres(paths: dict[str, Path]) -> None:
    for path in paths.values():
        require_file(path)

    tables = {
        SITES_TABLE: normalize_sites_dataframe(pd.read_csv(paths["sites"])),
        AIR_TABLE: normalize_air_dataframe(pd.read_csv(paths["air"])),
        WEATHER_TABLE: normalize_weather_dataframe(pd.read_csv(paths["weather"])),
        NMU_TABLE: normalize_nmu_dataframe(pd.read_csv(paths["nmu"])),
        AIR_WEATHER_HOURLY_TABLE: normalize_hourly_mart_dataframe(pd.read_csv(paths["hourly"])),
        AIR_WEATHER_DAILY_TABLE: normalize_daily_mart_dataframe(pd.read_csv(paths["daily"])),
        POLLUTION_BY_WIND_TABLE: normalize_wind_mart_dataframe(pd.read_csv(paths["wind"])),
        DATA_QUALITY_TABLE: normalize_quality_mart_dataframe(pd.read_csv(paths["quality"])),
        ML_REGRESSION_TABLE: normalize_ml_regression_dataframe(pd.read_csv(paths["ml_regression"])),
        ML_CLASSIFICATION_TABLE: normalize_ml_classification_dataframe(pd.read_csv(paths["ml_classification"])),
        ML_METRICS_TABLE: normalize_ml_metrics_dataframe(pd.read_csv(paths["ml_metrics"])),
        ML_FEATURE_IMPORTANCE_TABLE: normalize_ml_feature_importance_dataframe(pd.read_csv(paths["ml_feature_importance"])),
        ML_CLUSTERS_TABLE: normalize_ml_clusters_dataframe(pd.read_csv(paths["ml_clusters"])),
        ML_CLUSTER_PROFILE_TABLE: normalize_ml_cluster_profile_dataframe(pd.read_csv(paths["ml_cluster_profile"])),
    }

    engine = create_engine(postgres_url())
    recreate_tables(engine, tables)
    create_views(engine)

    for table_name, df in tables.items():
        print(f"Rows loaded into {table_name}: {len(df)}")
    print("Views created: " + ", ".join(VIEW_NAMES))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cleaned-csv", default=str(DEFAULT_CLEANED_CSV))
    parser.add_argument("--sites-csv", default=str(DEFAULT_SITES_CSV))
    parser.add_argument("--weather-csv", default=str(DEFAULT_WEATHER_CSV))
    parser.add_argument("--nmu-csv", default=str(DEFAULT_NMU_CSV))
    parser.add_argument("--air-weather-hourly-csv", default=str(DEFAULT_AIR_WEATHER_HOURLY_CSV))
    parser.add_argument("--air-weather-daily-csv", default=str(DEFAULT_AIR_WEATHER_DAILY_CSV))
    parser.add_argument("--pollution-by-wind-csv", default=str(DEFAULT_POLLUTION_BY_WIND_CSV))
    parser.add_argument("--data-quality-csv", default=str(DEFAULT_DATA_QUALITY_CSV))
    parser.add_argument("--ml-regression-csv", default=str(DEFAULT_ML_REGRESSION_CSV))
    parser.add_argument("--ml-classification-csv", default=str(DEFAULT_ML_CLASSIFICATION_CSV))
    parser.add_argument("--ml-metrics-csv", default=str(DEFAULT_ML_METRICS_CSV))
    parser.add_argument("--ml-feature-importance-csv", default=str(DEFAULT_ML_FEATURE_IMPORTANCE_CSV))
    parser.add_argument("--ml-clusters-csv", default=str(DEFAULT_ML_CLUSTERS_CSV))
    parser.add_argument("--ml-cluster-profile-csv", default=str(DEFAULT_ML_CLUSTER_PROFILE_CSV))
    args = parser.parse_args()

    paths = {
        "air": Path(args.cleaned_csv),
        "sites": Path(args.sites_csv),
        "weather": Path(args.weather_csv),
        "nmu": Path(args.nmu_csv),
        "hourly": Path(args.air_weather_hourly_csv),
        "daily": Path(args.air_weather_daily_csv),
        "wind": Path(args.pollution_by_wind_csv),
        "quality": Path(args.data_quality_csv),
        "ml_regression": Path(args.ml_regression_csv),
        "ml_classification": Path(args.ml_classification_csv),
        "ml_metrics": Path(args.ml_metrics_csv),
        "ml_feature_importance": Path(args.ml_feature_importance_csv),
        "ml_clusters": Path(args.ml_clusters_csv),
        "ml_cluster_profile": Path(args.ml_cluster_profile_csv),
    }

    load_to_postgres(paths)


if __name__ == "__main__":
    main()
