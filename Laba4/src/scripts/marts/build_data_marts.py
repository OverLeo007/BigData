from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_AIR_INPUT = "/opt/krasair/data/staging/air_cleaned.csv"
DEFAULT_WEATHER_INPUT = "/opt/krasair/data/staging/weather_cleaned.csv"
DEFAULT_JOINED_OUTPUT = "/opt/krasair/data/marts/air_weather_hourly.csv"
DEFAULT_DAILY_OUTPUT = "/opt/krasair/data/marts/air_weather_daily.csv"
DEFAULT_WIND_OUTPUT = "/opt/krasair/data/marts/pollution_by_wind.csv"
DEFAULT_QA_OUTPUT = "/opt/krasair/data/marts/data_quality_summary.csv"


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("KrasAirDataMarts").getOrCreate()


def read_csv(spark: SparkSession, path: str):
    return (
        spark.read.option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )


def prepare_air(df):
    return (
        df.withColumn("site_id", F.col("site_id").cast("integer"))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("date", F.to_date("date"))
        .withColumn("hour", F.col("hour").cast("integer"))
        .withColumn("month", F.col("month").cast("integer"))
        .withColumn("aqi", F.col("aqi").cast("double"))
        .withColumn("pm25", F.col("pm25").cast("double"))
        .withColumn("pm10", F.col("pm10").cast("double"))
        .withColumn("air_temperature", F.col("temperature").cast("double"))
        .withColumn("air_humidity", F.col("humidity").cast("double"))
        .withColumn("air_pressure", F.col("pressure").cast("double"))
        .withColumn("is_pollution_peak", F.lower(F.col("is_pollution_peak").cast("string")) == "true")
        .withColumn(
            "has_missing_pollution_value",
            F.lower(F.col("has_missing_pollution_value").cast("string")) == "true",
        )
    )


def prepare_weather(df):
    return (
        df.withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("date", F.to_date("date"))
        .withColumn("hour", F.col("hour").cast("integer"))
        .withColumn("weather_temperature", F.col("weather_temperature").cast("double"))
        .withColumn("weather_humidity", F.col("weather_humidity").cast("double"))
        .withColumn("precipitation", F.col("precipitation").cast("double"))
        .withColumn("rain", F.col("rain").cast("double"))
        .withColumn("snowfall", F.col("snowfall").cast("double"))
        .withColumn("surface_pressure", F.col("surface_pressure").cast("double"))
        .withColumn("wind_speed", F.col("wind_speed").cast("double"))
        .withColumn("wind_direction", F.col("wind_direction").cast("double"))
        .withColumn("has_precipitation", F.lower(F.col("has_precipitation").cast("string")) == "true")
    )


def build_joined_air_weather(air, weather):
    return (
        air.alias("a")
        .join(weather.alias("w"), on="timestamp", how="left")
        .select(
            F.col("a.site_id"),
            F.col("a.timestamp"),
            F.col("a.date"),
            F.col("a.hour"),
            F.col("a.month"),
            F.col("a.aqi"),
            F.col("a.pm25"),
            F.col("a.pm10"),
            F.col("a.air_temperature"),
            F.col("a.air_humidity"),
            F.col("a.air_pressure"),
            F.col("a.is_pollution_peak"),
            F.col("a.has_missing_pollution_value"),
            F.col("w.weather_temperature"),
            F.col("w.weather_humidity"),
            F.col("w.precipitation"),
            F.col("w.rain"),
            F.col("w.snowfall"),
            F.col("w.surface_pressure"),
            F.col("w.wind_speed"),
            F.col("w.wind_direction"),
            F.col("w.wind_speed_group"),
            F.col("w.has_precipitation"),
        )
    )


def build_daily_mart(joined):
    return (
        joined.groupBy("date", "site_id")
        .agg(
            F.count("*").alias("rows_count"),
            F.avg("aqi").alias("avg_aqi"),
            F.max("aqi").alias("max_aqi"),
            F.avg("pm25").alias("avg_pm25"),
            F.max("pm25").alias("max_pm25"),
            F.avg("pm10").alias("avg_pm10"),
            F.max("pm10").alias("max_pm10"),
            F.avg("weather_temperature").alias("avg_weather_temperature"),
            F.avg("weather_humidity").alias("avg_weather_humidity"),
            F.avg("wind_speed").alias("avg_wind_speed"),
            F.sum("precipitation").alias("total_precipitation"),
            F.sum(F.when(F.col("is_pollution_peak"), 1).otherwise(0)).alias("pollution_peak_count"),
            F.sum(F.when(F.col("has_precipitation"), 1).otherwise(0)).alias("precipitation_hours"),
        )
        .orderBy("date", "site_id")
    )


def build_wind_mart(joined):
    return (
        joined.groupBy("site_id", "wind_speed_group")
        .agg(
            F.count("*").alias("rows_count"),
            F.avg("aqi").alias("avg_aqi"),
            F.avg("pm25").alias("avg_pm25"),
            F.avg("pm10").alias("avg_pm10"),
            F.sum(F.when(F.col("is_pollution_peak"), 1).otherwise(0)).alias("pollution_peak_count"),
        )
        .orderBy("site_id", "wind_speed_group")
    )


def build_quality_mart(joined):
    return joined.agg(
        F.count("*").alias("rows_count"),
        F.sum(F.when(F.col("weather_temperature").isNull(), 1).otherwise(0)).alias("missing_weather_rows"),
        F.sum(F.when(F.col("has_missing_pollution_value"), 1).otherwise(0)).alias("missing_pollution_rows"),
        F.countDistinct("site_id").alias("site_count"),
        F.min("timestamp").alias("first_timestamp"),
        F.max("timestamp").alias("last_timestamp"),
    )


def write_single_csv(df, output_path: str) -> None:
    path = Path(output_path)
    temp_dir = path.with_name(f".{path.stem}_spark_tmp")

    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    if path.exists():
        path.unlink()

    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(str(temp_dir))
    )

    part_files = sorted(temp_dir.glob("part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"Spark did not create part csv in {temp_dir}")

    shutil.move(str(part_files[0]), path)
    shutil.rmtree(temp_dir)


def build_marts(
    *,
    air_input: str,
    weather_input: str,
    joined_output: str,
    daily_output: str,
    wind_output: str,
    qa_output: str,
) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        air = prepare_air(read_csv(spark, air_input))
        weather = prepare_weather(read_csv(spark, weather_input))
        joined = build_joined_air_weather(air, weather)

        joined_count = joined.count()

        write_single_csv(joined, joined_output)
        write_single_csv(build_daily_mart(joined), daily_output)
        write_single_csv(build_wind_mart(joined), wind_output)
        write_single_csv(build_quality_mart(joined), qa_output)

        print(f"Joined air-weather rows: {joined_count}")
        print(f"Saved hourly mart CSV: {joined_output}")
        print(f"Saved daily mart CSV: {daily_output}")
        print(f"Saved wind mart CSV: {wind_output}")
        print(f"Saved quality mart CSV: {qa_output}")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--air-input", default=DEFAULT_AIR_INPUT)
    parser.add_argument("--weather-input", default=DEFAULT_WEATHER_INPUT)
    parser.add_argument("--joined-output", default=DEFAULT_JOINED_OUTPUT)
    parser.add_argument("--daily-output", default=DEFAULT_DAILY_OUTPUT)
    parser.add_argument("--wind-output", default=DEFAULT_WIND_OUTPUT)
    parser.add_argument("--qa-output", default=DEFAULT_QA_OUTPUT)
    args = parser.parse_args()

    build_marts(
        air_input=args.air_input,
        weather_input=args.weather_input,
        joined_output=args.joined_output,
        daily_output=args.daily_output,
        wind_output=args.wind_output,
        qa_output=args.qa_output,
    )


if __name__ == "__main__":
    main()
