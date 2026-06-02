from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_INPUT = "/opt/krasair/data/raw/weather/weather_hourly.csv"
DEFAULT_OUTPUT = "/opt/krasair/data/staging/weather_cleaned.csv"

NUMERIC_LIMITS = {
    "weather_temperature": (-60, 60),
    "weather_humidity": (0, 100),
    "precipitation": (0, 500),
    "rain": (0, 500),
    "snowfall": (0, 500),
    "surface_pressure": (650, 1100),
    "wind_speed": (0, 80),
    "wind_direction": (0, 360),
}


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("KrasWeatherCleaning").getOrCreate()


def cast_double(df, source_column: str, target_column: str):
    return df.withColumn(
        target_column,
        F.regexp_replace(F.col(source_column).cast("string"), ",", ".").cast("double"),
    )


def cast_columns(df):
    return (
        df.withColumn("timestamp", F.to_timestamp(F.col("time"), "yyyy-MM-dd'T'HH:mm"))
        .transform(lambda frame: cast_double(frame, "temperature_2m", "weather_temperature"))
        .transform(lambda frame: cast_double(frame, "relative_humidity_2m", "weather_humidity"))
        .transform(lambda frame: cast_double(frame, "precipitation", "precipitation"))
        .transform(lambda frame: cast_double(frame, "rain", "rain"))
        .transform(lambda frame: cast_double(frame, "snowfall", "snowfall"))
        .transform(lambda frame: cast_double(frame, "surface_pressure", "surface_pressure"))
        .transform(lambda frame: cast_double(frame, "wind_speed_10m", "wind_speed"))
        .transform(lambda frame: cast_double(frame, "wind_direction_10m", "wind_direction"))
    )


def clean_values(df):
    result = df
    for column, (lower_bound, upper_bound) in NUMERIC_LIMITS.items():
        result = result.withColumn(
            column,
            F.when(
                (F.col(column) >= lower_bound) & (F.col(column) <= upper_bound),
                F.col(column),
            ),
        )
    return result


def add_features(df):
    return (
        df.withColumn("date", F.to_date("timestamp"))
        .withColumn("hour", F.hour("timestamp"))
        .withColumn(
            "wind_speed_group",
            F.when(F.col("wind_speed").isNull(), F.lit("unknown"))
            .when(F.col("wind_speed") < 2, F.lit("calm"))
            .when(F.col("wind_speed") < 5, F.lit("moderate"))
            .otherwise(F.lit("strong")),
        )
        .withColumn("has_precipitation", F.coalesce(F.col("precipitation") > 0, F.lit(False)))
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


def clean_weather_data(input_path: str, output_path: str) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw = (
            spark.read.option("header", "true")
            .option("quote", '"')
            .option("escape", '"')
            .csv(input_path)
        )

        raw_count = raw.count()

        cleaned = add_features(
            clean_values(cast_columns(raw))
            .where(F.col("timestamp").isNotNull())
            .dropDuplicates(["timestamp"])
        )

        final_columns = [
            "timestamp",
            "date",
            "hour",
            "weather_temperature",
            "weather_humidity",
            "precipitation",
            "rain",
            "snowfall",
            "surface_pressure",
            "wind_speed",
            "wind_direction",
            "wind_speed_group",
            "has_precipitation",
        ]
        cleaned = cleaned.select(*final_columns)
        cleaned_count = cleaned.count()

        write_single_csv(cleaned, output_path)

        print(f"Raw rows: {raw_count}")
        print(f"Cleaned rows: {cleaned_count}")
        print(f"Removed rows: {raw_count - cleaned_count}")
        print(f"Saved CSV: {output_path}")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    clean_weather_data(args.input, args.output)


if __name__ == "__main__":
    main()
