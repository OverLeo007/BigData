from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_INPUT = "/opt/krasair/data/raw/air/air_measurements.csv"
DEFAULT_OUTPUT = "/opt/krasair/data/staging/air_cleaned.csv"

NUMERIC_LIMITS = {
    "aqi": (0, 1000),
    "iaqi": (0, 1000),
    "pm25": (0, 2000),
    "pm10": (0, 3000),
    "pm25_mcp": (0, 2000),
    "temperature": (-60, 60),
    "humidity": (0, 100),
    "pressure": (650, 820),
}

NUMERIC_COLUMNS = ["site_id", *NUMERIC_LIMITS.keys()]


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("KrasAirCleaning").getOrCreate()


def cast_columns(df):
    result = df.withColumn("timestamp", F.to_timestamp(F.col("time"), "yyyy-MM-dd HH:mm:ss"))

    for column in NUMERIC_COLUMNS:
        result = result.withColumn(
            column,
            F.regexp_replace(F.col(column).cast("string"), ",", ".").cast("double"),
        )

    return result.withColumn("site_id", F.col("site_id").cast("integer"))


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
        .withColumn("month", F.month("timestamp"))
        .withColumn(
            "is_pollution_peak",
            F.coalesce(F.col("aqi") > 100, F.lit(False))
            | F.coalesce(F.col("pm25") > 35, F.lit(False))
            | F.coalesce(F.col("pm10") > 150, F.lit(False)),
        )
        .withColumn(
            "has_missing_pollution_value",
            F.col("aqi").isNull() | F.col("pm25").isNull() | F.col("pm10").isNull(),
        )
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


def clean_air_data(input_path: str, output_path: str) -> None:
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
            .where(F.col("site_id").isNotNull())
            .where(F.col("timestamp").isNotNull())
            .dropDuplicates(["site_id", "timestamp"])
        )

        final_columns = [
            "site_id",
            "timestamp",
            "date",
            "hour",
            "month",
            "aqi",
            "iaqi",
            "pm25",
            "pm10",
            "pm25_mcp",
            "temperature",
            "humidity",
            "pressure",
            "is_pollution_peak",
            "has_missing_pollution_value",
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

    clean_air_data(args.input, args.output)


if __name__ == "__main__":
    main()
