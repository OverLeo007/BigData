from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


DEFAULT_INPUT = "/opt/krasair/data/raw/nmu/nmu_announcements.csv"
DEFAULT_OUTPUT = "/opt/krasair/data/staging/nmu_hourly.csv"


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("KrasAirNmuCleaning").getOrCreate()


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


def clean_nmu_data(input_path: str, output_path: str) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw = (
            spark.read.option("header", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", "true")
            .csv(input_path)
        )

        announcements = (
            raw.withColumn("period_start", F.to_timestamp("period_start"))
            .withColumn("period_end", F.to_timestamp("period_end"))
            .withColumn("regime_level", F.col("regime_level").cast("integer"))
            .where(F.col("period_start").isNotNull() & F.col("period_end").isNotNull())
            .where(F.col("period_end") > F.col("period_start"))
        )

        # Expand each [period_start, period_end) period into hourly timestamps.
        expanded = (
            announcements.withColumn(
                "timestamp",
                F.explode(F.expr("sequence(period_start, period_end, interval 1 hour)")),
            )
            .where(F.col("timestamp") < F.col("period_end"))
            .select(
                "timestamp",
                "regime",
                "regime_level",
                "announcement_text",
            )
        )

        # When periods overlap on the same hour, keep the most severe regime.
        ranked = Window.partitionBy("timestamp").orderBy(F.col("regime_level").desc())
        hourly = (
            expanded.withColumn("rank", F.row_number().over(ranked))
            .where(F.col("rank") == 1)
            .drop("rank")
            .withColumn("date", F.to_date("timestamp"))
            .withColumn("hour", F.hour("timestamp"))
            .withColumn("is_nmu", F.lit(True))
            .select(
                "timestamp",
                "date",
                "hour",
                "is_nmu",
                "regime",
                "regime_level",
                "announcement_text",
            )
            .orderBy("timestamp")
        )

        hourly_count = hourly.count()
        write_single_csv(hourly, output_path)

        print(f"NMU announcements: {announcements.count()}")
        print(f"NMU hourly rows: {hourly_count}")
        print(f"Saved CSV: {output_path}")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    clean_nmu_data(args.input, args.output)


if __name__ == "__main__":
    main()
