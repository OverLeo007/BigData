from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Поля события (как их шлёт producer); числовые приходят строками — типизируем здесь.
EVENT_SCHEMA = StructType([
    StructField("site_id", IntegerType()),
    StructField("site_name", StringType()),
    StructField("event_time", StringType()),
    StructField("ingest_time", StringType()),
    StructField("aqi", StringType()),
    StructField("iaqi", StringType()),
    StructField("pm25", StringType()),
    StructField("pm10", StringType()),
    StructField("pm25_mcp", StringType()),
    StructField("temperature", StringType()),
    StructField("humidity", StringType()),
    StructField("pressure", StringType()),
])

NUMERIC_FIELDS = ["aqi", "iaqi", "pm25", "pm10", "pm25_mcp", "temperature", "humidity", "pressure"]

# Валидные диапазоны (как в clean_air_data.py) — для фильтрации.
AQI_MIN, AQI_MAX = 0, 1000


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("KrasAirStreaming").getOrCreate()


def transform(raw):
    """Шаг 1 — преобразование: JSON → типизированные поля + производный признак."""
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
    )
    for field in NUMERIC_FIELDS:
        parsed = parsed.withColumn(
            field, F.regexp_replace(F.col(field), ",", ".").cast("double")
        )
    return parsed.withColumn(
        "is_pollution_peak",
        F.coalesce(F.col("aqi") > 100, F.lit(False))
        | F.coalesce(F.col("pm25") > 35, F.lit(False))
        | F.coalesce(F.col("pm10") > 150, F.lit(False)),
    )


def filter_valid(df):
    """Шаг 2 — фильтрация: только валидные события (есть время/пост, AQI в диапазоне)."""
    return df.where(
        F.col("event_time").isNotNull()
        & F.col("site_id").isNotNull()
        & F.col("aqi").isNotNull()
        & (F.col("aqi") >= AQI_MIN)
        & (F.col("aqi") <= AQI_MAX)
    )


def aggregate(df, window_duration: str, watermark: str):
    """Шаг 3 — оконное агрегирование по событийному времени и посту."""
    return (
        df.withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window_duration), "site_id", "site_name")
        .agg(
            F.round(F.avg("aqi"), 1).alias("avg_aqi"),
            F.max("aqi").alias("max_aqi"),
            F.round(F.avg("pm25"), 1).alias("avg_pm25"),
            F.sum(F.col("is_pollution_peak").cast("int")).alias("peak_count"),
            F.count(F.lit(1)).alias("sample_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "site_id", "site_name",
            "avg_aqi", "max_aqi", "avg_pm25", "peak_count", "sample_count",
        )
    )


def make_batch_writer(args):
    """foreachBatch: один снимок состояния → console + Postgres + выходной топик."""
    jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"

    def write_batch(batch_df, batch_id: int) -> None:
        batch_df.persist()
        count = batch_df.count()
        print(f"=== batch {batch_id}: {count} оконных агрегатов ===", flush=True)
        batch_df.orderBy("window_start", "site_id").show(30, truncate=False)

        # Postgres — снимок состояния (overwrite + truncate, чтобы не плодить дубли окон).
        (
            batch_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", args.pg_table)
            .option("user", args.pg_user)
            .option("password", args.pg_password)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )

        # Выходной топик с агрегатами.
        (
            batch_df.selectExpr(
                "CAST(site_id AS STRING) AS key",
                "to_json(struct(*)) AS value",
            )
            .write.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("topic", args.out_topic)
            .save()
        )
        batch_df.unpersist()

    return write_batch


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming over Kafka (Laba6).")
    parser.add_argument("--bootstrap-servers", default="kafka:9092",
                        help="внутри контейнера → kafka:9092")
    parser.add_argument("--topic", default="air-events")
    parser.add_argument("--out-topic", default="air-aggregates")
    parser.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    parser.add_argument("--window", default="1 hour")
    parser.add_argument("--watermark", default="2 hours")
    parser.add_argument("--available-now", action="store_true",
                        help="обработать всё доступное и завершиться (пакетный режим для пайплайна/теста)")
    parser.add_argument("--trigger-interval", default="10 seconds",
                        help="интервал триггера для непрерывного режима")
    parser.add_argument("--checkpoint", default="/opt/krasair/data/stream/checkpoint")
    parser.add_argument("--pg-host", default="postgres")
    parser.add_argument("--pg-port", default="5432")
    parser.add_argument("--pg-db", default="laba6")
    parser.add_argument("--pg-user", default="laba6")
    parser.add_argument("--pg-password", default="laba6")
    parser.add_argument("--pg-table", default="mart_stream_aggregates")
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    aggregated = aggregate(filter_valid(transform(raw)), args.window, args.watermark)

    writer = (
        aggregated.writeStream
        .outputMode("complete")  # снимок всех окон на каждый триггер
        .foreachBatch(make_batch_writer(args))
        .option("checkpointLocation", args.checkpoint)
    )

    if args.available_now:
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(processingTime=args.trigger_interval).start()

    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
