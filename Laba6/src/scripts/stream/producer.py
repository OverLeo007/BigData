from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

SRC_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = SRC_ROOT / "data" / "raw" / "air" / "air_measurements.csv"
DEFAULT_SITES = SRC_ROOT / "data" / "reference" / "air_sites.csv"

# Сырые поля, которые шлём в событие (без шумного _raw). Значения остаются «как в CSV» —
# приведение типов и валидацию делает consumer.
EVENT_FIELDS = [
    "aqi",
    "iaqi",
    "pm25",
    "pm10",
    "pm25_mcp",
    "temperature",
    "humidity",
    "pressure",
]


def load_site_names(sites_csv: Path) -> dict[str, str]:
    """site_id -> site_name из справочника постов (если файл есть)."""
    names: dict[str, str] = {}
    if not sites_csv.exists():
        return names
    with sites_csv.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            site_id = (row.get("site_id") or "").strip()
            if site_id:
                names[site_id] = (row.get("site_name") or "").strip()
    return names


def build_event(row: dict[str, str], site_names: dict[str, str]) -> dict:
    site_id = (row.get("site_id") or "").strip()
    event = {
        "site_id": int(site_id) if site_id.isdigit() else None,
        "site_name": site_names.get(site_id) or None,
        # время измерения = event-time для оконной агрегации в consumer
        "event_time": (row.get("time") or "").strip() or None,
        # момент отправки в поток — для наглядности и возможной обработки по processing-time
        "ingest_time": datetime.now(timezone.utc).isoformat(),
    }
    for field in EVENT_FIELDS:
        value = (row.get(field) or "").strip()
        event[field] = value or None
    return event


def iter_rows(input_csv: Path, loop: bool):
    """Построчно читает CSV; при --loop крутит файл по кругу (бесконечный поток)."""
    while True:
        with input_csv.open(encoding="utf-8", newline="") as fh:
            yield from csv.DictReader(fh)
        if not loop:
            return


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

    parser = argparse.ArgumentParser(description="Stream raw air measurements into Kafka (Laba6).")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Kafka bootstrap (хост → localhost:9092; внутри контейнера → kafka:9092)")
    parser.add_argument("--topic", default="air-events")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--sites", type=Path, default=DEFAULT_SITES)
    parser.add_argument("--rate", type=float, default=20.0,
                        help="событий в секунду (0 = без задержки)")
    parser.add_argument("--limit", type=int, default=None, help="максимум событий (для демо)")
    parser.add_argument("--loop", action="store_true", help="бесконечно повторять файл")
    args = parser.parse_args()

    if not args.input.exists():
        sys.exit(f"Нет входного файла {args.input} — сначала прогоните pipeline (стадия download).")

    site_names = load_site_names(args.sites)
    print(f"Постов в справочнике: {len(site_names)}; источник: {args.input}")

    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
            acks="all",
            linger_ms=50,
        )
    except NoBrokersAvailable:
        sys.exit(f"Брокер недоступен на {args.bootstrap_servers}. Поднят ли laba6-kafka?")

    delay = 1.0 / args.rate if args.rate and args.rate > 0 else 0.0
    sent = 0
    try:
        for row in iter_rows(args.input, args.loop):
            event = build_event(row, site_names)
            key = str(event["site_id"]) if event["site_id"] is not None else None
            producer.send(args.topic, key=key, value=event)
            sent += 1
            if sent % 200 == 0:
                print(f"  отправлено {sent} событий → {args.topic}")
            if args.limit and sent >= args.limit:
                break
            if delay:
                time.sleep(delay)
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
    finally:
        producer.flush()
        producer.close()
        print(f"Готово. Всего отправлено: {sent} событий в топик '{args.topic}'.")


if __name__ == "__main__":
    main()
