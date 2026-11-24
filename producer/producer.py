"""Read ten original CSV files and publish one JSON message for every CSV row."""
import csv
import json
import os
from pathlib import Path
import re
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "lab5_sales")
DELAY = float(os.getenv("SEND_DELAY_SECONDS", "0.001"))


def order(path):
    match = re.search(r"\((\d+)\)", path.name)
    return int(match.group(1)) if match else 10


def main():
    files = sorted(DATA_DIR.glob("MOCK_DATA*.csv"), key=order)
    if len(files) != 10:
        raise RuntimeError(f"Expected 10 CSV files, found {len(files)} in {DATA_DIR}")

    producer = None
    for attempt in range(60):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BROKERS.split(","),
                key_serializer=lambda v: v.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
                acks="all", retries=10, max_in_flight_requests_per_connection=1,
            )
            producer.partitions_for(TOPIC)
            break
        except NoBrokersAvailable:
            print(f"Kafka unavailable, retry {attempt + 1}/60", flush=True)
            time.sleep(2)
    if producer is None:
        raise RuntimeError("Could not connect to Kafka")
    print(f"Connected to Kafka {BROKERS}; topic={TOPIC}", flush=True)
    total = 0
    expected_headers = None
    try:
        for file_index, path in enumerate(files, 1):
            with path.open("r", newline="", encoding="utf-8-sig") as stream:
                reader = csv.DictReader(stream)
                if expected_headers is None:
                    expected_headers = reader.fieldnames
                    if not expected_headers or len(expected_headers) != 50:
                        raise RuntimeError("Expected 50 CSV fields")
                if reader.fieldnames != expected_headers:
                    raise RuntimeError(f"Headers differ in {path.name}")
                count = 0
                for row_no, row in enumerate(reader, 1):
                    if None in row or row_no > 1000:
                        raise RuntimeError(f"Invalid CSV row in {path.name}: {row_no}")
                    event_id = f"file{file_index:02d}-row{row_no:04d}"
                    payload = {k: (v if v is not None else "") for k, v in row.items()}
                    payload.update(event_id=event_id, source_file=path.name, source_row=str(row_no))
                    producer.send(TOPIC, key=event_id, value=payload)
                    count += 1
                    total += 1
                    if total % 1000 == 0:
                        producer.flush()
                        print(f"Produced {total}/10000 JSON messages", flush=True)
                    if DELAY:
                        time.sleep(DELAY)
            if count != 1000:
                raise RuntimeError(f"Expected 1000 rows in {path.name}, found {count}")
            print(f"Sent {path.name}: {count} rows", flush=True)
        producer.flush()
        if total != 10000:
            raise RuntimeError(f"Expected 10000 messages, produced {total}")
        print("SUCCESS: produced 10000 JSON messages to Kafka", flush=True)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
