"""
Elasticsearch sink — reads every FIX message from 'fix-orders' and indexes it
into Elasticsearch so orders can be searched and visualised in Kibana.

Run after the broker AND Elasticsearch are up:
    python es_sink.py

Press Ctrl+C to stop.

Key concepts demonstrated:
  - Fan-out: this consumer uses group.id='es-sink-group', completely independent
    of the existing 'learn-group'. Both groups receive every message; Kafka tracks
    their offsets separately. producer.py and consumer.py need no changes.
  - subscribe() vs assign(): here we use subscribe() so Kafka automatically
    assigns all 3 partitions to this single-instance sink. No manual pinning needed.
  - Idempotent indexing: the FIX ClOrdID (tag 11) is used as the Elasticsearch
    document _id. Re-running producer.py overwrites the same documents rather than
    creating duplicates.
  - Kafka metadata enrichment: each document is stored with _kafka_topic,
    _kafka_partition, and _kafka_offset so you can trace any ES doc back to its
    exact position in the Kafka log.

After running, query indexed orders:
    curl http://localhost:9200/fix-orders/_search?pretty

Or open Kibana at http://localhost:5601
"""

import json
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException
from elasticsearch import Elasticsearch, ConnectionError as ESConnectionError

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "fix-orders"
GROUP_ID = "es-sink-group"   # separate group — does not affect learn-group consumers

ES_HOST = "http://localhost:9200"
ES_INDEX = "fix-orders"

running = True


def shutdown(signum, frame):
    global running
    print("\nShutdown signal received, stopping ES sink ...")
    running = False


def wait_for_elasticsearch(es: Elasticsearch, retries: int = 15, delay: int = 3) -> None:
    """Block until Elasticsearch is reachable, or raise after retries are exhausted."""
    print(f"Waiting for Elasticsearch at {ES_HOST} ...")
    for attempt in range(1, retries + 1):
        try:
            if es.ping():
                print(f"Elasticsearch is ready.\n")
                return
        except ESConnectionError:
            pass
        print(f"  attempt {attempt}/{retries} — not ready yet, retrying in {delay}s ...")
        time.sleep(delay)
    raise RuntimeError(f"Elasticsearch did not become available after {retries} attempts.")


def main():
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    es = Elasticsearch(ES_HOST)
    wait_for_elasticsearch(es)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            # Independent consumer group — offsets tracked separately from learn-group
            "group.id": GROUP_ID,
            # Start from the beginning so we pick up messages sent before this sink started
            "auto.offset.reset": "earliest",
            # Commit offsets automatically every 5 s
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
        }
    )

    # subscribe() lets Kafka assign all partitions automatically.
    # Because there is only one instance of this sink, it will receive all 3 partitions.
    consumer.subscribe([TOPIC])
    print(f"Subscribed to '{TOPIC}' as group '{GROUP_ID}'.")
    print(f"Indexing into Elasticsearch index '{ES_INDEX}' ...\n")

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                code = msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    print(
                        f"  [EOF]  partition={msg.partition()} @ offset={msg.offset()}"
                    )
                elif code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print("  [waiting] topic not found yet — run producer.py to create it ...")
                else:
                    raise KafkaException(msg.error())
            else:
                key = msg.key().decode() if msg.key() else None

                try:
                    fix_msg = json.loads(msg.value())
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    print(f"  [SKIP] could not parse message: {exc}")
                    continue

                # Enrich the FIX payload with Kafka coordinates before indexing
                document = {
                    **fix_msg,
                    "_kafka_topic":     msg.topic(),
                    "_kafka_partition": msg.partition(),
                    "_kafka_offset":    msg.offset(),
                }

                # Use ClOrdID (tag 11) as the document ID for idempotent indexing.
                # If the producer is re-run, existing documents are overwritten
                # rather than duplicated.
                doc_id = fix_msg.get("11")

                result = es.index(index=ES_INDEX, id=doc_id, document=document)

                msg_type = fix_msg.get("35", "?")
                symbol   = fix_msg.get("55", "?")
                action   = "CANCEL" if msg_type == "F" else "ORDER"

                print(
                    f"  [INDEXED]  {action}  symbol={symbol}"
                    f"  id={doc_id}  partition={msg.partition()}"
                    f"  offset={msg.offset()}  result={result['result']}"
                )

    except KafkaException as exc:
        print(f"Kafka error: {exc}", file=sys.stderr)
    finally:
        consumer.close()
        es.close()
        print("ES sink closed.")


if __name__ == "__main__":
    main()
