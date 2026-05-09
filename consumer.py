"""
Kafka consumer — reads FIX order messages from assigned partitions of 'fix-orders'.

Usage:
    python consumer.py --id 0   # reads partitions 0 and 1  (AAPL + MSFT orders)
    python consumer.py --id 1   # reads partition 2          (GOOGL orders)

Run both in separate terminals, then run producer.py to see FIX messages
arrive on the correct consumer.

Key concepts demonstrated:
  - assign() vs subscribe():
      subscribe() lets the broker automatically distribute partitions across
      all consumers in a group (automatic rebalancing).
      assign() manually pins this consumer to specific partitions — no
      rebalancing, no group coordinator involved. Useful when you need
      deterministic, stable routing.
  - TopicPartition: the object that names a topic + partition number.
  - Each consumer only ever sees messages from its assigned partitions,
    even though both consumers share the same group.id for offset tracking.
"""

import argparse
import json
import signal
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "fix-orders"
GROUP_ID = "learn-group"

# Partition assignment per consumer ID
PARTITION_MAP = {
    0: [0, 1],  # consumer 0 handles AAPL (p0) and MSFT (p1)
    1: [2],     # consumer 1 handles GOOGL (p2)
}

running = True


def shutdown(signum, frame):
    global running
    print("\nShutdown signal received, stopping consumer ...")
    running = False


def main():
    parser = argparse.ArgumentParser(description="Kafka partition-assigned consumer")
    parser.add_argument(
        "--id",
        type=int,
        choices=[0, 1],
        required=True,
        help="Consumer ID — 0: partitions 0+1, 1: partition 2",
    )
    args = parser.parse_args()
    consumer_id = args.id
    partitions = PARTITION_MAP[consumer_id]

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            # group.id is still used for offset commits even with assign()
            "group.id": GROUP_ID,
            # Read from the beginning of each partition on first run
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
        }
    )

    # assign() bypasses the group coordinator and directly pins this consumer
    # to the specified partitions. The broker will not reassign these partitions
    # to other consumers — ownership is entirely managed by your code.
    topic_partitions = [TopicPartition(TOPIC, p) for p in partitions]
    consumer.assign(topic_partitions)

    print(
        f"[consumer-{consumer_id}] assigned to '{TOPIC}' "
        f"partitions {partitions}. Waiting for messages ...\n"
    )

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
                    value = json.loads(msg.value())
                except (json.JSONDecodeError, UnicodeDecodeError):
                    value = msg.value()

                print(
                    f"[consumer-{consumer_id}]  partition={msg.partition()}  "
                    f"offset={msg.offset()}  key={key}"
                )
                print(f"  payload: {json.dumps(value, indent=4)}\n")

    except KafkaException as exc:
        print(f"Kafka error: {exc}", file=sys.stderr)
    finally:
        consumer.close()
        print(f"Consumer {consumer_id} closed.")


if __name__ == "__main__":
    main()
