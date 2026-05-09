"""
Kafka producer — sends FIX protocol messages to the 'fix-orders' topic.

Run after the broker is up:
    python producer.py

Key concepts demonstrated:
  - Explicit partition routing: all orders for the same instrument land on the
    same partition, preserving per-instrument chronological order (critical in trading)
  - Delivery callback: fires when the broker acks or rejects each message
  - flush(): blocks until every in-flight message is confirmed

Partition layout (3 partitions):
  Partition 0 — AAPL orders  (read by consumer 0)
  Partition 1 — MSFT orders  (read by consumer 0)
  Partition 2 — GOOGL orders (read by consumer 1)

FIX tag reference (FIX 4.4):
  8  = BeginString      (protocol version)
  35 = MsgType          D = New Order Single, F = Order Cancel Request
  49 = SenderCompID     (firm sending the message)
  56 = TargetCompID     (destination / exchange)
  11 = ClOrdID          (unique client order ID)
  41 = OrigClOrdID      (original ClOrdID being cancelled — cancel messages only)
  55 = Symbol           (instrument ticker)
  54 = Side             1 = Buy, 2 = Sell
  38 = OrderQty         (number of shares/contracts)
  44 = Price            (limit price — omitted for market orders)
  40 = OrdType          1 = Market, 2 = Limit
  60 = TransactTime     (ISO 8601 timestamp, added at send time)
"""

import json
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaException

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "fix-orders"

# Instrument → partition mapping.
# All orders for the same symbol route to the same partition so the consumer
# always sees them in the order they were produced (per-instrument ordering).
SYMBOL_PARTITION = {
    "AAPL":  0,
    "MSFT":  1,
    "GOOGL": 2,
}


def clordid():
    """Generate a short unique client order ID."""
    return str(uuid.uuid4())[:8].upper()


def on_delivery(err, msg):
    """Called once per message when the broker acknowledges (or rejects) it."""
    if err:
        print(f"  [FAILED]  key={msg.key()}  error={err}")
    else:
        print(
            f"  [OK]  partition={msg.partition()}  offset={msg.offset()}"
            f"  key={msg.key().decode() if msg.key() else None}"
        )


def new_order(symbol, side, qty, ord_type, price=None):
    """Build a FIX New Order Single (MsgType=D)."""
    msg = {
        "8":  "FIX.4.4",
        "35": "D",
        "49": "TRADER1",
        "56": "EXCHANGE",
        "11": clordid(),
        "55": symbol,
        "54": side,       # "1"=Buy, "2"=Sell
        "38": qty,
        "40": ord_type,   # "1"=Market, "2"=Limit
    }
    if price is not None:
        msg["44"] = price
    return msg


def cancel_order(symbol, orig_clordid):
    """Build a FIX Order Cancel Request (MsgType=F)."""
    return {
        "8":  "FIX.4.4",
        "35": "F",
        "49": "TRADER1",
        "56": "EXCHANGE",
        "11": clordid(),
        "41": orig_clordid,
        "55": symbol,
    }


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    # Build orders — ClOrdIDs are captured where needed for cancel references
    aapl_order_1 = new_order("AAPL", "1", 100, "2", 189.50)   # Buy 100 AAPL limit
    msft_order_1 = new_order("MSFT", "2", 200, "2", 420.00)   # Sell 200 MSFT limit
    aapl_order_2 = new_order("AAPL", "1",  50, "2", 190.25)   # Buy 50 AAPL limit
    googl_order_1 = new_order("GOOGL", "1", 75, "2", 178.50)  # Buy 75 GOOGL limit
    aapl_cancel  = cancel_order("AAPL", aapl_order_1["11"])    # Cancel first AAPL order
    msft_order_2 = new_order("MSFT", "2", 150, "1")            # Sell 150 MSFT market
    googl_order_2 = new_order("GOOGL", "1", 30, "2", 179.00)  # Buy 30 GOOGL limit
    aapl_order_3 = new_order("AAPL", "2",  80, "2", 191.00)   # Sell 80 AAPL limit
    msft_order_3 = new_order("MSFT", "1",  60, "2", 421.50)   # Buy 60 MSFT limit

    messages = [
        aapl_order_1,
        msft_order_1,
        aapl_order_2,
        googl_order_1,
        aapl_cancel,
        msft_order_2,
        googl_order_2,
        aapl_order_3,
        msft_order_3,
    ]

    print(f"Sending {len(messages)} FIX messages to '{TOPIC}' ...\n")
    print(f"  Partition 0 → AAPL  (consumer 0)")
    print(f"  Partition 1 → MSFT  (consumer 0)")
    print(f"  Partition 2 → GOOGL (consumer 1)\n")

    for fix_msg in messages:
        # Tag 60 (TransactTime) is stamped at the moment of sending
        fix_msg["60"] = datetime.now(timezone.utc).isoformat()

        symbol = fix_msg["55"]
        partition = SYMBOL_PARTITION[symbol]
        key = symbol.encode()
        value = json.dumps(fix_msg).encode()

        try:
            producer.produce(TOPIC, key=key, value=value, partition=partition, callback=on_delivery)
            producer.poll(0)
        except KafkaException as exc:
            print(f"Produce error: {exc}")

    remaining = producer.flush(timeout=30)
    if remaining:
        print(f"\nWarning: {remaining} message(s) were not delivered within the timeout.")
    else:
        print("\nAll messages delivered successfully.")


if __name__ == "__main__":
    main()
