# learn-kafka

A Python project for learning Apache Kafka hands-on through a simulated FIX protocol
trading system. It runs a single-node Kafka broker in Docker and demonstrates core
Kafka concepts through a working producer and two partition-assigned consumers.

---

## What this project covers

| Concept | Where you see it |
|---------|-----------------|
| Topics and partitions | `fix-orders` is created with 3 partitions |
| FIX protocol messages | Producer sends New Order Single (D) and Order Cancel Request (F) |
| Explicit partition routing | `partition=` in `produce()` pins each instrument to a fixed partition |
| Delivery acknowledgement | Delivery callback fires when the broker acks each message |
| `flush()` | Blocks until all in-flight messages are confirmed |
| Manual partition assignment | `assign()` + `TopicPartition` pins each consumer to fixed partitions |
| Consumer groups and offset tracking | Both consumers share `group.id = learn-group` |
| `auto.offset.reset=earliest` | Consumers always start from the beginning on first run |

---

## Project structure

```
learn-kafka/
├── docker-compose.yml   # Single-node KRaft Kafka broker (apache/kafka:4.1.2)
├── requirements.txt     # confluent-kafka==2.14.0
├── producer.py          # Sends 9 FIX messages (8 New Order Single + 1 Cancel) across 3 partitions
├── consumer.py          # Reads from assigned partitions (--id 0 or --id 1)
├── README.md            # This file
├── PROJECT_STATE.md     # Detailed technical reference for all files
└── .gitignore
```

---

## How the pieces fit together

```
producer.py
  AAPL orders  ──► partition 0 ──► consumer 0  (--id 0)
  MSFT orders  ──► partition 1 ──► consumer 0  (--id 0)
  GOOGL orders ──► partition 2 ──► consumer 1  (--id 1)
                        │
                  Kafka broker
                 (Docker, port 9092)
               topic: fix-orders, 3 partitions
```

---

## FIX message format

Messages are JSON-encoded FIX 4.4 dicts using standard numeric tag keys:

```json
{
  "8":  "FIX.4.4",
  "35": "D",
  "49": "TRADER1",
  "56": "EXCHANGE",
  "11": "A3F2B1C0",
  "55": "AAPL",
  "54": "1",
  "38": 100,
  "40": "2",
  "44": 189.50,
  "60": "2026-05-09T04:19:33.380093+00:00"
}
```

| Tag | Field | Values |
|-----|-------|--------|
| 8 | BeginString | FIX.4.4 |
| 35 | MsgType | `D` = New Order Single, `F` = Order Cancel Request |
| 49 | SenderCompID | TRADER1 |
| 56 | TargetCompID | EXCHANGE |
| 11 | ClOrdID | unique 8-char order ID |
| 41 | OrigClOrdID | original order ID (cancel messages only) |
| 55 | Symbol | AAPL / MSFT / GOOGL |
| 54 | Side | `1` = Buy, `2` = Sell |
| 38 | OrderQty | shares |
| 44 | Price | limit price (absent on market orders) |
| 40 | OrdType | `1` = Market, `2` = Limit |
| 60 | TransactTime | ISO 8601 timestamp stamped at send time |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- Python 3.8 or newer

---

## Setup

**1. Create and activate a virtual environment**

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

**2. Install the Kafka client**

```powershell
pip install -r requirements.txt
```

**3. Start the Kafka broker**

```powershell
docker compose up -d
```

Wait about 10 seconds for the broker to be ready. You can verify with:

```powershell
docker compose logs broker
```

---

## Running the demo

Open three terminals, all with the virtual environment activated.

**Terminal A — Consumer 0** (reads partitions 0 and 1 — AAPL + MSFT orders)

```powershell
python consumer.py --id 0
```

**Terminal B — Consumer 1** (reads partition 2 — GOOGL orders)

```powershell
python consumer.py --id 1
```

**Terminal C — Producer** (sends 9 FIX messages across all 3 partitions)

```powershell
python producer.py
```

### Expected output

Producer:
```
Sending 9 FIX messages to 'fix-orders' ...

  Partition 0 → AAPL  (consumer 0)
  Partition 1 → MSFT  (consumer 0)
  Partition 2 → GOOGL (consumer 1)

  [OK]  partition=0  offset=0  key=AAPL
  [OK]  partition=1  offset=0  key=MSFT
  [OK]  partition=0  offset=1  key=AAPL
  [OK]  partition=2  offset=0  key=GOOGL
  [OK]  partition=0  offset=2  key=AAPL
  [OK]  partition=1  offset=1  key=MSFT
  [OK]  partition=2  offset=1  key=GOOGL
  [OK]  partition=0  offset=3  key=AAPL
  [OK]  partition=1  offset=2  key=MSFT

All messages delivered successfully.
```

Consumer 0 (receives AAPL and MSFT — partitions 0 and 1):
```
[consumer-0] assigned to 'fix-orders' partitions [0, 1]. Waiting for messages ...

[consumer-0]  partition=0  offset=0  key=AAPL
  payload: { "8": "FIX.4.4", "35": "D", "55": "AAPL", "54": "1", "38": 100, ... }

[consumer-0]  partition=1  offset=0  key=MSFT
  payload: { "8": "FIX.4.4", "35": "D", "55": "MSFT", "54": "2", "38": 200, ... }
...
```

Consumer 1 (receives GOOGL — partition 2 only):
```
[consumer-1] assigned to 'fix-orders' partitions [2]. Waiting for messages ...

[consumer-1]  partition=2  offset=0  key=GOOGL
  payload: { "8": "FIX.4.4", "35": "D", "55": "GOOGL", "54": "1", "38": 75, ... }
...
```

---

## Messages sent (9 total)

| # | MsgType | Symbol | Side | Qty | OrdType | Price | Partition |
|---|---------|--------|------|-----|---------|-------|-----------|
| 1 | D | AAPL | Buy | 100 | Limit | 189.50 | 0 |
| 2 | D | MSFT | Sell | 200 | Limit | 420.00 | 1 |
| 3 | D | AAPL | Buy | 50 | Limit | 190.25 | 0 |
| 4 | D | GOOGL | Buy | 75 | Limit | 178.50 | 2 |
| 5 | F | AAPL | — | — | Cancel | — | 0 (cancels msg 1) |
| 6 | D | MSFT | Sell | 150 | Market | — | 1 |
| 7 | D | GOOGL | Buy | 30 | Limit | 179.00 | 2 |
| 8 | D | AAPL | Sell | 80 | Limit | 191.00 | 0 |
| 9 | D | MSFT | Buy | 60 | Limit | 421.50 | 1 |

---

## Things to try next

**Re-read all messages from offset 0**
Change `GROUP_ID` in `consumer.py` to a new string (e.g. `learn-group-2`) and restart.
Kafka keeps messages until the retention period expires, so old messages are always
available to a new consumer group.

**Observe per-instrument ordering**
Run `producer.py` several times. Each instrument's offsets increment independently
and always land on the same partition and consumer, preserving chronological order
per symbol — the same guarantee a real trading system relies on.

**Fan-out (pub/sub)**
Run two instances of `consumer.py --id 0` with *different* `GROUP_ID` values.
Both will receive every AAPL and MSFT message independently — same as two separate
downstream systems consuming the same order flow.

**Automatic rebalancing**
Change `assign()` to `subscribe([TOPIC])` in `consumer.py` and start two instances
with the same `group.id`. Kafka will automatically split the 3 partitions between
them and rebalance when either consumer stops.

**Add Execution Reports**
Extend `producer.py` with an `exec_report()` helper using MsgType=8 and tags
17 (ExecID), 39 (OrdStatus), 150 (ExecType), 14 (CumQty), 151 (LeavesQty) to
simulate the exchange acknowledging or filling an order.

---

## Stopping everything

```powershell
# Stop consumers with Ctrl+C in each terminal, then:
docker compose down
```

`docker compose down` removes the container and its ephemeral storage, so the next
`docker compose up -d` starts with a clean broker and an empty `fix-orders` topic.
