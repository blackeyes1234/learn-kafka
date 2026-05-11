# learn-kafka

A Python project for learning Apache Kafka hands-on through a simulated FIX protocol
trading system. It runs a single-node Kafka broker, Elasticsearch, and Kibana in Docker
and demonstrates core Kafka concepts through a working producer, two partition-assigned
consumers, and an Elasticsearch sink.

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
| Consumer groups and offset tracking | `learn-group` (consumers) and `es-sink-group` (ES sink) are independent |
| `auto.offset.reset=earliest` | Consumers always start from the beginning on first run |
| Fan-out | `es_sink.py` uses a separate group — same messages delivered to both groups |
| Elasticsearch sink | Every FIX message is indexed into ES; Kibana provides a visual UI |

---

## Project structure

```
learn-kafka/
├── docker-compose.yml   # Kafka broker + Elasticsearch + Kibana
├── requirements.txt     # confluent-kafka==2.14.0, elasticsearch==8.19.3
├── producer.py          # Sends 9 FIX messages (8 New Order Single + 1 Cancel)
├── consumer.py          # Partition-assigned consumer (--id 0 or --id 1)
├── es_sink.py           # Kafka consumer that indexes every message into Elasticsearch
├── README.md            # This file
├── PROJECT_STATE.md     # Detailed technical reference for all files
└── .gitignore
```

---

## How the pieces fit together

```
producer.py
  AAPL orders  ──► partition 0 ──► consumer.py --id 0  (learn-group)
  MSFT orders  ──► partition 1 ──► consumer.py --id 0  (learn-group)
  GOOGL orders ──► partition 2 ──► consumer.py --id 1  (learn-group)
                        │
                        ├──► es_sink.py  (es-sink-group)
                        │         │
                        │         ▼
                        │    Elasticsearch :9200
                        │         │
                        │         ▼
                        │      Kibana :5601
                        │
                  Kafka broker (Docker, port 9092)
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
| 11 | ClOrdID | unique 8-char order ID (used as Elasticsearch document ID) |
| 41 | OrigClOrdID | original order ID (cancel messages only) |
| 55 | Symbol | AAPL / MSFT / GOOGL |
| 54 | Side | `1` = Buy, `2` = Sell |
| 38 | OrderQty | shares |
| 44 | Price | limit price (absent on market orders) |
| 40 | OrdType | `1` = Market, `2` = Limit |
| 60 | TransactTime | ISO 8601 timestamp stamped at send time (Kibana timestamp field) |

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

**2. Install dependencies**

```powershell
pip install -r requirements.txt
```

**3. Start all services**

```powershell
docker compose up -d
```

Wait about 20–30 seconds for Elasticsearch and Kibana to be ready (Kafka is faster at ~10s).
You can check status with:

```powershell
docker compose logs elasticsearch
docker compose logs kibana
```

---

## Running the demo

Open four terminals, all with the virtual environment activated.

**Terminal A — Consumer 0** (reads partitions 0 and 1 — AAPL + MSFT orders)

```powershell
python consumer.py --id 0
```

**Terminal B — Consumer 1** (reads partition 2 — GOOGL orders)

```powershell
python consumer.py --id 1
```

**Terminal C — Elasticsearch sink** (reads all partitions, indexes every message into ES)

```powershell
python es_sink.py
```

**Terminal D — Producer** (sends 9 FIX messages across all 3 partitions)

```powershell
python producer.py
```

### Expected output — ES sink

```
Waiting for Elasticsearch at http://localhost:9200 ...
Elasticsearch is ready.

Subscribed to 'fix-orders' as group 'es-sink-group'.
Indexing into Elasticsearch index 'fix-orders' ...

  [INDEXED]  ORDER  symbol=AAPL  id=A3F2B1C0  partition=0  offset=0  result=created
  [INDEXED]  ORDER  symbol=MSFT  id=D7E4C2A1  partition=1  offset=0  result=created
  [INDEXED]  ORDER  symbol=AAPL  id=F1B9E3D2  partition=0  offset=1  result=created
  [INDEXED]  ORDER  symbol=GOOGL id=C5A8F4B3  partition=2  offset=0  result=created
  [INDEXED]  CANCEL symbol=AAPL  id=E2D6C7A4  partition=0  offset=2  result=created
  ...
```

---

## Viewing orders in Kibana

1. Open **http://localhost:5601**
2. Dismiss the security warning (expected — security is disabled for local dev)
3. Click **Explore on my own**
4. Hamburger menu → **Discover** → **Create data view**
5. Index pattern: `fix-orders`, Name: `fix-orders`, Timestamp: `60`
6. Click **Save data view to Kibana**

All 9 indexed FIX orders appear in the Discover view. You can filter by field (e.g. `55: AAPL` to see only Apple orders) or use the search bar.

Query directly via the REST API:

```powershell
curl http://localhost:9200/fix-orders/_search?pretty
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
Change `GROUP_ID` in `consumer.py` or `es_sink.py` to a new string and restart.
Kafka keeps messages until the retention period expires, so old messages are always
available to a new consumer group.

**Observe per-instrument ordering**
Run `producer.py` several times. Each instrument's offsets increment independently
and always land on the same partition and consumer, preserving chronological order
per symbol — the same guarantee a real trading system relies on.

**Fan-out (pub/sub)**
Run two instances of `consumer.py --id 0` with *different* `GROUP_ID` values.
Both receive every AAPL and MSFT message independently — same as two separate
downstream systems consuming the same order flow. `es_sink.py` already demonstrates
this pattern with `es-sink-group`.

**Automatic rebalancing**
Change `assign()` to `subscribe([TOPIC])` in `consumer.py` and start two instances
with the same `group.id`. Kafka will automatically split the 3 partitions between
them and rebalance when either consumer stops.

**Persist Elasticsearch data across restarts**
Add a named volume to the `elasticsearch` service in `docker-compose.yml` so indexed
orders survive `docker compose down`:
```yaml
    volumes:
      - esdata:/usr/share/elasticsearch/data
volumes:
  esdata:
```

**Add Execution Reports**
Extend `producer.py` with an `exec_report()` helper using MsgType=8 and tags
17 (ExecID), 39 (OrdStatus), 150 (ExecType), 14 (CumQty), 151 (LeavesQty) to
simulate the exchange acknowledging or filling an order.

---

## Stopping everything

```powershell
# Stop all Python scripts with Ctrl+C in each terminal, then:
docker compose down
```

`docker compose down` removes all containers and their ephemeral storage. The next
`docker compose up -d` starts with a clean broker, an empty `fix-orders` topic,
and an empty Elasticsearch index.
