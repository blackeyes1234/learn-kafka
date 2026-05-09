# learn-kafka

A minimal Python project for learning Apache Kafka hands-on. It runs a single-node
Kafka broker in Docker and demonstrates core concepts through a working producer and
two partition-assigned consumers.

---

## What this project covers

| Concept | Where you see it |
|---------|-----------------|
| Topics and partitions | `learn-topic` is created with 3 partitions |
| Producing messages | `producer.py` sends JSON events to specific partitions |
| Explicit partition routing | `partition=` in `produce()` overrides key-based hashing |
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
├── producer.py          # Sends 8 events across 3 partitions
├── consumer.py          # Reads from assigned partitions (--id 0 or --id 1)
└── .gitignore
```

---

## How the pieces fit together

```
producer.py
  alice  events  ──► partition 0 ──► consumer 0  (--id 0)
  bob    events  ──► partition 1 ──► consumer 0  (--id 0)
  charlie events ──► partition 2 ──► consumer 1  (--id 1)
                         │
                   Kafka broker
                  (Docker, port 9092)
```

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

Wait about 10 seconds for the broker to be ready. You can verify it with:

```powershell
docker compose logs broker
```

---

## Running the demo

Open three terminals, all with the virtual environment activated.

**Terminal A — Consumer 0** (reads partitions 0 and 1 — alice and bob events)

```powershell
python consumer.py --id 0
```

**Terminal B — Consumer 1** (reads partition 2 — charlie events)

```powershell
python consumer.py --id 1
```

**Terminal C — Producer** (sends 8 messages across all 3 partitions)

```powershell
python producer.py
```

### Expected output

Producer:
```
Sending 8 messages to 'learn-topic' ...

  Partition 0 → alice   (consumer 0)
  Partition 1 → bob     (consumer 0)
  Partition 2 → charlie (consumer 1)

  [OK]  partition=0  offset=0  key=alice
  [OK]  partition=0  offset=1  key=alice
  [OK]  partition=0  offset=2  key=alice
  [OK]  partition=1  offset=0  key=bob
  [OK]  partition=1  offset=1  key=bob
  [OK]  partition=2  offset=0  key=charlie
  [OK]  partition=2  offset=1  key=charlie
  [OK]  partition=2  offset=2  key=charlie

All messages delivered successfully.
```

Consumer 0 (receives only partitions 0 and 1):
```
[consumer-0] assigned to 'learn-topic' partitions [0, 1]. Waiting for messages ...

[consumer-0]  partition=0  offset=0  key=alice
  payload: { "id": 1, "event": "user_signup", "user": "alice", ... }

[consumer-0]  partition=1  offset=0  key=bob
  payload: { "id": 3, "event": "page_view", "user": "bob", ... }
...
```

Consumer 1 (receives only partition 2):
```
[consumer-1] assigned to 'learn-topic' partitions [2]. Waiting for messages ...

[consumer-1]  partition=2  offset=0  key=charlie
  payload: { "id": 4, "event": "user_signup", "user": "charlie", ... }
...
```

---

## Things to try next

**Re-read all messages from offset 0**
Change `GROUP_ID` in `consumer.py` to a new string (e.g. `learn-group-2`) and restart.
Kafka keeps messages until the retention period expires, so old messages are always available
to a new consumer group.

**Observe partition stickiness**
Run `producer.py` several times. Notice that each user's messages always go to the same
partition and always land on the same consumer, preserving per-user order.

**Fan-out (pub/sub)**
Run two instances of `consumer.py --id 0` with *different* `GROUP_ID` values.
Both will receive every message from partitions 0 and 1 independently.

**Automatic rebalancing**
Change `assign()` back to `subscribe([TOPIC])` in `consumer.py` and start two instances
with the same `group.id`. Kafka will automatically split the 3 partitions between them
and rebalance when either consumer stops.

---

## Stopping everything

```powershell
# Stop consumers with Ctrl+C in each terminal, then:
docker compose down
```

`docker compose down` removes the container and its ephemeral storage, so the next
`docker compose up -d` starts with a clean broker and an empty topic.
