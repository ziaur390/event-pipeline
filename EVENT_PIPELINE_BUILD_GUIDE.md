# Event-Driven Data Pipeline — complete build guide

Everything you need, from `mkdir` to submitting the NADRA application.
Read Part 1 and 2 fully before typing anything. The understanding is the deliverable.

---

# PART 1 — What we are building and why

## 1.1 The problem, in plain terms

A system receives a constant stream of events. In NADRA's world these would be things like: a citizen record was updated, a verification request came in, a device sent a heartbeat. Thousands per hour.

Something has to process each one: validate it, look up reference data, write the result somewhere permanent, and let an operator see whether the whole thing is healthy.

The naive version of this is a `while True:` loop that reads from a queue and writes to a database. That version breaks in four specific ways:

1. The consumer crashes mid-processing. The event is gone forever.
2. The consumer is slow, so the queue grows without limit and the producer falls over.
3. A malformed event arrives. It fails, goes back on the queue, fails again, and now it is an infinite loop blocking every other event behind it.
4. The same event arrives twice. Your database now has duplicate records and your reports are wrong.

**This project is the version that does not break in those four ways.** That is the entire point. Every component you add exists to kill one of those four failure modes.

## 1.2 Why build this instead of four small projects

The NADRA job description asks for six things you currently cannot demonstrate:

- in-streaming and real-time platforms (RabbitMQ, Kafka)
- managing data sources including Redis
- configuration of containerized services
- Kubernetes deployment
- monitoring
- CI/CD for all of it

Building four toy projects for those six things gives you four things that look like tutorials. Building one pipeline that genuinely needs all of them gives you something that looks like work, plus an architecture you can draw on a whiteboard.

Interviewers ask "walk me through something you built." A pipeline is a story. Four separate demos are a list.

## 1.3 What "done" looks like

At the end you will be able to type one command and have seven containers running: a producer publishing events, a consumer processing them, a broker moving them, a cache deduplicating them, a database storing them, an API reporting on them, and a proxy in front of the API. You will have a Grafana-style metrics view, a Kubernetes deployment, a green CI badge, and a README with an architecture diagram.

You will also be able to answer, without hesitating: *"Why did you use RabbitMQ and Kafka both?"* and *"What happens when a message fails three times?"*

---

# PART 2 — The architecture and why every piece exists

## 2.1 The diagram

```
                      ┌──────────────┐
                      │   PRODUCER   │  generates events (Python)
                      └──────┬───────┘
                             │ publish
                             ▼
        ┌────────────────────────────────────────┐
        │              RABBITMQ                  │
        │  exchange: events (topic)              │
        │  queue: events.worker                  │
        │  queue: events.dead   (dead letters)   │
        └──────┬───────────────────────┬─────────┘
               │ consume               │ after 3 failed attempts
               ▼                       ▼
        ┌──────────────┐        ┌─────────────┐
        │   CONSUMER   │───────▶│  DEAD LETTER│
        │   (Python)   │        │    QUEUE    │
        └──┬────┬───┬──┘        └─────────────┘
           │    │   │
   dedup   │    │   │  persist
   ┌───────┘    │   └────────────┐
   ▼            ▼                ▼
┌───────┐  ┌─────────┐    ┌────────────┐
│ REDIS │  │  NGINX  │    │ POSTGRESQL │
│ cache │  │  proxy  │    │  durable   │
│ dedup │  └────┬────┘    │   storage  │
└───────┘       │         └────────────┘
                ▼
          ┌──────────┐      ┌────────────┐
          │   API    │─────▶│ PROMETHEUS │
          │ FastAPI  │ /metrics           │
          └──────────┘      └────────────┘
```

## 2.2 Why each component, and what it kills

### Producer (Python service)
**Why it exists:** every pipeline has a source. In production this would be an HTTP endpoint or a database change stream. We simulate one so the pipeline can run on your laptop.

**Kills:** nothing on its own. It is the input.

**What you learn:** how to publish a durable message with a persistent delivery mode and a message id.

---

### RabbitMQ (the broker)
**Why it exists:** it decouples the producer from the consumer. The producer publishes and moves on. If the consumer is down, or slow, or restarting, the messages wait in a queue instead of being lost.

**Kills failure mode 2** (slow consumer). The queue absorbs the backlog. This is called *buffering*, and it is the single most important reason brokers exist.

**Why RabbitMQ specifically, and not just call the database directly:**
- Messages survive a broker restart (we set `durable=True`)
- Messages survive a consumer crash (we only acknowledge after successful processing)
- Failed messages can be routed somewhere else instead of blocking the queue

**Terms you must know:**
- **Exchange** — where the producer sends a message. It does not store anything. It routes.
- **Queue** — where messages wait for a consumer.
- **Binding** — the rule that connects an exchange to a queue.
- **Routing key** — a label on the message, like `event.created`, that the binding matches against.
- **Topic exchange** — matching with wildcards. `event.#` matches `event.created`, `event.retry`, and anything else starting with `event.`.
- **Ack / nack** — "I processed this successfully, remove it" / "I failed, do something else with it."
- **Prefetch count** — how many unacknowledged messages the broker hands one consumer at a time. Without this, one consumer takes everything and starves the others.

**Kills failure mode 1** (crash loses the event). Because we acknowledge *after* processing, a crash mid-processing means the message is never acknowledged, so RabbitMQ redelivers it.

---

### The dead-letter queue (DLQ)
**Why it exists:** this is the answer to failure mode 3, and it is the thing that separates a real pipeline from a tutorial.

A malformed event fails. `nack` with `requeue=True` puts it straight back at the front of the queue. The consumer picks it up again, fails again, and now you have an infinite loop that starves every good message behind it.

Instead: fail three times, then route it to a separate queue and move on.

**Kills failure mode 3** (poison message blocks the queue).

**How it is wired:**
1. Declare a dead-letter exchange (`events.dlx`, fanout) and a queue (`events.dead`)
2. Tell the main queue: `x-dead-letter-exchange: events.dlx`
3. In the consumer, track attempts in a message header
4. Under the limit: republish to a retry queue with a TTL, which dead-letters *back* to the main queue after the delay
5. Over the limit: `basic_nack(requeue=False)`, which sends it to `events.dead`

The retry-with-delay part is the elegant bit. A queue with `x-message-ttl: 5000` and `x-dead-letter-routing-key: events.worker` acts as a timer. Messages sit there for five seconds and then reappear on the main queue automatically. No `sleep()` in your code.

**This is the single most impressive thing in the project.** Learn it properly and you can talk about it for five minutes.

---

### Redis (cache and deduplication)
**Why it exists:** two jobs, both about not doing expensive work twice.

**Job 1 — caching.** Looking up reference data in PostgreSQL for every single event is wasteful when the same device appears thousands of times. Redis holds it in memory.

**Job 2 — deduplication.** This is the important one.

RabbitMQ and Kafka both give you **at-least-once** delivery. That means *at least once* — duplicates are not just possible, they are normal. A consumer crashes after processing but before acknowledging, and the message is redelivered. Now you have processed it twice.

Redis fixes this with one atomic operation:

```
SET processed:<event_id> 1 EX 3600 NX
```

`NX` means "only set if it does not already exist." If it sets, this is the first time we have seen the event. If it does not set, we have seen it before, so skip.

**Kills failure mode 4** (duplicate processing).

**Why Redis for this and not PostgreSQL:** it is in memory and it is one atomic call. A database round-trip on every event is slower, and you would need a transaction to make the check-and-set atomic. Redis gives you that for free.

**Why you still need a database-level guard too:** Redis can be flushed, restarted, or evicted. So the `events` table has a `UNIQUE` constraint on `event_id` and we insert with `ON CONFLICT DO NOTHING`. Redis is the fast path, the database is the authoritative one. Belt and braces. Say that in an interview and you sound like you have run something in production.

---

### PostgreSQL (durable storage)
**Why it exists:** it is the source of truth. Redis is a cache and can disappear. PostgreSQL is where the processed events live permanently.

**Kills:** nothing directly. It is the output.

**What matters here:** the unique constraint, and `JSONB` for the event payload so you can query inside it later.

---

### FastAPI + Nginx (operational interface)
**Why it exists:** an operator needs to ask "is this thing healthy right now?" without reading container logs. The API answers that.

Nginx sits in front because that is how this is done in production: TLS terminates at the proxy, the proxy routes to the app, the app is never exposed directly. Nginx is named in the NADRA requirements under webservers, so having it in the stack is the point.

**Milestone 5 adds this.**

---

### Prometheus (metrics)
**Why it exists:** logs tell you what happened to one event. Metrics tell you what is happening to all of them.

The metrics that matter here:
- `events_consumed_total` — is the consumer keeping up?
- `events_deduplicated_total` — how many duplicates are we seeing? A sudden spike means something upstream is misbehaving.
- `dlq_depth` — are we silently losing messages? If this is climbing, the pipeline is broken and nobody noticed.
- `queue_depth` — if this climbs, the consumer is too slow.

`dlq_depth` is the one to highlight. A pipeline with no DLQ monitoring fails silently. That is a genuinely senior observation.

---

### Kafka (second broker)
**Why it exists, honestly:** because the JD names it, and because being able to explain the difference between RabbitMQ and Kafka is exactly what "knowledge of in-streaming and real-time platforms" means.

**The difference, in two sentences each:**

*RabbitMQ is a smart broker with dumb consumers.* The broker tracks which messages have been acknowledged and deletes them once they are. It decides which consumer gets which message. Good for task queues: "do this job, once."

*Kafka is a dumb broker with smart consumers.* It is an append-only log. Messages are not deleted when read; they are retained for a configured period. Consumers track their own position (their *offset*) in the log. Good for event streams: "here is what happened, in order, replay it if you need to."

**The practical consequence:** in Kafka you can rewind and reprocess yesterday's events. In RabbitMQ you cannot, because they are gone.

**How to include both without building everything twice:** make the broker a configuration switch. `BROKER=rabbitmq` or `BROKER=kafka`. The consumer code has a small adapter for each. Same business logic. That is what a real abstraction looks like, and it takes one extra file rather than a second project.

---

### Docker Compose
**Why it exists:** seven services, one command. Reproducibility.

The reason this matters for the job: "containerization services like docker/Kubernetes" is a requirement, and `docker compose up` bringing up a realistic multi-service system is the demonstration.

---

### Kubernetes
**Why it exists:** Compose runs on one machine. Kubernetes schedules across many, restarts failed containers, and scales replicas.

**What you actually need:** a Deployment and a Service per component, a ConfigMap for shared configuration, and a Namespace. Deploy to a local `kind` cluster.

**The one thing to understand:** in Compose you say `depends_on`. In Kubernetes you do not, because containers come and go. Instead every service must retry until its dependency appears. This is why the producer and consumer in this guide both have retry loops in their startup code. That is not laziness, it is the correct pattern for both environments.

**Kills:** nothing new. It is the same pipeline, deployed the way production runs it.

---

### GitHub Actions
**Why it exists:** "CI/CD pipelines" is a named requirement, and a green badge on the repo README is visible proof.

**What the workflow does:** on every push, install dependencies, lint, run pytest, build the Docker images. That is enough. Do not over-engineer it.

---

### Bash scripts
**Why it exists:** "scripting language like Python, Java, Bash, Ruby etc." is a requirement, and a `scripts/` folder with working scripts proves Bash usage in a way that a skills line cannot.

Three scripts: `up.sh`, `down.sh`, `logs.sh`.

---

# PART 3 — The four concepts to understand before you type

If you understand these four things, everything else is syntax.

## 3.1 Delivery guarantees

| Guarantee | What it means | What you risk |
|---|---|---|
| At-most-once | Acknowledge before processing | Losing events |
| **At-least-once** | **Acknowledge after processing** | **Duplicate events** |
| Exactly-once | Broker and consumer agree, transactionally | Complexity; often not achievable |

**We build at-least-once plus idempotent processing.** That is the industry standard, and it is what you say in an interview. "I chose at-least-once delivery with idempotent consumers, because at-most-once loses data and exactly-once is a lie in most real systems."

## 3.2 Idempotency

An operation is idempotent if doing it twice has the same effect as doing it once.

`INSERT ... ON CONFLICT DO NOTHING` is idempotent. `INSERT` alone is not.
`SET key value` is idempotent. `INCREMENT counter` is not.

At-least-once delivery plus idempotent handlers equals correctness. Learn that sentence.

## 3.3 Backpressure

What happens when events arrive faster than you can process them?

Bad answer: memory grows until the process dies.
Good answer: messages wait in the broker queue, and you monitor the queue depth and scale consumers when it grows.

`prefetch_count` is the control knob at the consumer. The broker queue is the buffer. The metric is your early warning.

## 3.4 Poison messages

A message your code cannot process, no matter how many times you retry it. Malformed JSON, a missing field, a value out of range.

Retrying forever is the wrong answer. The right answer is bounded retries, then quarantine. That is the dead-letter queue.

---

# PART 4 — Prerequisites

Install these before starting:

1. **Docker Desktop** — running, with at least 6 GB of memory allocated. Seven containers is not trivial.
2. **Python 3.11+**
3. **Git**
4. **kubectl** and **kind** (for Milestone 8 only — install later if you want)
5. **A terminal.** Git Bash is fine on Windows.

Check them:

```bash
docker --version
docker compose version
python --version
git --version
```

Verify Docker actually works before going further:

```bash
docker run --rm hello-world
```

---

# PART 5 — The build, milestone by milestone

Build in this order. Each milestone is independently testable. **Do not move to the next one until the current one works.**

---

## MILESTONE 1 — Skeleton and the first message

**Goal:** a producer publishes, RabbitMQ holds, a consumer prints. Nothing else.

**Why first:** if you add the database and Redis before the broker works, you will not know which piece is broken.

### Step 1.1 — Create the repo

```bash
mkdir event-pipeline && cd event-pipeline
git init
mkdir -p producer consumer db nginx prometheus k8s scripts tests .github/workflows
touch README.md
```

### Step 1.2 — `docker-compose.yml` (partial for now)

Create `docker-compose.yml`:

```yaml
services:
  rabbitmq:
    image: rabbitmq:3.13-management
    container_name: pipeline-rabbitmq
    ports:
      - "5672:5672"     # AMQP protocol, this is what your code talks to
      - "15672:15672"   # management web UI
    environment:
      RABBITMQ_DEFAULT_USER: pipeline
      RABBITMQ_DEFAULT_PASS: pipeline
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq

volumes:
  rabbitmq_data:
```

**Why the healthcheck:** `depends_on` alone only waits for the container to *start*, not for RabbitMQ to be *ready*. The healthcheck makes `depends_on: condition: service_healthy` wait for actual readiness. This is the number one cause of "my consumer crashes on startup."

**Why the named volume:** so queues and messages survive `docker compose down`.

Start it:

```bash
docker compose up -d rabbitmq
docker compose ps
```

Open `http://localhost:15672`, log in as `pipeline` / `pipeline`. You will see the management UI. This UI is your debugging tool for the whole project.

### Step 1.3 — The producer

`producer/requirements.txt`:

```
pika==1.3.2
```

`producer/Dockerfile`:

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "-u", "producer.py"]
```

**Why `-u`:** unbuffered output, so `docker compose logs -f` shows lines immediately instead of in chunks.

`producer/producer.py`:

```python
import json
import logging
import os
import random
import time
import uuid

import pika

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s producer %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://pipeline:pipeline@rabbitmq:5672/%2F")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5"))

EXCHANGE = "events"
ROUTING_KEY = "event.created"

EVENT_TYPES = [
    "device.heartbeat",
    "citizen.record.updated",
    "sensor.reading",
    "transaction.posted",
]


def connect():
    """Retry until the broker is accepting connections.

    Required in Kubernetes, where containers start in any order, and useful in
    Compose where a healthcheck reduces but does not remove the race.
    """
    while True:
        try:
            return pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        except pika.exceptions.AMQPConnectionError:
            log.warning("broker not ready, retrying in 3s")
            time.sleep(3)


def main():
    connection = connect()
    channel = connection.channel()

    # durable=True so the exchange survives a broker restart.
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

    log.info("producer ready, publishing %.1f events/sec to %s", EVENTS_PER_SECOND, EXCHANGE)
    published = 0

    while True:
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": random.choice(EVENT_TYPES),
            "payload": {
                "device_id": f"dev-{random.randint(1, 500)}",
                "value": round(random.uniform(0, 100), 2),
            },
        }

        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=ROUTING_KEY,
            body=json.dumps(event).encode(),
            properties=pika.BasicProperties(
                delivery_mode=2,                  # persistent message
                content_type="application/json",
                message_id=event["event_id"],
            ),
        )

        published += 1
        if published % 25 == 0:
            log.info("published %d events", published)

        time.sleep(1.0 / EVENTS_PER_SECOND)


if __name__ == "__main__":
    main()
```

**Why `delivery_mode=2`:** marks the message persistent so it is written to disk. Without it, a broker restart loses messages even though the queue is durable. Queue durability and message persistence are two separate settings, and you need both.

**Why `message_id`:** so the consumer can deduplicate using the event id.

### Step 1.4 — The consumer (minimal version)

`consumer/requirements.txt`:

```
pika==1.3.2
```

`consumer/Dockerfile`: same as the producer, with `consumer.py`.

`consumer/consumer.py` — minimal, just to prove the flow:

```python
import json
import logging
import os
import time

import pika

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s consumer %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://pipeline:pipeline@rabbitmq:5672/%2F")

EXCHANGE = "events"
QUEUE = "events.worker"
BINDING_KEY = "event.#"


def connect():
    while True:
        try:
            return pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        except pika.exceptions.AMQPConnectionError:
            log.warning("broker not ready, retrying in 3s")
            time.sleep(3)


def setup(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=BINDING_KEY)
    channel.basic_qos(prefetch_count=10)
    log.info("listening on %s", QUEUE)


def on_message(channel, method, properties, body):
    event = json.loads(body)
    log.info("received %s type=%s", event["event_id"], event["event_type"])
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = connect()
    channel = connection.channel()
    setup(channel)
    channel.basic_consume(queue=QUEUE, on_message_callback=on_message)
    channel.start_consuming()


if __name__ == "__main__":
    main()
```

### Step 1.5 — Wire them into Compose

Append to `docker-compose.yml`:

```yaml
  producer:
    build: ./producer
    container_name: pipeline-producer
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_URL: amqp://pipeline:pipeline@rabbitmq:5672/%2F
      EVENTS_PER_SECOND: "5"
    restart: unless-stopped

  consumer:
    build: ./consumer
    container_name: pipeline-consumer
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      RABBITMQ_URL: amqp://pipeline:pipeline@rabbitmq:5672/%2F
    restart: unless-stopped
```

**Why `restart: unless-stopped`:** if the consumer crashes on a bad message, we want it back. In production this is what Kubernetes does for you; in Compose you ask for it.

### Step 1.6 — Run it

```bash
docker compose up -d --build
docker compose logs -f consumer
```

**Success looks like:** a stream of `received <uuid> type=device.heartbeat` lines.

Now open `http://localhost:15672` → Queues. You will see `events.worker` with a message rate. Watch the "Ready" count. If it stays at 0, the consumer is keeping up. If it climbs, the producer is too fast.

**Break it on purpose — this is the important part.** Stop the consumer:

```bash
docker compose stop consumer
```

Watch the queue depth in the UI climb. Wait 30 seconds. Restart it:

```bash
docker compose start consumer
```

The consumer drains the backlog. **You have just demonstrated buffering, which is the reason brokers exist.** Remember this moment, it is an interview answer.

### Step 1.7 — Commit

```bash
cat > .gitignore <<'EOF'
__pycache__/
*.pyc
.env
.venv/
EOF

git add .
git commit -m "feat: producer and consumer with topic exchange over RabbitMQ"
```

---

## MILESTONE 2 — Durable storage in PostgreSQL

**Goal:** processed events land in a table.

**Why now:** the pipeline currently throws away everything it processes. Now it keeps it.

### Step 2.1 — `db/init.sql`

```sql
CREATE TABLE IF NOT EXISTS events (
    id           BIGSERIAL PRIMARY KEY,
    event_id     UUID        NOT NULL UNIQUE,
    event_type   TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_type         ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_processed_at ON events (processed_at DESC);

CREATE TABLE IF NOT EXISTS dead_letters (
    id         BIGSERIAL PRIMARY KEY,
    event_id   UUID,
    payload    JSONB,
    error      TEXT,
    failed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Why `event_id UUID NOT NULL UNIQUE`:** this is the authoritative idempotency guard. Redis is the fast path, this is the one that cannot be evicted.

**Why `JSONB` and not `TEXT`:** you can index and query inside it. `SELECT payload->>'device_id' FROM events` works. With `TEXT` it does not.

**Why the indexes:** the API filters by type and sorts by time. Without indexes those queries do full table scans.

### Step 2.2 — Add PostgreSQL to Compose

```yaml
  postgres:
    image: postgres:16-alpine
    container_name: pipeline-postgres
    environment:
      POSTGRES_USER: pipeline
      POSTGRES_PASSWORD: pipeline
      POSTGRES_DB: pipeline
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./db/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U pipeline"]
      interval: 10s
      timeout: 5s
      retries: 5
```

Add `postgres_data:` under `volumes:` at the bottom.

**Why `init.sql` is mounted to `/docker-entrypoint-initdb.d/`:** the Postgres image runs any `.sql` files in that directory on first start. It runs **only** when the data volume is empty. If you change `init.sql` later, you must `docker compose down -v` to wipe the volume and re-run it. This trips up everybody once.

### Step 2.3 — Update the consumer

`consumer/requirements.txt`:

```
pika==1.3.2
psycopg2-binary==2.9.9
```

Replace `consumer/consumer.py` with the version below. Read the comments, they explain the design decisions.

```python
import json
import logging
import os
import time

import pika
import psycopg2
from psycopg2.extras import Json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s consumer %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://pipeline:pipeline@rabbitmq:5672/%2F")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://pipeline:pipeline@postgres:5432/pipeline")

EXCHANGE = "events"
QUEUE = "events.worker"
BINDING_KEY = "event.#"

_db = None


def get_db():
    """One connection, reconnected if the server drops it."""
    global _db
    if _db is None or _db.closed:
        _db = psycopg2.connect(DATABASE_URL)
        _db.autocommit = True
        log.info("connected to postgres")
    return _db


def setup(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=BINDING_KEY)
    channel.basic_qos(prefetch_count=10)
    log.info("listening on %s", QUEUE)


def handle(event):
    """Persist one event. Must be idempotent: safe to run twice."""
    cur = get_db().cursor()
    cur.execute(
        """
        INSERT INTO events (event_id, event_type, payload)
        VALUES (%s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        """,
        (event["event_id"], event["event_type"], Json(event["payload"])),
    )
    return cur.rowcount          # 0 means it was already there


def on_message(channel, method, properties, body):
    try:
        event = json.loads(body)
        inserted = handle(event)
        if inserted == 0:
            log.info("duplicate (caught by unique constraint) %s", event["event_id"])
        else:
            log.info("stored %s type=%s", event["event_id"], event["event_type"])
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as exc:
        # Failure path is deliberately incomplete here; Milestone 4 adds retries.
        log.exception("failed to process message: %s", exc)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    while True:
        try:
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            channel = connection.channel()
            setup(channel)
            channel.basic_consume(queue=QUEUE, on_message_callback=on_message)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            log.warning("broker connection lost, reconnecting in 5s")
            time.sleep(5)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
```

**Why `autocommit = True`:** each statement commits immediately. For a single-insert handler this is correct and simpler. In a real multi-statement transaction you would turn it off and commit explicitly.

**Why `return cur.rowcount`:** `ON CONFLICT DO NOTHING` returns 0 rows affected when the row already existed. That gives you duplicate detection for free, using the database's own consistency guarantee rather than trusting Redis.

**Why the outer `while True`:** if the broker connection drops, `pika` raises. Catching it and reconnecting means the container does not die. This is why the outer loop exists.

### Step 2.4 — Wire the dependency

Update the consumer service in `docker-compose.yml`:

```yaml
  consumer:
    build: ./consumer
    container_name: pipeline-consumer
    depends_on:
      rabbitmq:
        condition: service_healthy
      postgres:
        condition: service_healthy
    environment:
      RABBITMQ_URL: amqp://pipeline:pipeline@rabbitmq:5672/%2F
      DATABASE_URL: postgresql://pipeline:pipeline@postgres:5432/pipeline
    restart: unless-stopped
```

### Step 2.5 — Run and verify

```bash
docker compose up -d --build
docker compose logs -f consumer
```

Check the data actually landed:

```bash
docker compose exec postgres psql -U pipeline -d pipeline -c \
  "SELECT event_type, count(*) FROM events GROUP BY event_type;"
```

**Success looks like:** counts per event type, climbing.

Try a query into the JSON:

```bash
docker compose exec postgres psql -U pipeline -d pipeline -c \
  "SELECT payload->>'device_id' AS device, count(*)
   FROM events GROUP BY device ORDER BY count(*) DESC LIMIT 5;"
```

**Success rate of this milestone:** you now have a pipeline that stores what it processes, with a database-level idempotency guarantee.

---

## MILESTONE 3 — Redis for caching and deduplication

**Goal:** stop doing expensive lookups repeatedly, and deduplicate in front of the database.

**Why now:** the database guard works, but only *after* you have already done the work. Redis lets you skip the work entirely.

### Step 3.1 — Add Redis to Compose

```yaml
  redis:
    image: redis:7-alpine
    container_name: pipeline-redis
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    volumes:
      - redis_data:/var/lib/redis
```

Add `redis_data:` under `volumes:`.

### Step 3.2 — Update the consumer

Add `redis==5.0.7` to `requirements.txt`, then add to `consumer.py`:

```python
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

DEDUP_TTL = 3600        # remember processed event ids for one hour
CACHE_TTL = 300         # reference data cache: five minutes

_r = None


def get_redis():
    global _r
    if _r is None:
        _r = redis.from_url(REDIS_URL, decode_responses=True)
    return _r


def is_duplicate(event_id):
    """Atomic check-and-set.

    SET key value EX ttl NX returns True only when the key did NOT exist.
    So a truthy return means this is the first time we have seen this event.
    """
    first_time = get_redis().set(f"processed:{event_id}", "1", ex=DEDUP_TTL, nx=True)
    return not first_time


def lookup_zone(device_id):
    """Simulate an expensive reference-data lookup we want to avoid repeating."""
    key = f"device:{device_id}"
    cached = get_redis().get(key)
    if cached is not None:
        return cached, True

    zone = f"zone-{abs(hash(device_id)) % 8}"
    get_redis().setex(key, CACHE_TTL, zone)
    return zone, False
```

Then change `on_message` to use them:

```python
def on_message(channel, method, properties, body):
    try:
        event = json.loads(body)
        event_id = event["event_id"]

        if is_duplicate(event_id):
            log.info("duplicate (caught by redis) %s", event_id)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        device_id = event["payload"].get("device_id", "unknown")
        zone, was_cached = lookup_zone(device_id)

        inserted = handle(event)
        log.info(
            "stored %s type=%s device=%s zone=%s cache_hit=%s db_written=%s",
            event_id, event["event_type"], device_id, zone, was_cached, inserted,
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as exc:
        log.exception("failed to process message: %s", exc)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
```

Add `REDIS_URL` to the consumer environment in Compose and add `redis` to `depends_on`.

### Step 3.3 — Verify

```bash
docker compose up -d --build
docker compose logs -f consumer
```

Watch for `cache_hit=True` appearing on repeat device ids.

**Prove the dedup works.** Publish the same event id twice on purpose:

```bash
docker compose exec rabbitmq rabbitmqadmin publish \
  exchange=events routing_key=event.created \
  payload='{"event_id":"11111111-1111-1111-1111-111111111111","event_type":"test","payload":{"device_id":"dev-1"}}'

docker compose exec rabbitmq rabbitmqadmin publish \
  exchange=events routing_key=event.created \
  payload='{"event_id":"11111111-1111-1111-1111-111111111111","event_type":"test","payload":{"device_id":"dev-1"}}'
```

The first is stored. The second logs `duplicate (caught by redis)`.

Then flush Redis and publish it a third time:

```bash
docker compose exec redis redis-cli FLUSHALL
```

Now Redis has forgotten, so the consumer does the work, reaches PostgreSQL, and the unique constraint catches it: `duplicate (caught by unique constraint)`.

**This is the two-layer idempotency story, demonstrated end to end.** Be ready to explain it exactly like that.

---

## MILESTONE 4 — Dead-letter queue and bounded retries

**Goal:** a message that keeps failing stops blocking the pipeline.

**Why now:** this is the hardest and most valuable part. Do it after the happy path is solid.

### Step 4.1 — The design

```
fail
  │
  ├── attempts < MAX_RETRIES
  │      └─▶ publish to events.retry (TTL 5s)
  │             └─ TTL expires, dead-letters BACK to events.worker
  │                   └─ retried
  │
  └── attempts >= MAX_RETRIES
         └─▶ basic_nack(requeue=False) ─▶ events.dlx ─▶ events.dead
```

The clever part is the retry queue. It has **no consumer**. It exists only to hold messages for five seconds, then dead-letter them back to the main queue. A queue used as a timer.

### Step 4.2 — Rewrite `setup()` in the consumer

```python
EXCHANGE = "events"
QUEUE = "events.worker"
BINDING_KEY = "event.#"

DLX = "events.dlx"              # dead-letter exchange
DLQ = "events.dead"             # where permanently-failed messages end up
RETRY_QUEUE = "events.retry"    # delay queue, no consumer

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_MS = 5000           # five seconds between attempts


def setup(channel):
    # 1. Dead-letter exchange and queue, declared FIRST so the main queue can
    #    reference them. RabbitMQ rejects a queue that points at a missing exchange.
    channel.exchange_declare(exchange=DLX, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=DLQ, durable=True)
    channel.queue_bind(queue=DLQ, exchange=DLX)

    # 2. Retry queue: no consumer. Messages sit for RETRY_DELAY_MS, then the
    #    expiry dead-letters them back to the main queue.
    channel.queue_declare(
        queue=RETRY_QUEUE,
        durable=True,
        arguments={
            "x-message-ttl": RETRY_DELAY_MS,
            "x-dead-letter-exchange": "",             # default exchange
            "x-dead-letter-routing-key": QUEUE,       # back to the worker queue
        },
    )

    # 3. Main exchange and work queue.
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(
        queue=QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DLX,   # final destination for nacked messages
        },
    )
    channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=BINDING_KEY)

    channel.basic_qos(prefetch_count=10)
    log.info("listening on %s (retry after %dms, max %d attempts)",
             QUEUE, RETRY_DELAY_MS, MAX_RETRIES)
```

**Why `x-dead-letter-exchange: ""` on the retry queue:** an empty string means the *default* exchange, which routes directly by queue name. Combined with `x-dead-letter-routing-key: events.worker`, expired messages land back on the work queue. That is the whole trick.

### Step 4.3 — Rewrite the failure path

```python
def record_dead_letter(event_id, payload, error):
    cur = get_db().cursor()
    cur.execute(
        "INSERT INTO dead_letters (event_id, payload, error) VALUES (%s, %s, %s)",
        (event_id, Json(payload), str(error)[:2000]),
    )


def on_message(channel, method, properties, body):
    headers = properties.headers or {}
    attempts = int(headers.get("x-attempts", 0))

    try:
        event = json.loads(body)
        event_id = event["event_id"]

        if is_duplicate(event_id):
            log.info("duplicate (redis) %s", event_id)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        handle(event)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as exc:
        if attempts < MAX_RETRIES:
            log.warning("attempt %d/%d failed, retrying in %dms: %s",
                        attempts + 1, MAX_RETRIES, RETRY_DELAY_MS, exc)
            channel.basic_publish(
                exchange="",                      # default exchange
                routing_key=RETRY_QUEUE,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                    headers={**headers, "x-attempts": attempts + 1},
                ),
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)

        else:
            log.error("max retries reached, dead-lettering: %s", exc)
            try:
                payload = json.loads(body)
                record_dead_letter(payload.get("event_id"), payload, exc)
            except Exception:
                record_dead_letter(None, {"raw": body.decode(errors="replace")}, exc)

            # requeue=False routes it to the queue's dead-letter exchange.
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
```

**Why publish to the retry queue rather than `nack(requeue=True)`:** `requeue=True` puts the message straight back, with no delay, and it will be redelivered immediately to the same consumer. With a poison message that is a hot loop burning CPU. The retry queue adds a delay *and* carries a header so we can count attempts.

**Why record the dead letter in the database:** otherwise the DLQ is a black hole nobody looks at. Now you can query it.

### Step 4.4 — Verify

Watch it work:

```bash
docker compose logs -f consumer
```

Then publish a message that is guaranteed to fail. The cleanest way is to make `handle()` reject a sentinel value:

```python
# temporary, for testing
if event["event_type"] == "poison":
    raise ValueError("deliberate failure for retry testing")
```

Publish a poison message and watch the log. You should see:

```
attempt 1/3 failed, retrying in 5000ms
attempt 2/3 failed, retrying in 5000ms
attempt 3/3 failed, retrying in 5000ms
max retries reached, dead-lettering
```

Then check the dead-letter table:

```bash
docker compose exec postgres psql -U pipeline -d pipeline -c \
  "SELECT event_id, error, failed_at FROM dead_letters;"
```

And the DLQ depth in the RabbitMQ UI at `http://localhost:15672` → Queues → `events.dead`.

**Remove the poison sentinel afterwards, or keep it behind a `if os.getenv("TEST_MODE")` flag.** Actually, keep it available and non-default — it is useful for demos and it shows you test failure paths.

### Step 4.5 — Commit

```bash
git add .
git commit -m "feat: dead-letter queue with bounded retries via delay queue"
```

**This milestone is the one to prepare hardest for.** If an interviewer asks about reliability, this is where you point.

---

## MILESTONE 5 — API and Nginx

**Goal:** an operational interface, behind a reverse proxy.

### Step 5.1 — The API

`api/requirements.txt`:

```
fastapi==0.115.0
uvicorn[standard]==0.32.0
psycopg2-binary==2.9.9
redis==5.0.7
pika==1.3.2
prometheus-client==0.21.0
```

`api/main.py`:

```python
import json
import logging
import os

import pika
import psycopg2
import psycopg2.extras
import redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

logging.basicConfig(level=logging.INFO, format="%(asctime)s api %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://pipeline:pipeline@postgres:5432/pipeline")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://pipeline:pipeline@rabbitmq:5672/%2F")

app = FastAPI(title="Event Pipeline API", version="1.0.0")

QUEUE_DEPTH = Gauge("pipeline_queue_depth", "Messages waiting in the work queue")
DLQ_DEPTH = Gauge("pipeline_dlq_depth", "Messages in the dead-letter queue")
EVENTS_STORED = Gauge("pipeline_events_stored", "Total events in the database")
DEAD_LETTERS = Gauge("pipeline_dead_letter_rows", "Total rows in dead_letters")


@app.get("/health")
def health():
    """Liveness. Must be cheap: orchestration calls it constantly."""
    return {"status": "ok"}


@app.get("/ready")
def ready():
    """Readiness. Checks dependencies, so a container with no database
    is not sent traffic."""
    checks = {}
    try:
        psycopg2.connect(DATABASE_URL, connect_timeout=2).close()
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {exc}"
    try:
        redis.from_url(REDIS_URL, socket_connect_timeout=2).ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {exc}"
    try:
        conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        conn.close()
        checks["rabbitmq"] = "ok"
    except Exception as exc:
        checks["rabbitmq"] = f"error: {exc}"

    if any(v != "ok" for v in checks.values()):
        raise HTTPException(status_code=503, detail=checks)
    return checks


@app.get("/stats")
def stats():
    """What an operator actually wants to know."""
    result = {}
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM events")
        result["events_total"] = cur.fetchone()[0]

        cur.execute("SELECT event_type, count(*) FROM events GROUP BY event_type ORDER BY 2 DESC")
        result["by_type"] = dict(cur.fetchall())

        cur.execute("SELECT count(*) FROM dead_letters")
        result["dead_letters"] = cur.fetchone()[0]

        cur.execute("""
            SELECT date_trunc('minute', processed_at) AS minute, count(*)
            FROM events
            WHERE processed_at > now() - interval '10 minutes'
            GROUP BY 1 ORDER BY 1 DESC
        """)
        result["last_10_minutes"] = [
            {"minute": m.isoformat(), "count": c} for m, c in cur.fetchall()
        ]
    return result


@app.get("/events")
def recent_events(limit: int = 20):
    limit = min(max(limit, 1), 200)      # clamp, never trust a query parameter
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor(
        cursor_factory=psycopg2.extras.RealDictCursor
    ) as cur:
        cur.execute(
            "SELECT event_id, event_type, payload, processed_at "
            "FROM events ORDER BY processed_at DESC LIMIT %s",
            (limit,),
        )
        return cur.fetchall()


@app.get("/dead-letters")
def dead_letters(limit: int = 20):
    limit = min(max(limit, 1), 200)
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor(
        cursor_factory=psycopg2.extras.RealDictCursor
    ) as cur:
        cur.execute(
            "SELECT event_id, error, failed_at FROM dead_letters "
            "ORDER BY failed_at DESC LIMIT %s",
            (limit,),
        )
        return cur.fetchall()


@app.get("/metrics")
def metrics():
    """Prometheus scrapes this."""
    try:
        with psycopg2.connect(DATABASE_URL, connect_timeout=2) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM events")
            EVENTS_STORED.set(cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM dead_letters")
            DEAD_LETTERS.set(cur.fetchone()[0])
    except Exception:
        pass

    try:
        conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        ch = conn.channel()
        q = ch.queue_declare(queue="events.worker", durable=True, passive=True)
        QUEUE_DEPTH.set(q.method.message_count)
        d = ch.queue_declare(queue="events.dead", durable=True, passive=True)
        DLQ_DEPTH.set(d.method.message_count)
        conn.close()
    except Exception:
        pass

    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
```

**Why `/health` and `/ready` are separate:** `/health` answers "is the process alive?" and must never touch the database, or it will report unhealthy when the database hiccups and Kubernetes will kill a perfectly good container. `/ready` answers "should traffic come here?" and *does* check dependencies. Getting these two confused causes outages. This is a good interview answer.

**Why the query was declared `passive=True`:** passive means "tell me about this queue, do not create it." You want the API to report on the queue, not to accidentally define infrastructure.

**Why `limit` is clamped:** never let a caller ask for a million rows.

### Step 5.2 — Nginx

`nginx/nginx.conf`:

```nginx
worker_processes auto;

events {
    worker_connections 1024;
}

http {
    upstream pipeline_api {
        server api:8000;
        keepalive 32;
    }

    # Rate limit: 20 requests/second per IP, burst of 40.
    limit_req_zone $binary_remote_addr zone=api_limit:10m rate=20r/s;

    server {
        listen 80;
        server_name _;

        # Do not advertise the Nginx version.
        server_tokens off;

        add_header X-Content-Type-Options nosniff always;
        add_header X-Frame-Options DENY always;

        location / {
            limit_req zone=api_limit burst=40 nodelay;
            proxy_pass http://pipeline_api;
            proxy_http_version 1.1;
            proxy_set_header Connection "";
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;

            proxy_connect_timeout 5s;
            proxy_read_timeout 30s;
        }
    }
}
```

**Why `proxy_set_header Connection ""` with `proxy_http_version 1.1`:** enables keepalive between Nginx and the upstream, which is the main performance win of putting a proxy in front.

**Why the rate limit:** it is the concrete thing a reverse proxy does for you. Two lines, and you can explain it.

### Step 5.3 — Wire into Compose

```yaml
  api:
    build: ./api
    container_name: pipeline-api
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      rabbitmq:
        condition: service_healthy
    environment:
      DATABASE_URL: postgresql://pipeline:pipeline@postgres:5432/pipeline
      REDIS_URL: redis://redis:6379/0
      RABBITMQ_URL: amqp://pipeline:pipeline@rabbitmq:5672/%2F
    ports:
      - "8000:8000"
    restart: unless-stopped

  nginx:
    image: nginx:1.27-alpine
    container_name: pipeline-nginx
    depends_on:
      - api
    ports:
      - "8080:80"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
    restart: unless-stopped
```

### Step 5.4 — Verify

```bash
docker compose up -d --build
curl -s http://localhost:8080/health   | python -m json.tool
curl -s http://localhost:8080/ready    | python -m json.tool
curl -s http://localhost:8080/stats    | python -m json.tool
curl -s http://localhost:8080/events?limit=3 | python -m json.tool
curl -s http://localhost:8080/dead-letters   | python -m json.tool
curl -s http://localhost:8080/metrics | head -20
```

**Note that everything goes through port 8080 (Nginx), not 8000 (the API).** Port 8000 is exposed only for debugging. In Kubernetes you would not expose it at all.

---

## MILESTONE 6 — Prometheus

**Goal:** metrics scraped and visible.

### Step 6.1 — `prometheus/prometheus.yml`

```yaml
global:
  scrape_interval: 10s
  evaluation_interval: 10s

scrape_configs:
  - job_name: pipeline-api
    metrics_path: /metrics
    static_configs:
      - targets: ["api:8000"]
```

### Step 6.2 — Add Prometheus to Compose

```yaml
  prometheus:
    image: prom/prometheus:v2.55.0
    container_name: pipeline-prometheus
    depends_on:
      - api
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus_data:/prometheus
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.retention.time=7d"
    restart: unless-stopped
```

Add `prometheus_data:` under `volumes:`.

### Step 6.2b — Optional but recommended: Grafana

```yaml
  grafana:
    image: grafana/grafana:11.3.0
    container_name: pipeline-grafana
    depends_on:
      - prometheus
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_AUTH_ANONYMOUS_ENABLED: "true"
    volumes:
      - grafana_data:/var/lib/grafana
    restart: unless-stopped
```

### Step 6.3 — Verify

```bash
docker compose up -d --build
```

Open `http://localhost:9090` → Status → Targets. `pipeline-api` should be **UP**.

Type this into the Prometheus query box:

```
pipeline_dlq_depth
pipeline_queue_depth
pipeline_events_stored
```

You will see the values. **Take a screenshot of this and put it in the README.** A metrics screenshot is worth more than a paragraph claiming you did monitoring.

In Grafana at `http://localhost:3000` (admin/admin), add Prometheus as a datasource at `http://prometheus:9090` and build one panel per metric.

### Step 6.4 — The observability point to make in an interview

> "The metric I care about most is dead-letter depth. A pipeline with no DLQ monitoring fails silently: events stop being processed, nothing errors, and nobody notices until someone asks why last week's data is missing. Queue depth tells you about load; DLQ depth tells you about correctness."

---

## MILESTONE 7 — Kafka alongside RabbitMQ

**Goal:** demonstrate both brokers behind one interface, and be able to explain the difference.

**Why this shape and not a second pipeline:** because that is what an abstraction is. Same consumer logic, two transports.

### Step 7.1 — Add Kafka to Compose, behind a profile

```yaml
  kafka:
    image: bitnami/kafka:3.7
    container_name: pipeline-kafka
    profiles: ["kafka"]
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_NODE_ID: "0"
      KAFKA_CFG_PROCESS_ROLES: "controller,broker"
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: "0@kafka:9093"
      KAFKA_CFG_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
      KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092"
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"
      ALLOW_PLAINTEXT_LISTENER: "yes"
    volumes:
      - kafka_data:/bitnami/kafka
```

Add `kafka_data:` under `volumes:`.

**Why `profiles: ["kafka"]`:** this service does not start by default. You run it only when you want to compare brokers:

```bash
docker compose --profile kafka up -d
```

That keeps the default `docker compose up` fast and light, and it shows you understand Compose profiles.

**Why KRaft mode and no ZooKeeper:** ZooKeeper was removed from Kafka as of 3.5+. Running a ZooKeeper container in 2026 would look dated.

### Step 7.2 — The broker abstraction

`consumer/brokers.py`:

```python
"""Two transports, one interface.

RabbitMQ and Kafka model the same problem differently:

  RabbitMQ  smart broker, dumb consumer. The broker tracks acknowledgements and
            deletes messages once consumed, and decides which consumer gets what.
            Natural fit for task queues: do this job, once.

  Kafka     dumb broker, smart consumer. An append-only log. Messages are retained
            for a configured period regardless of who read them, and each consumer
            tracks its own offset. Natural fit for event streams you may replay.
"""

import json
import logging
import os

log = logging.getLogger(__name__)


class RabbitMQBroker:
    def __init__(self):
        import pika
        self.pika = pika
        self.url = os.environ["RABBITMQ_URL"]
        self.queue = os.getenv("RABBITMQ_QUEUE", "events.worker")

    def connect(self):
        return self.pika.BlockingConnection(self.pika.URLParameters(self.url))

    def consume(self, channel, callback):
        channel.basic_qos(prefetch_count=10)
        channel.basic_consume(queue=self.queue, on_message_callback=callback)
        channel.start_consuming()

    def ack(self, channel, token):
        channel.basic_ack(delivery_tag=token)

    def reject(self, channel, token, requeue=False):
        channel.basic_nack(delivery_tag=token, requeue=requeue)

    def publish(self, channel, payload, routing_key):
        channel.basic_publish(
            exchange="",
            routing_key=routing_key,
            body=json.dumps(payload).encode(),
            properties=self.pika.BasicProperties(delivery_mode=2),
        )


class KafkaBroker:
    def __init__(self):
        from kafka import KafkaConsumer
        self.KafkaConsumer = KafkaConsumer
        self.servers = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "events")

    def consume(self, _channel, callback):
        consumer = self.KafkaConsumer(
            self.topic,
            bootstrap_servers=self.servers,
            group_id="pipeline-workers",
            auto_offset_reset="earliest",
            enable_auto_commit=False,        # we commit only after processing
            value_deserializer=lambda b: json.loads(b.decode()),
        )
        for record in consumer:
            callback(consumer, record)

    def ack(self, consumer, _token):
        consumer.commit()                    # advance the offset

    def reject(self, consumer, _token, requeue=False):
        # Kafka has no nack. A failed message is simply not committed, so it will
        # be redelivered, or you route it to a separate dead-letter TOPIC yourself.
        consumer.seek_to_committed()
        log.warning("kafka: message not committed, will be redelivered")


def get_broker():
    kind = os.getenv("BROKER", "rabbitmq").lower()
    return KafkaBroker() if kind == "kafka" else RabbitMQBroker()
```

**The single most important observation in this file** is in the `reject` method. RabbitMQ has a native nack and a native dead-letter exchange. Kafka has neither. In Kafka you implement dead-lettering yourself by producing the failed message to a separate topic.

**That contrast is the answer to "why both?"** Say it exactly like this:

> "They solve different problems. RabbitMQ is a smart broker with dumb consumers: it tracks acknowledgements, deletes on ack, and has first-class dead-lettering. Kafka is a dumb broker with smart consumers: it is an append-only log, consumers track their own offsets, and messages are retained so you can replay them. I used RabbitMQ for the work queue and added Kafka behind the same interface to compare the two models. The clearest difference shows up in failure handling: RabbitMQ gives you nack and a dead-letter exchange out of the box, Kafka makes you write that yourself because there is no nack."

### Step 7.3 — Verify

```bash
docker compose --profile kafka up -d
BROKER=kafka docker compose up -d consumer
docker compose logs -f consumer
```

Then explain the difference in your README under a heading like "Why two brokers?".

---

## MILESTONE 8 — Kubernetes

**Goal:** the same pipeline, deployed the way production runs it.

### Step 8.1 — Install kind

```bash
# Windows with winget
winget install Kubernetes.kind
winget install Kubernetes.kubectl
```

Or download from `kind.sigs.k8s.io` and `kubernetes.io/docs/tasks/tools`.

### Step 8.2 — Manifests

`k8s/00-namespace.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: pipeline
```

`k8s/01-configmap.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: pipeline-config
  namespace: pipeline
data:
  RABBITMQ_URL: "amqp://pipeline:pipeline@rabbitmq:5672/%2F"
  REDIS_URL: "redis://redis:6379/0"
  DATABASE_URL: "postgresql://pipeline:pipeline@postgres:5432/pipeline"
  MAX_RETRIES: "3"
```

`k8s/10-postgres.yaml`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: postgres-pvc
  namespace: pipeline
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: pipeline
spec:
  replicas: 1
  selector:
    matchLabels: {app: postgres}
  template:
    metadata:
      labels: {app: postgres}
    spec:
      containers:
        - name: postgres
          image: postgres:16-alpine
          env:
            - {name: POSTGRES_USER,  value: pipeline}
            - {name: POSTGRES_PASSWORD, value: pipeline}
            - {name: POSTGRES_DB, value: pipeline}
          ports:
            - containerPort: 5432
          volumeMounts:
            - {name: data, mountPath: /var/lib/postgresql/data}
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "pipeline"]
            initialDelaySeconds: 5
            periodSeconds: 5
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: postgres-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: pipeline
spec:
  selector: {app: postgres}
  ports:
    - {port: 5432, targetPort: 5432}
```

`k8s/20-rabbitmq.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rabbitmq
  namespace: pipeline
spec:
  replicas: 1
  selector:
    matchLabels: {app: rabbitmq}
  template:
    metadata:
      labels: {app: rabbitmq}
    spec:
      containers:
        - name: rabbitmq
          image: rabbitmq:3.13-management
          env:
            - {name: RABBITMQ_DEFAULT_USER, value: pipeline}
            - {name: RABBITMQ_DEFAULT_PASS, value: pipeline}
          ports:
            - {containerPort: 5672}
            - {containerPort: 15672}
          readinessProbe:
            exec:
              command: ["rabbitmq-diagnostics", "-q", "ping"]
            initialDelaySeconds: 15
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: rabbitmq
  namespace: pipeline
spec:
  selector: {app: rabbitmq}
  ports:
    - {name: amqp,   port: 5672,  targetPort: 5672}
    - {name: mgmt,   port: 15672, targetPort: 15672}
```

`k8s/30-redis.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
  namespace: pipeline
spec:
  replicas: 1
  selector:
    matchLabels: {app: redis}
  template:
    metadata:
      labels: {app: redis}
    spec:
      containers:
        - name: redis
          image: redis:7-alpine
          ports:
            - {containerPort: 6379}
          readinessProbe:
            exec:
              command: ["redis-cli", "ping"]
            initialDelaySeconds: 3
            periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: redis
  namespace: pipeline
spec:
  selector: {app: redis}
  ports:
    - {port: 6379, targetPort: 6379}
```

`k8s/40-consumer.yaml` — note the probes:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: consumer
  namespace: pipeline
spec:
  replicas: 2                      # two workers, sharing the same queue
  selector:
    matchLabels: {app: consumer}
  template:
    metadata:
      labels: {app: consumer}
    spec:
      containers:
        - name: consumer
          image: event-pipeline-consumer:latest
          imagePullPolicy: IfNotPresent
          envFrom:
            - configMapRef: {name: pipeline-config}
          resources:
            requests: {cpu: "100m", memory: "128Mi"}
            limits:   {cpu: "500m", memory: "512Mi"}
```

`k8s/50-api.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
  namespace: pipeline
spec:
  replicas: 2
  selector:
    matchLabels: {app: api}
  template:
    metadata:
      labels: {app: api}
    spec:
      containers:
        - name: api
          image: event-pipeline-api:latest
          imagePullPolicy: IfNotPresent
          envFrom:
            - configMapRef: {name: pipeline-config}
          ports:
            - {containerPort: 8000}
          livenessProbe:                 # cheap, no dependencies
            httpGet: {path: /health, port: 8000}
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:                # checks dependencies
            httpGet: {path: /ready, port: 8000}
            initialDelaySeconds: 10
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: api
  namespace: pipeline
spec:
  selector: {app: api}
  ports:
    - {port: 8000, targetPort: 8000}
```

**Why `replicas: 2` on the consumer:** the whole point of a broker is that multiple workers share the queue. RabbitMQ round-robins with `prefetch_count` limiting how much each worker holds. Scale to four and watch the queue drain faster.

**Why `livenessProbe` uses `/health` and `readinessProbe` uses `/ready`:** this is the distinction from Milestone 5. Getting it backwards means Kubernetes restarts healthy containers whenever the database has a blip.

**Why `resources.requests` and `limits`:** the scheduler needs requests to place pods; limits stop one pod starving the node. Missing limits is the most common junior mistake.

### Step 8.3 — Build, load, apply

```bash
kind create cluster --name pipeline

# kind runs its own container runtime, so images must be loaded into the cluster
docker build -t event-pipeline-consumer:latest ./consumer
docker build -t event-pipeline-api:latest ./api
kind load docker-image event-pipeline-consumer:latest --name pipeline
kind load docker-image event-pipeline-api:latest --name pipeline

kubectl apply -f k8s/
kubectl -n pipeline get pods -w
```

**Why `kind load docker-image`:** the cluster cannot see your local Docker images. Without this, pods sit in `ErrImagePull`. This is the number one kind gotcha.

### Step 8.4 — Verify

```bash
kubectl -n pipeline get pods
kubectl -n pipeline get svc
kubectl -n pipeline logs -f deploy/consumer
kubectl -n pipeline port-forward svc/api 8000:8000
```

Then `curl http://localhost:8000/health`.

Scale the consumer and watch:

```bash
kubectl -n pipeline scale deploy/consumer --replicas=4
kubectl -n pipeline get pods -w
```

**Screenshot `kubectl get pods` showing 4 running consumers.** That single screenshot proves the Kubernetes requirement.

### Step 8.5 — Tear down

```bash
kind delete cluster --name pipeline
```

---

## MILESTONE 9 — GitHub Actions CI

**Goal:** a green badge, and automated build-and-test on every push.

`.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install ruff pytest redis pika psycopg2-binary

      - name: Lint
        run: ruff check .

      - name: Run tests
        run: pytest -v

  build:
    runs-on: ubuntu-latest
    needs: test
    strategy:
      matrix:
        service: [producer, consumer, api]
    steps:
      - uses: actions/checkout@v4

      - uses: docker/setup-buildx-action@v3

      - name: Build ${{ matrix.service }} image
        uses: docker/build-push-action@v6
        with:
          context: ./${{ matrix.service }}
          push: false
          tags: event-pipeline-${{ matrix.service }}:${{ github.sha }}
          cache-from: type=gha
          cache-to: type=gha,mode=max
```

**Why `needs: test`:** do not build images for code that fails its tests. That is the "CI" in CI/CD.

**Why the matrix:** one job definition, three services. Shows you understand workflow reuse.

**Why `push: false`:** a portfolio project does not need published images. Building them is the proof.

Add the badge to the README:

```markdown
![CI](https://github.com/YOUR-USERNAME/event-pipeline/actions/workflows/ci.yml/badge.svg)
```

### Step 9.1 — A test that actually tests something

`tests/test_pipeline.py`:

```python
"""Unit tests. These run in CI with no broker, no database and no network."""

import json
import sys
import uuid
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "consumer"))


def make_event(event_type="device.heartbeat", device_id="dev-1"):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "payload": {"device_id": device_id, "value": 42.0},
    }


def test_event_serialises_as_valid_json():
    event = make_event()
    assert json.loads(json.dumps(event))["event_id"] == event["event_id"]


def test_duplicate_detection_returns_true_on_second_call():
    """This is the behaviour the whole idempotency design depends on."""
    import consumer

    fake = MagicMock()
    fake.set.return_value = None            # key already existed
    consumer._r = fake
    assert consumer.is_duplicate("abc") is True

    fake.set.return_value = True            # key was newly created
    assert consumer.is_duplicate("abc") is False


def test_zone_lookup_uses_cache_when_present():
    import consumer

    fake = MagicMock()
    fake.get.return_value = "zone-3"
    consumer._r = fake

    zone, was_cached = consumer.lookup_zone("dev-1")
    assert zone == "zone-3"
    assert was_cached is True
    fake.setex.assert_not_called()


def test_zone_lookup_writes_cache_on_miss():
    import consumer

    fake = MagicMock()
    fake.get.return_value = None
    consumer._r = fake

    _zone, was_cached = consumer.lookup_zone("dev-1")
    assert was_cached is False
    fake.setex.assert_called_once()
```

**Why these tests and not "does it import":** each one pins down a design decision. If someone changes the deduplication to use `get` then `set`, `test_duplicate_detection_returns_true_on_second_call` fails, because that change breaks atomicity. Tests that protect design decisions are worth writing.

Add `ruff` config to `pyproject.toml`:

```toml
[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP"]
```

### Step 9.2 — Verify

```bash
pip install ruff pytest
ruff check .
pytest -v
```

Push to GitHub. Watch the Actions tab. Get the green badge.

---

## MILESTONE 10 — Bash scripts

`scripts/up.sh`:

```bash
#!/usr/bin/env bash
# Bring the whole stack up and wait until the API answers.
set -euo pipefail

echo "==> starting stack"
docker compose up -d --build

echo "==> waiting for api to become ready"
for attempt in $(seq 1 30); do
    if curl -fsS http://localhost:8080/ready >/dev/null 2>&1; then
        echo "==> stack is ready after ${attempt}s"
        docker compose ps
        echo
        echo "  API        http://localhost:8080/health"
        echo "  RabbitMQ   http://localhost:15672  (pipeline / pipeline)"
        echo "  Prometheus http://localhost:9090"
        echo "  Grafana    http://localhost:3000  (admin / admin)"
        exit 0
    fi
    sleep 1
done

echo "!! did not become ready in 30s" >&2
docker compose logs --tail=50
exit 1
```

`scripts/down.sh`:

```bash
#!/usr/bin/env bash
# Stop the stack. Pass --volumes to also delete all data.
set -euo pipefail

if [[ "${1:-}" == "--volumes" ]]; then
    echo "==> stopping stack and DELETING all data"
    docker compose down -v
else
    echo "==> stopping stack (data preserved; use --volumes to wipe)"
    docker compose down
fi
```

`scripts/logs.sh`:

```bash
#!/usr/bin/env bash
# Tail logs from one service, or all of them.
set -euo pipefail

service="${1:-}"
if [[ -z "$service" ]]; then
    docker compose logs -f --tail=50
else
    docker compose logs -f --tail=100 "$service"
fi
```

```bash
chmod +x scripts/*.sh
```

**Why `set -euo pipefail`:** `-e` exit on error, `-u` error on undefined variables, `-o pipefail` fail if any command in a pipe fails. Every serious Bash script starts with this line. Mentioning it shows you write Bash deliberately rather than by accident.

**Why the readiness loop:** starting a stack and immediately curling it is the most common script bug. Wait for readiness, then report.

---

## MILESTONE 11 — Optional: Oracle XE

**Goal:** cover the "Oracle" keyword with something real.

**Do this last.** If you run out of days, skip it and drop Oracle from the resume.

```yaml
  oracle:
    image: gvenzl/oracle-xe:21-slim
    container_name: pipeline-oracle
    profiles: ["oracle"]
    ports:
      - "1521:1521"
    environment:
      ORACLE_PASSWORD: oracle
      APP_USER: pipeline
      APP_USER_PASSWORD: pipeline
    volumes:
      - oracle_data:/opt/oracle/oradata
```

Connect:

```bash
docker compose --profile oracle up -d oracle
docker compose exec oracle sqlplus pipeline/pipeline@//localhost:1521/XEPDB1
```

```sql
CREATE TABLE reference_devices (
    device_id  VARCHAR2(50) PRIMARY KEY,
    zone_name  VARCHAR2(50) NOT NULL,
    installed  DATE DEFAULT SYSDATE
);

INSERT INTO reference_devices (device_id, zone_name) VALUES ('dev-1', 'zone-1');
COMMIT;
SELECT * FROM reference_devices;
```

Then a Python connector using `oracledb`:

```python
import os
import oracledb

def lookup_zone_from_oracle(device_id):
    """Read reference data from the legacy Oracle source."""
    with oracledb.connect(
        user=os.environ["ORACLE_USER"],
        password=os.environ["ORACLE_PASSWORD"],
        dsn=os.environ["ORACLE_DSN"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zone_name FROM reference_devices WHERE device_id = :1",
                [device_id],
            )
            row = cur.fetchone()
            return row[0] if row else None
```

**Why bother:** the NADRA requirement says "hands on experience in managing data sources like Oracle, Postgres, Mango, Redis etc." Three of those four are covered by Milestones 2 and 3. Oracle is the fourth. One evening closes it.

**Why `profiles: ["oracle"]`:** Oracle XE is a 2 GB image. Keep it out of the default startup.

---

## MILESTONE 12 — README and architecture diagram

**This is not optional.** The README is what an interviewer actually reads.

````markdown
# Event-Driven Data Pipeline

A containerized event pipeline with at-least-once delivery, idempotent processing,
bounded retries, dead-letter handling, and Prometheus monitoring. Runs on Docker
Compose locally and on Kubernetes in production configuration.

![CI](https://github.com/USERNAME/event-pipeline/actions/workflows/ci.yml/badge.svg)

## Architecture

[ paste the diagram from Part 2.1 here ]

## Why each component

| Component | Why it is here |
|---|---|
| RabbitMQ | Decouples producer from consumer. Buffers load. Survives consumer crashes via acknowledgement. |
| Dead-letter queue | A malformed message must not block the queue forever. Three attempts, then quarantine. |
| Redis | Atomic deduplication (SET NX) and caching of reference lookups. |
| PostgreSQL | Durable source of truth, with a UNIQUE constraint on event_id as the authoritative idempotency guard. |
| FastAPI + Nginx | Operational interface behind a reverse proxy with rate limiting. |
| Prometheus | Queue depth and DLQ depth. DLQ depth is the correctness metric. |
| Kubernetes | Same pipeline, scheduled with replicas and proper liveness/readiness probes. |

## Quick start

```bash
./scripts/up.sh
```

| Service | URL |
|---|---|
| API | http://localhost:8080/health |
| RabbitMQ management | http://localhost:15672 (pipeline/pipeline) |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin/admin) |

## Design decisions

**At-least-once with idempotent consumers.** Exactly-once delivery is not achievable
across a broker and a database without distributed transactions. At-least-once plus
idempotent handlers gives correctness with far less complexity.

**Two layers of idempotency.** Redis `SET NX` is the fast path and skips the work
entirely. The PostgreSQL `UNIQUE` constraint on `event_id` is the authoritative guard
that cannot be evicted or lost. Redis is an optimisation; the database is the truth.

**Retries use a delay queue, not requeue.** `nack(requeue=True)` redelivers
immediately, which turns a poison message into a hot loop. A queue with a TTL and a
dead-letter routing key back to the work queue gives a real delay with no `sleep()`
in application code.

**Separate liveness and readiness endpoints.** `/health` never touches a dependency,
so a database blip cannot cause Kubernetes to restart a healthy container. `/ready`
does check dependencies, so traffic is only sent to pods that can serve it.

**Two brokers behind one interface.** RabbitMQ is a smart broker with dumb consumers:
it tracks acknowledgements, deletes on ack, and has native dead-lettering. Kafka is a
dumb broker with smart consumers: an append-only log with consumer-managed offsets and
retention, so streams can be replayed. The clearest practical difference is failure
handling, since Kafka has no `nack` and requires dead-lettering to be implemented as a
separate topic.

## Failure modes this handles

| Failure | Handling |
|---|---|
| Consumer crashes mid-processing | Message not acknowledged, so it is redelivered on restart |
| Consumer slower than producer | Messages buffer in the queue; queue depth is monitored |
| Malformed event | Three attempts via delay queue, then dead-lettered and recorded in the database |
| Same event delivered twice | Redis `SET NX` skips it; database `UNIQUE` constraint is the backstop |
| Broker restarts | Durable queues and persistent messages survive |
| Dependency down at startup | Every service retries its connections instead of exiting |

## Running the tests

```bash
pytest -v
ruff check .
```

## Kubernetes

```bash
kind create cluster --name pipeline
kind load docker-image event-pipeline-consumer:latest --name pipeline
kind load docker-image event-pipeline-api:latest --name pipeline
kubectl apply -f k8s/
kubectl -n pipeline get pods
```
````

---

# PART 6 — Interview preparation

## The seven questions you will be asked

**1. "Walk me through this project."**

Producer publishes to a topic exchange. RabbitMQ holds messages in a durable queue. The consumer acknowledges only after writing to PostgreSQL, which gives at-least-once delivery. Redis deduplicates atomically before the work is done, and a unique constraint in the database catches anything Redis misses. Failures retry three times through a delay queue and then go to a dead-letter queue, with the failure recorded in a table. Prometheus scrapes queue and DLQ depth. The whole thing runs on Compose locally and on Kubernetes with two consumer replicas.

**2. "Why RabbitMQ and not Kafka?"**

Different models. RabbitMQ is a smart broker with dumb consumers: it tracks acknowledgements and deletes on ack, which fits a work queue where each job runs once. Kafka is an append-only log with consumer-managed offsets and retention, which fits a stream you may need to replay. I used RabbitMQ for the queue and added Kafka behind the same interface to compare them. The sharpest difference is failure handling: RabbitMQ has native nack and a dead-letter exchange; Kafka has neither, so dead-lettering has to be implemented as a separate topic.

**3. "What happens when a message fails?"**

Attempt counter in a message header. Under three attempts, it is republished to a retry queue with a five-second TTL and a dead-letter routing key back to the work queue. That is a timer implemented as a queue, so there is no `sleep()` in code. On the fourth failure the counter is exhausted, the error is written to a `dead_letters` table, and the message is nacked with `requeue=False`, which routes it to the dead-letter exchange.

**4. "How do you handle duplicates?"**

At-least-once delivery means duplicates are normal, not exceptional. Two layers. Redis `SET key value EX ttl NX` is atomic: it returns true only when the key did not exist, so a falsy return means we have seen this event and skip it. That is the fast path. The database has a `UNIQUE` constraint on `event_id` and inserts with `ON CONFLICT DO NOTHING`, which is the authoritative guard that survives a Redis flush.

**5. "What would you do differently at ten times the volume?"**

Partition the work. In Kafka that is more partitions and more consumer group members. In RabbitMQ it is more consumers with `prefetch_count` tuning, and a sharded queue if one queue becomes the bottleneck. Batch the database writes instead of one insert per event. Move the deduplication into a Redis cluster so it scales horizontally. Replace the single PostgreSQL instance with a primary and read replicas, and send analytics queries to a replica.

**6. "How do you know it is working in production?"**

Three metrics. Queue depth tells me the consumer is keeping up. Dead-letter depth tells me correctness, because a pipeline that silently drops events looks perfectly healthy on every other metric. And event throughput per minute tells me the pipeline is actually doing work rather than sitting idle. DLQ depth is the one I would alert on.

**7. "What is the weakest part of this design?"**

The single PostgreSQL instance is a single point of failure with no replication, and the retry queue depends on RabbitMQ being up, so a broker outage stops retries entirely. I also have no authentication on the API beyond network isolation. If this were going to production I would add database replication, run RabbitMQ as a cluster, and put authentication in front of the API.

**Answering "what is weak about it" honestly is worth more than pretending it is perfect.** Every design has trade-offs, and knowing yours is the difference between someone who built a thing and someone who understands it.

---

# PART 7 — Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Consumer exits immediately on start | Broker not ready yet | The retry loop should handle it. Check `RABBITMQ_URL` includes the URL-encoded vhost `%2F` |
| Tables do not exist | `init.sql` only runs on an empty volume | `docker compose down -v` then `up` |
| `ErrImagePull` in kind | kind cannot see local images | `kind load docker-image <name> --name pipeline` |
| Queue depth climbs forever | Consumer failing silently, or too slow | Check `docker compose logs consumer` for exceptions |
| `PRECONDITION_FAILED - inequivalent arg` | Queue already exists with different arguments | Arguments are immutable. `docker compose down -v` to recreate |
| Messages vanish after broker restart | `delivery_mode` not 2, or queue not durable | Both are needed. Queue durability and message persistence are separate |
| Retry loop hammers the broker | Using `requeue=True` | Use the delay queue from Milestone 4 |
| Prometheus target DOWN | Wrong target host | Inside Compose, it is `api:8000`, not `localhost:8000` |
| Port already in use | Something else on 5432 / 8080 | Change the left side of the port mapping |

---

# PART 8 — After it works: update the resume and submit

Send me the repo URL and I will rebuild the NADRA resume. These lines get added:

**Replace the weak Redis line in skills:**
```
Databases and Data Stores: PostgreSQL, MySQL, SQL Server, MongoDB, Oracle, Redis
```

**New skills line:**
```
Messaging and Streaming: RabbitMQ (exchanges, routing keys, dead-letter queues,
delay queues), Apache Kafka (topics, partitions, consumer groups, offsets),
at-least-once delivery, idempotent consumers
```

**New project entry:**
```
Event-Driven Data Pipeline | Docker Compose, RabbitMQ, Kafka, Redis, PostgreSQL, Prometheus, Kubernetes
  github.com/ziaur390/event-pipeline
- Built a containerized event pipeline with at-least-once delivery, atomic Redis
  deduplication, and bounded retries through a delay queue into a dead-letter queue.
- Deployed to Kubernetes with liveness and readiness probes and two consumer
  replicas; automated lint, test and image builds with GitHub Actions.
- Instrumented queue depth and dead-letter depth in Prometheus.
```

**Result: 12 / 20 JD items becomes 16 / 20**, and the "in-streaming and real-time platforms" requirement goes from nothing to a real implementation you can draw on a whiteboard.

---

# PART 9 — Start here

Three commands. That is your entire first step.

```bash
mkdir event-pipeline && cd event-pipeline && git init
# create docker-compose.yml from Milestone 1.2
docker compose up -d rabbitmq
```

Then open `http://localhost:15672` (pipeline / pipeline) and confirm you can see the management UI. That is Milestone 1 half done, and it takes ten minutes.

**The one thing that can still block you regardless of this project: HEC degree attestation.** Start that today, before anything else. It takes longer than the code and it is required at interview.
