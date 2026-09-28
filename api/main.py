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
