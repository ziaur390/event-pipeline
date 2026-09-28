import json
import logging
import os
import time

import pika
import psycopg2
import redis
from psycopg2.extras import Json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s consumer %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://pipeline:pipeline@rabbitmq:5672/%2F")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://pipeline:pipeline@postgres:5432/pipeline")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

DEDUP_TTL = 3600        # remember processed event ids for one hour
CACHE_TTL = 300         # reference data cache: five minutes

EXCHANGE = "events"
QUEUE = "events.worker"
BINDING_KEY = "event.#"

DLX = "events.dlx"              # dead-letter exchange
DLQ = "events.dead"             # where permanently-failed messages end up
RETRY_QUEUE = "events.retry"    # delay queue, no consumer

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_MS = 5000           # five seconds between attempts

_db = None
_r = None


def get_db():
    """One connection, reconnected if the server drops it."""
    global _db
    if _db is None or _db.closed:
        _db = psycopg2.connect(DATABASE_URL)
        _db.autocommit = True
        log.info("connected to postgres")
    return _db


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


def handle(event):
    """Persist one event. Must be idempotent: safe to run twice."""
    # Test hook: a poison event type for demonstrating the retry/DLQ path.
    if os.getenv("TEST_MODE") and event.get("event_type") == "poison":
        raise ValueError("deliberate failure for retry testing")

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

        # ponytail: the guide marks dedup BEFORE processing, so a failed event is
        # stuck as "seen" and never retries. Fix: release the key on failure so
        # retries re-run the handler. Race window is ack-based redelivery, rare.
        first_time = get_redis().set(f"processed:{event_id}", "1", ex=DEDUP_TTL, nx=True)
        if not first_time:
            log.info("duplicate (caught by redis) %s", event_id)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        device_id = event["payload"].get("device_id", "unknown")
        zone, was_cached = lookup_zone(device_id)

        inserted = handle(event)
        if inserted == 0:
            log.info("duplicate (caught by unique constraint) %s", event_id)
        else:
            log.info(
                "stored %s type=%s device=%s zone=%s cache_hit=%s",
                event_id, event["event_type"], device_id, zone, was_cached,
            )
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as exc:
        # The event was NOT processed, so un-mark it in Redis; otherwise every
        # retry short-circuits as a "duplicate" and the DLQ path is unreachable.
        try:
            get_redis().delete(f"processed:{event['event_id']}")
        except Exception:
            pass

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
