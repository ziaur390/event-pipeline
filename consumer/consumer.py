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
