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
