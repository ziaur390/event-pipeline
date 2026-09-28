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
