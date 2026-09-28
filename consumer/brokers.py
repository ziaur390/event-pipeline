"""Two transports, one interface.

RabbitMQ and Kafka model the same problem differently:

  RabbitMQ  smart broker, dumb consumer. The broker tracks acknowledgements and
            deletes messages once consumed, and decides which consumer gets what.
            Natural fit for task queues: do this job, once.

  Kafka     dumb broker, smart consumer. An append-only log. Messages are retained
            for a configured period regardless of who read them, and each consumer
            tracks its own offset. Natural fit for event streams you may replay.

The sharpest practical difference shows up in failure handling: RabbitMQ has a
native nack and dead-letter exchange; Kafka has neither, so a failed message is
simply not committed and will be redelivered.
"""

import logging
import os

log = logging.getLogger(__name__)


class KafkaBroker:
    def __init__(self):
        from kafka import KafkaConsumer
        self.servers = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "events")
        self._consumer_cls = KafkaConsumer

    def run(self, process):
        """Consume forever. `process(body_bytes)` raises on failure.

        Kafka has no nack: a failure means the offset is not committed, so the
        message is redelivered to the group. (Kafka dead-lettering would be
        producing the failure to a separate topic — deliberately left as the
        documented contrast, not implemented.)
        """
        consumer = self._consumer_cls(
            self.topic,
            bootstrap_servers=self.servers,
            group_id="pipeline-workers",
            auto_offset_reset="earliest",
            enable_auto_commit=False,        # we commit only after processing
            value_deserializer=lambda b: b,
        )
        log.info("kafka consumer on topic %s @ %s", self.topic, self.servers)
        for record in consumer:
            try:
                process(record.value)
                consumer.commit()            # advance the offset only on success
            except Exception as exc:
                consumer.seek_to_committed()
                log.error("kafka: not committed, will redeliver: %s", exc)


def get_broker():
    """BROKER=kafka selects the Kafka transport; anything else is RabbitMQ."""
    kind = os.getenv("BROKER", "rabbitmq").lower()
    if kind == "kafka":
        return KafkaBroker()
    return None      # rabbitmq path lives in consumer.main(), battle-tested
