"""
Kafka I/O layer.
- KafkaConsumerWrapper: each pod runs ONE consumer in the same group; Kafka
  rebalances partitions across pods automatically (this is the "Assigned Partition(s)"
  box in the HLD). HPA scaling pods up/down naturally triggers a rebalance.
- KafkaProducerWrapper: publishes generated SOAP notes to soap-topic.
"""
import json
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config import settings

logger = logging.getLogger(__name__)


class KafkaConsumerWrapper:
    def __init__(self):
        self.consumer = AIOKafkaConsumer(
            settings.KAFKA_INPUT_TOPIC,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=settings.KAFKA_CONSUMER_GROUP,
            auto_offset_reset=settings.KAFKA_AUTO_OFFSET_RESET,
            enable_auto_commit=False,  # we commit manually after successful processing
            max_poll_records=settings.KAFKA_MAX_POLL_RECORDS,
            session_timeout_ms=settings.KAFKA_SESSION_TIMEOUT_MS,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

    async def start(self):
        await self.consumer.start()
        logger.info(
            "Kafka consumer started: topic=%s group=%s",
            settings.KAFKA_INPUT_TOPIC, settings.KAFKA_CONSUMER_GROUP,
        )

    async def stop(self):
        await self.consumer.stop()

    async def commit(self):
        await self.consumer.commit()

    def __aiter__(self):
        return self.consumer.__aiter__()


class KafkaProducerWrapper:
    def __init__(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",  # at-least-once delivery guarantee, per HLD reliability features
        )

    async def start(self):
        await self.producer.start()
        logger.info("Kafka producer started: topic=%s", settings.KAFKA_OUTPUT_TOPIC)

    async def stop(self):
        await self.producer.stop()

    async def publish_soap(self, key: str, soap_response: dict):
        await self.producer.send_and_wait(
            settings.KAFKA_OUTPUT_TOPIC, key=key.encode("utf-8") if key else None,
            value=soap_response,
        )
