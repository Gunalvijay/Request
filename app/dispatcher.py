import asyncio
import logging
import time

from config import settings
from kafka_client import KafkaConsumerWrapper
from kafka_client import KafkaProducerWrapper
from medgemma_client import MedGemmaClient

from metrics import (
    MESSAGES_CONSUMED,
    MESSAGES_PROCESSED,
    MESSAGES_FAILED,
    PROCESSING_LATENCY,
    QUEUE_DEPTH,
)

logger = logging.getLogger(__name__)


class Dispatcher:

    def __init__(self):
        self.consumer = KafkaConsumerWrapper()
        self.producer = KafkaProducerWrapper()
        self.llm = MedGemmaClient()

        self.queue = asyncio.Queue(
            maxsize=settings.MAX_QUEUE_SIZE
        )

        self._worker = None
        self._running = False

    async def start(self):
        await self.consumer.start()
        await self.producer.start()

        self._running = True

        self._worker = asyncio.create_task(
            self._worker_loop(1)
        )

        logger.info("Worker started")

        await self._consume_loop()

    async def stop(self):
        self._running = False

        if self._worker:
            self._worker.cancel()

            await asyncio.gather(
                self._worker,
                return_exceptions=True
            )

        await self.consumer.stop()
        await self.producer.stop()
        await self.llm.close()

    async def _consume_loop(self):
        async for record in self.consumer:
            MESSAGES_CONSUMED.inc()

            await self.queue.put(record)

            QUEUE_DEPTH.set(
                self.queue.qsize()
            )

    async def _worker_loop(self, worker_id):
        while self._running:
            record = await self.queue.get()

            started = time.time()

            try:
                payload = record.value

                transcript_id = (
                    payload.get("transcript_id")
                    or payload.get("id")
                )

                transcript = payload.get("transcript")

                if not transcript_id:
                    raise ValueError(
                        "Missing transcript_id or id"
                    )

                if not transcript:
                    raise ValueError(
                        "Missing transcript"
                    )

                logger.info(
                    "Processing transcript=%s",
                    transcript_id
                )

                soap = await self.llm.generate_soap(
                    transcript
                )

                response = {
                    "transcript_id": transcript_id,
                    "soap": soap,
                }

                await self.producer.publish_soap(
                    key=transcript_id,
                    soap_response=response,
                )

                await self.consumer.commit()

                MESSAGES_PROCESSED.inc()

                logger.info(
                    "Processed transcript=%s",
                    transcript_id
                )

            except Exception:
                MESSAGES_FAILED.inc()

                logger.exception(
                    "processing failed"
                )

            finally:
                PROCESSING_LATENCY.observe(
                    time.time() - started
                )

                QUEUE_DEPTH.set(
                    self.queue.qsize()
                )

                self.queue.task_done()