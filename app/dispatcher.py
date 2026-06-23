"""
Request Dispatcher (Async) + Worker Pool (Parallel Processing), matching the HLD.

Flow inside a pod:
  Kafka Consumer -> asyncio.Queue -> [W1..Wk worker coroutines] -> MedGemma -> Producer

The dispatcher just pulls messages off Kafka and feeds an in-memory queue;
a fixed-size pool of worker coroutines drains the queue concurrently. This keeps
one pod able to process several requests in parallel (bounded by WORKER_POOL_SIZE)
without spinning up a new pod for every request — actual horizontal scaling is
handled by the HPA at the pod level (see k8s/hpa.yaml).
"""
import asyncio
import logging
import time

from config import settings
from kafka_client import KafkaConsumerWrapper, KafkaProducerWrapper
from medgemma_client import MedGemmaClient
from metrics import (
    MESSAGES_CONSUMED, MESSAGES_PROCESSED, MESSAGES_FAILED,
    PROCESSING_LATENCY, QUEUE_DEPTH,
)

logger = logging.getLogger(__name__)


class Dispatcher:
    def __init__(self):
        self.consumer = KafkaConsumerWrapper()
        self.producer = KafkaProducerWrapper()
        self.llm = MedGemmaClient()
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=settings.MAX_QUEUE_SIZE)
        self._workers: list[asyncio.Task] = []
        self._running = False

    async def start(self):
        await self.consumer.start()
        await self.producer.start()
        self._running = True

        # Spin up the worker pool (W1..Wk)
        for i in range(settings.WORKER_POOL_SIZE):
            task = asyncio.create_task(self._worker_loop(worker_id=i + 1))
            self._workers.append(task)

        logger.info("Started %d workers", settings.WORKER_POOL_SIZE)
        await self._consume_loop()

    async def stop(self):
        self._running = False
        for task in self._workers:
            task.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        await self.consumer.stop()
        await self.producer.stop()
        await self.llm.close()

    async def _consume_loop(self):
        """Pulls records from Kafka and dispatches them onto the in-process queue."""
        try:
            async for record in self.consumer:
                MESSAGES_CONSUMED.inc()
                await self.queue.put(record)
                QUEUE_DEPTH.set(self.queue.qsize())
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Consume loop crashed")

    async def _worker_loop(self, worker_id: int):
        """One worker: dequeue -> call MedGemma -> publish SOAP -> commit offset."""
        while self._running:
            try:
                record = await self.queue.get()
            except asyncio.CancelledError:
                break

            start = time.monotonic()
            try:
                payload = record.value  # already JSON-deserialized
                transcript_id = payload.get("transcript_id") or payload.get("id", "")
                transcript = payload.get("transcript", "")

                logger.info("[worker-%s] processing transcript_id=%s", worker_id, transcript_id)
                soap = await self.llm.generate_soap(transcript)

                response = {
                    "transcript_id": transcript_id,
                    "soap": soap,
                    "model": settings.MEDGEMMA_MODEL,
                }
                await self.producer.publish_soap(key=transcript_id, soap_response=response)

                # Manual commit after successful publish -> at-least-once semantics
                await self.consumer.commit()

                MESSAGES_PROCESSED.inc()
                PROCESSING_LATENCY.observe(time.monotonic() - start)

            except Exception:
                MESSAGES_FAILED.inc()
                logger.exception("[worker-%s] failed to process record", worker_id)
                # Not committing offset -> message will be redelivered (at-least-once)
            finally:
                self.queue.task_done()
                QUEUE_DEPTH.set(self.queue.qsize())
