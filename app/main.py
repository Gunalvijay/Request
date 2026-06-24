"""
Entrypoint for the SOAP Generation Microservice pod.

Each pod:
 1. Starts a Kafka consumer (assigned a subset of partitions by the broker)
 3. Publishes generated SOAP notes back to Kafka
 4. Exposes /healthz, /ready, /metrics for k8s probes & scraping

Horizontal scaling across pods is handled entirely outside this code by the
HPA (see k8s/hpa.yaml) — this process only needs to behave well as ONE
replica among many in the same consumer group.
"""
import asyncio
import logging
import signal

from config import settings
from dispatcher import Dispatcher
from health import start_health_server, mark_ready

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


async def main():
    start_health_server()
    logger.info("%s starting up...", settings.SERVICE_NAME)

    dispatcher = Dispatcher()
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    run_task = asyncio.create_task(dispatcher.start())
    mark_ready()
    logger.info("%s ready", settings.SERVICE_NAME)

    await stop_event.wait()
    logger.info("Shutting down dispatcher...")
    await dispatcher.stop()
    run_task.cancel()
    await asyncio.gather(run_task, return_exceptions=True)
    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
