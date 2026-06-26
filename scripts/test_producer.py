"""
Quick manual test producer - simulates Team 2 publishing transcript requests.
Usage: python scripts/test_producer.py
"""
import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from aiokafka import AIOKafkaProducer  # noqa: E402

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "172.19.0.4:30092")

SAMPLE_TRANSCRIPT = (
    "Doctor: What brings you in today? "
    "Patient: I've had a sore throat and fever for three days, around 101F. "
    "Doctor: Any cough or congestion? Patient: A little dry cough, no congestion. "
    "Doctor: On exam, throat is erythematous with tonsillar exudate, no significant "
    "lymphadenopathy. Vitals: Temp 100.8F, HR 88, BP 118/76. "
    "Doctor: This looks like streptococcal pharyngitis. We'll do a rapid strep test "
    "and start amoxicillin if positive. Follow up in 5 days if not improving."
)


async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    try:
        for i in range(10, 31):
            message = {"transcript_id": f"test-{i}", "transcript": SAMPLE_TRANSCRIPT}
            await producer.send_and_wait("transcript-topic", value=message)
            print(f"Published transcript-{i}")
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
