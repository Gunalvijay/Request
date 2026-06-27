from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
import os
import json

producer = None


class TranscriptRequest(BaseModel):
    transcript_id: str
    transcript: str


BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

TOPIC = "transcript-topic"


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v:
        json.dumps(v).encode()
    )

    await producer.start()

    yield

    await producer.stop()


app = FastAPI(
    title="Transcript Producer",
    lifespan=lifespan
)


@app.post("/transcripts")
async def publish(
    request: TranscriptRequest
):
    payload = request.model_dump()

    await producer.send_and_wait(
        TOPIC,
        payload
    )

    return {
        "status": "published",
        "transcript_id":
        request.transcript_id
    }