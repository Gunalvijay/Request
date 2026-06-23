# SOAP Generation Microservice (Team 3)

Generates structured SOAP notes (Subjective, Objective, Assessment, Plan) from
clinical transcripts using the **MedGemma** LLM, served via **Ollama**.
Built around Kafka for event consumption/production, designed to scale
horizontally via Kubernetes HPA.

This is a focused implementation of the HLD: Kafka in/out, MedGemma inference,
worker-pool parallelism, and HPA. Redis, Prometheus/Grafana, and S3/MinIO are
intentionally stubbed out for now (metrics are still exposed on `/metrics` so
they can be wired in later with zero code changes).

## Architecture

```
transcript-topic (Kafka, 4 partitions)
        |
        v
  Kafka Consumer (per pod, assigned partition subset)
        |
        v
  Request Dispatcher (async) --> asyncio.Queue
        |
        v
  Worker Pool (W1..Wk)  ---->  MedGemma Client (Ollama)
        |
        v
  Kafka Producer --> soap-topic (Kafka, 4 partitions)
```

Each pod runs ONE Kafka consumer (in a shared consumer group) and a configurable
pool of async workers (`WORKER_POOL_SIZE`) that call MedGemma concurrently.
Horizontal scale-out across pods is handled by the **HPA**, not by this code.

## Project layout

```
app/
  config.py            settings from env vars
  kafka_client.py       Kafka consumer/producer wrappers (aiokafka)
  medgemma_client.py    async client calling MedGemma via Ollama
  dispatcher.py         dispatcher + worker pool (parallel processing)
  metrics.py            Prometheus metric definitions (ready, not wired to Grafana yet)
  health.py             /healthz, /ready, /metrics HTTP server
  main.py               entrypoint, wires it all together
k8s/
  deployment.yaml       Deployment + Service
  configmap.yaml        runtime config
  hpa.yaml               HorizontalPodAutoscaler (CPU/memory now, Kafka-lag via KEDA when ready)
docker-compose.yaml      local Kafka + Ollama + service for end-to-end testing
scripts/test_producer.py  publishes sample transcripts to transcript-topic
Dockerfile
requirements.txt
```

## Message contracts

**Input** (`transcript-topic`):
```json
{ "transcript_id": "abc-123", "transcript": "Doctor: ... Patient: ..." }
```

**Output** (`soap-topic`):
```json
{
  "transcript_id": "abc-123",
  "model": "medgemma:latest",
  "soap": {
    "subjective": "...",
    "objective": "...",
    "assessment": "...",
    "plan": "..."
  }
}
```

## Running locally

```bash
docker compose up -d --build

# pull the model into the ollama container once it's up
docker exec -it $(docker ps -qf "name=ollama") ollama pull medgemma

# publish a few test transcripts
python scripts/test_producer.py

# watch output
docker compose logs -f soap-generation-service
```

## Deploying to Kubernetes

```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/hpa.yaml
```

`kubectl get hpa soap-generation-hpa -w` to watch it scale pods between 2 and 10
based on CPU (70%) / memory (75%) utilization. The optional KEDA `ScaledObject`
in `hpa.yaml` (commented out) lets you switch to true Kafka-consumer-lag-based
scaling once KEDA is installed — no application code changes required.

## Reliability notes

- **At-least-once processing**: offsets are committed manually, only after the
  SOAP note is successfully published to `soap-topic`. A crash mid-processing
  re-delivers the message rather than losing it.
- **No single point of failure**: any pod can process any partition assigned
  to it; if a pod dies, Kafka rebalances its partitions to the survivors.
- **Graceful shutdown**: `SIGTERM` triggers a clean stop of the consumer,
  workers, and producer, with a `preStop` delay so in-flight requests finish
  before the pod is removed from rotation.

## Scaling knobs

| Knob | Where | Effect |
|---|---|---|
| `WORKER_POOL_SIZE` | ConfigMap | concurrent MedGemma calls per pod |
| `minReplicas`/`maxReplicas` | hpa.yaml | pod count bounds (cap at Kafka partition count for 1:1 mapping) |
| `averageUtilization` (cpu/memory) | hpa.yaml | scale-up sensitivity |
| KEDA `lagThreshold` | hpa.yaml (commented) | scale on backlog size, once enabled |


To see the soap output - docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic soap-topic --from-beginning --max-messages 5

To start a soap service - docker compose logs -f soap-generation-service

To publish sample events - python scripts/test_producer.py

To see the Kafka live logs - docker run -it `  
>> --network soap-microservice_default `
>> -p 4321:8080 `
>> -e DYNAMIC_CONFIG_ENABLED=true `
>> -e KAFKA_CLUSTERS_0_NAME=team3-local `
>> -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:29092 `
>> --name kafbat `
>> ghcr.io/kafbat/kafka-ui