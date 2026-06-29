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
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/kafka-service.yaml
kubectl apply -f k8s/producer-deployment.yaml
kubectl apply -f k8s/producer-service.yaml

#Expect three pods, wait untill every pod ready and running with 0 restarts
kubectl get pods -w

#If soap-generation-service-xxxx is still in init 0:1, see the live log of pulling medgemma
kubectl logs -f deployment/soap-generation-service -c pull-medgemma

#Create transcript-topic and soap-topic with required partitions and replication-factor
  #Get into kafka
  kubectl exec -it deployment/kafka -- sh

  
  #Create transcript-topic
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic transcript-topic \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1

  #Create soap-topic
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic soap-topic \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1

  #Expect _consumer_offset, transcript-topic and soap-topic
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

#Deploy Metrics-server API
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml

#Expect metrics-server-xxxx  1/1 Running
kubectl get pods -n kube-system

#if the above command fails
kubectl patch deployment metrics-server \
-n kube-system \
--type=json \
-p='[
{
"op":"add",
"path":"/spec/template/spec/containers/0/args/-",
"value":"--kubelet-insecure-tls"
}
]'

#Restart Metrics-server
kubectl rollout restart deployment metrics-server -n kube-system

#Verify Metrics API
kubectl top nodes
kubectl top pods

#Live logs of CPU, Memory usage and pod replication
kubectl get hpa -w

#Live logs of request processing
kubectl logs -f deployment/soap-generation-service \
-c soap-generation-service

#Req on transcript-topic
kubectl exec -it deployment/kafka -- sh

/opt/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic transcript-topic

#Req on soap-topic
kubectl exec -it deployment/kafka -- sh

/opt/kafka/bin/kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic soap-topic

#Test with multiple req
for i in {1..100}
do
curl -X POST http://localhost:8000/transcripts \
-H "Content-Type: application/json" \
-d "{\"transcript_id\":\"load-$i\",\"transcript\":\"patient has fever and headache\"}" &
done
 
wait

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