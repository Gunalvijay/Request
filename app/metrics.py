"""
Prometheus metrics. Kept minimal for now (Prometheus/Grafana wiring is out of
scope per current requirements) but exposed on /metrics so they're available
the moment observability is turned back on, and so HPA can later scale on
queue depth / Kafka lag via a custom metrics adapter (e.g. KEDA or
prometheus-adapter).
"""
from prometheus_client import Counter, Gauge, Histogram

MESSAGES_CONSUMED = Counter(
    "soap_messages_consumed_total", "Total transcript messages consumed from Kafka"
)
MESSAGES_PROCESSED = Counter(
    "soap_messages_processed_total", "Total SOAP notes successfully generated and published"
)
MESSAGES_FAILED = Counter(
    "soap_messages_failed_total", "Total messages that failed SOAP generation"
)
PROCESSING_LATENCY = Histogram(
    "soap_processing_latency_seconds", "End-to-end latency per transcript (dequeue -> publish)"
)
QUEUE_DEPTH = Gauge(
    "soap_inmemory_queue_depth", "Current depth of the in-process worker queue"
)
