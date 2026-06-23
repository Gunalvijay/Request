"""
Centralized configuration, loaded from environment variables (12-factor style).
These map directly onto the ConfigMap/Secret you'll wire up in k8s/deployment.yaml.
"""
import os


class Settings:
    # ---- Kafka ----
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "transcript-topic")
    KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "soap-topic")
    KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "soap-generation-group")
    # Each pod gets its own consumer instance; Kafka assigns it partitions automatically
    # within the consumer group -> this is what gives us the "~25% per pod" partition split.
    KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    KAFKA_MAX_POLL_RECORDS = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "10"))
    KAFKA_SESSION_TIMEOUT_MS = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000"))

    # ---- Worker pool (parallel processing within a pod) ----
    WORKER_POOL_SIZE = int(os.getenv("WORKER_POOL_SIZE", "4"))  # k workers per pod
    MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "100"))

    # ---- MedGemma / Ollama ----
    OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://ollama:11434")
    MEDGEMMA_MODEL = os.getenv("MEDGEMMA_MODEL", "medgemma:latest")
    LLM_TIMEOUT_SECONDS = int(os.getenv("LLM_TIMEOUT_SECONDS", "60"))
    LLM_MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))
    LLM_RETRY_BACKOFF_SECONDS = float(os.getenv("LLM_RETRY_BACKOFF_SECONDS", "2"))

    # ---- Service ----
    SERVICE_NAME = os.getenv("SERVICE_NAME", "soap-generation-service")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))
    METRICS_PORT = int(os.getenv("METRICS_PORT", "9100"))


settings = Settings()
