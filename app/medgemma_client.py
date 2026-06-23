"""
Thin async client around Ollama, used to call the MedGemma model.
Encapsulated so it's easy to swap the backend (e.g. vLLM, HF TGI) later
without touching the worker/dispatcher logic.
"""
import json
import logging
import asyncio
import httpx

from config import settings

logger = logging.getLogger(__name__)

SOAP_SYSTEM_PROMPT = """You are a clinical documentation assistant. Given a raw \
clinician-patient conversation transcript, generate a structured SOAP note.

Return ONLY valid JSON with this exact shape, no markdown, no commentary:
{
  "subjective": "<patient-reported symptoms, history, complaints>",
  "objective": "<observable/measurable findings: vitals, exam, labs>",
  "assessment": "<clinical assessment / diagnosis / differential>",
  "plan": "<treatment plan, medications, follow-up, referrals>"
}

If a section cannot be determined from the transcript, use "Not documented" as the value.
"""


class MedGemmaClient:
    """Async client for generating SOAP notes via MedGemma served through Ollama."""

    def __init__(self):
        self._client = httpx.AsyncClient(
            base_url=settings.OLLAMA_HOST,
            timeout=settings.LLM_TIMEOUT_SECONDS,
        )

    async def close(self):
        await self._client.aclose()

    async def generate_soap(self, transcript: str) -> dict:
        """
        Calls MedGemma (via Ollama /api/generate) and returns a parsed SOAP dict.
        Retries with exponential backoff on transient failures.
        """
        payload = {
            "model": settings.MEDGEMMA_MODEL,
            "system": SOAP_SYSTEM_PROMPT,
            "prompt": f"Transcript:\n{transcript}",
            "stream": False,
            "format": "json",
            "options": {"temperature": 0.2},
        }

        last_err = None
        for attempt in range(1, settings.LLM_MAX_RETRIES + 1):
            try:
                resp = await self._client.post("/api/generate", json=payload)
                resp.raise_for_status()
                body = resp.json()
                raw_text = body.get("response", "{}")
                soap = json.loads(raw_text)
                return self._validate_soap(soap)
            except Exception as exc:  # noqa: BLE001 - want to retry on any transient issue
                last_err = exc
                logger.warning(
                    "MedGemma call failed (attempt %s/%s): %s",
                    attempt, settings.LLM_MAX_RETRIES, exc,
                )
                if attempt < settings.LLM_MAX_RETRIES:
                    await asyncio.sleep(settings.LLM_RETRY_BACKOFF_SECONDS * attempt)

        raise RuntimeError(f"MedGemma generation failed after retries: {last_err}")

    @staticmethod
    def _validate_soap(soap: dict) -> dict:
        required = ["subjective", "objective", "assessment", "plan"]
        cleaned = {}
        for key in required:
            cleaned[key] = soap.get(key) or "Not documented"
        return cleaned
