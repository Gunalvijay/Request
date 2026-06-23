"""
Tiny HTTP server exposing:
  /healthz  - liveness probe
  /ready    - readiness probe
  /metrics  - Prometheus scrape endpoint (left wired up for later)

Kept dependency-free (stdlib http.server) so it doesn't compete with the
asyncio event loop driving Kafka/worker processing - run in a thread.
"""
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from config import settings

_ready = {"value": False}


def mark_ready():
    _ready["value"] = True


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence default access logs
        pass

    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
        elif self.path == "/ready":
            if _ready["value"]:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ready")
            else:
                self.send_response(503)
                self.end_headers()
        elif self.path == "/metrics":
            output = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(output)
        else:
            self.send_response(404)
            self.end_headers()


def start_health_server():
    server = HTTPServer(("0.0.0.0", settings.HEALTH_PORT), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
