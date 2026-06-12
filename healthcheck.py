import json
import time
import threading
import http.server
import socketserver
import logging

from config import _bot_metrics
from utils import get_wib

logger = logging.getLogger(__name__)

class HealthCheckHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            uptime_s = int(time.time() - _bot_metrics.get("start_time", time.time()))
            payload = {
                "status": "ok",
                "uptime_seconds": uptime_s,
                "alerts_sent": _bot_metrics.get("alerts_sent", 0),
                "api_errors": _bot_metrics.get("api_errors", 0),
                "scanner_errors": _bot_metrics.get("scanner_errors", 0),
                "timestamp": get_wib(),
            }
            body = json.dumps(payload, indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()
    def log_message(self, *args):
        pass

def start_healthcheck_server(port=8080):
    try:
        httpd = socketserver.TCPServer(("", port), HealthCheckHandler)
        httpd.allow_reuse_address = True
        t = threading.Thread(target=httpd.serve_forever, name="healthcheck", daemon=True)
        t.start()
        logger.info(f"[HEALTHCHECK] Server on :{port} — GET /health")
    except Exception as e:
        logger.warning(f"[HEALTHCHECK] Failed: {e}")
