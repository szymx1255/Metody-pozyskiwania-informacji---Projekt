import time
import threading
import logging
from typing import Optional, Any, Dict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger("meteofetch.http_client")

class RateLimiter:
    """Prosty token-bucket rate limiter (tokens per second)."""
    def __init__(self, rate: float = 1.0):
        self.rate = float(rate)
        self.tokens = self.rate
        self.last = time.time()
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            self.tokens += (now - self.last) * self.rate
            if self.tokens > self.rate:
                self.tokens = self.rate
            self.last = now
            if self.tokens < 1.0:
                need = (1.0 - self.tokens) / self.rate
                time.sleep(need)
                self.tokens = 0.0
            else:
                self.tokens -= 1.0

class HTTPClient:
    def __init__(self, retries: int = 3, backoff_factor: float = 0.5, rate_per_sec: float = 1.0):
        self.session = requests.Session()
        retries_cfg = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE"])
        )
        adapter = HTTPAdapter(max_retries=retries_cfg)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.rate_limiter = RateLimiter(rate=rate_per_sec)

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None, timeout: int = 15) -> Dict[str, Any]:
        self.rate_limiter.wait()
        try:
            r = self.session.get(url, params=params, timeout=(5, timeout))
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException:
            LOGGER.exception("HTTP GET failed: %s", url)
            raise

    def post_json(self, url: str, json_payload: Optional[Dict[str, Any]] = None, timeout: int = 15) -> Dict[str, Any]:
        self.rate_limiter.wait()
        try:
            r = self.session.post(url, json=json_payload, timeout=(5, timeout))
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException:
            LOGGER.exception("HTTP POST failed: %s", url)
            raise

# wygodny singleton klienta
_client: Optional[HTTPClient] = None

def get_client(rate_per_sec: float = 1.0, retries: int = 3, backoff: float = 0.5) -> HTTPClient:
    global _client
    if _client is None:
        _client = HTTPClient(retries=retries, backoff_factor=backoff, rate_per_sec=rate_per_sec)
    return _client