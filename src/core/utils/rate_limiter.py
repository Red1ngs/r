import time
import threading

class RateLimiter:
    def __init__(self, min_interval: float = 1.0):
        self._min_interval = min_interval
        self._last_call    = 0.0
        self._lock         = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            wait = self._min_interval - elapsed
            
            if wait > 0:
                self._last_call = now + wait
            else:
                wait = 0.0
                self._last_call = now

        if wait > 0:
            time.sleep(wait)