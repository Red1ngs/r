import random
import time
import threading

class RateLimiter:
    def __init__(self, min_delay: float = 1.0, max_delay: float = 3.0):
        self._min_delay    = min_delay
        self._max_delay    = max_delay
        self._last_call    = 0.0
        self._lock         = threading.Lock()

    def wait(self) -> None:
        # Новий випадковий інтервал для кожного запиту
        interval = random.uniform(self._min_delay, self._max_delay)
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            wait = interval - elapsed

            if wait > 0:
                self._last_call = now + wait
            else:
                wait = 0.0
                self._last_call = now

        if wait > 0:
            time.sleep(wait)