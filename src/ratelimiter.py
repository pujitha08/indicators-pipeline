# src/ratelimiter.py
import time
from collections import deque
from src import config

# Sliding-window limiter: allow up to RATE_LIMIT_PER_MIN calls in any 60s window.
_calls_window = deque()

def acquire():
    now = time.time()
    # drop timestamps older than 60s
    while _calls_window and (now - _calls_window[0]) > 60:
        _calls_window.popleft()
    if len(_calls_window) >= config.RATE_LIMIT_PER_MIN:
        sleep_for = 60 - (now - _calls_window[0]) + 0.01
        time.sleep(max(0.0, sleep_for))
    _calls_window.append(time.time())
