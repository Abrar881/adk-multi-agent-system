"""Thread-safe circuit breaker for external API calls.

State machine: CLOSED → OPEN → HALF_OPEN → CLOSED.

  CLOSED     Normal operation. Passes all requests. Counts consecutive failures.
  OPEN       Fast-fail. No requests reach the API. After recovery_timeout, moves
             to HALF_OPEN to allow a single probe.
  HALF_OPEN  One request is allowed through as a probe. Success → CLOSED, reset
             failure count. Failure → OPEN, restart recovery timer.

Usage in an API wrapper:
    _breaker = CircuitBreaker("monday", failure_threshold=5, recovery_timeout=60)

    def _monday_request(...):
        if not _breaker.allow_request():
            return {"error": "monday circuit open — API unavailable", "status": "circuit_open"}
        try:
            result = ...real API call...
            _breaker.record_success()
            return result
        except Exception as exc:
            _breaker.record_failure()
            raise
"""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_REGISTRY: dict[str, "CircuitBreaker"] = {}
_REGISTRY_LOCK = threading.Lock()


class CircuitBreaker:
    """Thread-safe three-state circuit breaker."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max: int = 1,
        on_open_callback: Any = None,
    ) -> None:
        self.name = name
        self._threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max = half_open_max
        self._on_open_callback = on_open_callback  # called once per OPEN transition (deduped 30 min)
        self._last_alert_at: float = 0.0
        _ALERT_DEDUP_SECS = 1800  # 30 minutes between alerts per breaker

        self._lock = threading.Lock()
        self._state = "closed"        # closed | open | half_open
        self._failures = 0
        self._opened_at: float = 0.0
        self._half_open_probes = 0    # probes currently in-flight

        with _REGISTRY_LOCK:
            _REGISTRY[name] = self

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def allow_request(self) -> bool:
        """Return True if the request should proceed."""
        with self._lock:
            if self._state == "closed":
                return True
            if self._state == "open":
                elapsed = time.monotonic() - self._opened_at
                if elapsed >= self._recovery_timeout:
                    self._transition("half_open")
                    self._half_open_probes = 1
                    return True
                return False
            # half_open
            if self._half_open_probes < self._half_open_max:
                self._half_open_probes += 1
                return True
            return False

    def record_success(self) -> None:
        """Call after a successful API response."""
        with self._lock:
            if self._state == "half_open":
                self._transition("closed")
            self._failures = 0
            self._half_open_probes = 0

    def record_failure(self) -> None:
        """Call after a failed API call."""
        with self._lock:
            self._failures += 1
            if self._state == "half_open":
                self._transition("open")
                return
            if self._state == "closed" and self._failures >= self._threshold:
                self._transition("open")

    def get_state(self) -> dict[str, Any]:
        """Return current state dict for health endpoint."""
        with self._lock:
            return {
                "state": self._state,
                "failures": self._failures,
                "threshold": self._threshold,
                "recovery_timeout_s": self._recovery_timeout,
                "seconds_until_probe": max(
                    0.0,
                    self._recovery_timeout - (time.monotonic() - self._opened_at),
                ) if self._state == "open" else 0.0,
            }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _transition(self, new_state: str) -> None:
        old_state = self._state
        self._state = new_state
        if new_state == "open":
            self._opened_at = time.monotonic()
        logger.warning(json.dumps({
            "event": "circuit_breaker_transition",
            "name": self.name,
            "from": old_state,
            "to": new_state,
            "consecutive_failures": self._failures,
        }))
        if new_state == "open" and self._on_open_callback is not None:
            now = time.monotonic()
            if now - self._last_alert_at >= 1800:  # 30-minute dedup
                self._last_alert_at = now
                try:
                    self._on_open_callback(self.name, self._failures)
                except Exception as cb_exc:
                    logger.warning(json.dumps({
                        "event": "circuit_breaker_callback_failed",
                        "name": self.name,
                        "error": str(cb_exc)[:100],
                    }))


def get_all_breaker_states() -> dict[str, dict]:
    """Return state snapshot of all registered breakers. Used by /health endpoint."""
    with _REGISTRY_LOCK:
        return {name: cb.get_state() for name, cb in _REGISTRY.items()}
