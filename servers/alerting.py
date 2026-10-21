"""Centralized admin alert infrastructure.

Thread-safe, deduped WhatsApp admin alerts for critical system failures.
Never raises — safe for fire-and-forget callers everywhere.

Uses raw requests (not MCP) so alerts work even when MCP subprocesses are down.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import sys
import threading
import time

import requests as _requests

logger = logging.getLogger(__name__)

CRITICAL = "CRITICAL"
HIGH = "HIGH"
WARNING = "WARNING"

_ALERT_DEDUP_SECS = 1800  # 30 minutes per operation key
_TWILIO_TIMEOUT = 15

_alert_history: dict[str, float] = {}
_alert_lock = threading.Lock()


def alert_admin(severity: str, operation: str, details: str) -> None:
    """Send WhatsApp admin alert. Thread-safe, deduped per operation, never raises."""
    key = f"{severity}:{operation}"
    now = time.monotonic()

    with _alert_lock:
        if now - _alert_history.get(key, 0.0) < _ALERT_DEDUP_SECS:
            return
        _alert_history[key] = now

    logger.error(json.dumps({
        "event": "admin_alert",
        "severity": severity,
        "operation": operation,
        "details": details[:500],
    }))

    msg = f"[{severity}] {operation}: {details}"
    try:
        _send_whatsapp_alert(msg)
    except Exception as exc:
        print(
            json.dumps({
                "event": "admin_alert_delivery_failed",
                "severity": severity,
                "operation": operation,
                "error": str(exc)[:200],
            }),
            file=sys.stderr,
            flush=True,
        )


def _send_whatsapp_alert(message: str) -> None:
    """Deliver message to all admin phones via Twilio. Per-phone try/except."""
    account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN", "")
    from_phone = os.getenv("TWILIO_WHATSAPP_NUMBER", "")
    from config.dashboard_settings import get_admin_phones
    admin_phones_list = get_admin_phones()

    if not all([account_sid, auth_token, from_phone]) or not admin_phones_list:
        return

    creds = base64.b64encode(f"{account_sid}:{auth_token}".encode()).decode()
    url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
    frm = from_phone if from_phone.startswith("whatsapp:") else f"whatsapp:{from_phone}"

    for phone in admin_phones_list:
        to = phone if phone.startswith("whatsapp:") else f"whatsapp:{phone}"
        for _attempt in range(2):  # up to 1 retry on transient failure
            try:
                resp = _requests.post(
                    url,
                    headers={"Authorization": f"Basic {creds}"},
                    data={"From": frm, "To": to, "Body": message[:1600]},
                    timeout=_TWILIO_TIMEOUT,
                )
                if resp.status_code in (200, 201):
                    break
                if _attempt == 0 and resp.status_code >= 500:
                    time.sleep(2)
                    continue
                logger.warning(json.dumps({
                    "event": "admin_alert_send_failed",
                    "to_suffix": to[-4:],
                    "http_status": resp.status_code,
                    "attempt": _attempt + 1,
                }))
                break
            except Exception as exc:
                if _attempt == 0:
                    time.sleep(2)
                    continue
                logger.warning(json.dumps({
                    "event": "admin_alert_send_error",
                    "to_suffix": to[-4:],
                    "error": str(exc)[:200],
                }))
