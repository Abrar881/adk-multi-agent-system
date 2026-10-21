"""Shared WhatsApp message logger — writes to Firestore `whatsapp_messages` collection.

Fire-and-forget: never raises. Imported by whatsapp_server.py, server.py, and main.py.
"""
import os
import logging
import asyncio

logger = logging.getLogger(__name__)

_COLLECTION = "whatsapp_messages"
_db = None


def _get_db():
    global _db
    if _db is None:
        from google.cloud import firestore
        project = os.getenv("GOOGLE_CLOUD_PROJECT", "")
        _db = firestore.Client(project=project) if project else firestore.Client()
    return _db


def _invalidate_db() -> None:
    global _db
    _db = None


def _normalize(phone: str) -> str:
    return (phone or "").replace("whatsapp:", "").strip()


def _conv_key(phone: str) -> str:
    p = _normalize(phone).replace("+", "").replace(" ", "")
    return p[-12:] if len(p) > 12 else p


def log_whatsapp_message(
    direction: str,
    message_id: str,
    from_phone: str,
    to_phone: str,
    body: str,
    message_type: str,
    status: str,
    contact_name: str = None,
    metadata: dict = None,
) -> None:
    """Write WhatsApp message record to Firestore. Never raises."""
    try:
        from google.cloud.firestore import SERVER_TIMESTAMP
        from_clean = _normalize(from_phone)
        to_clean = _normalize(to_phone)
        contact_phone = to_clean if direction == "outbound" else from_clean
        _get_db().collection(_COLLECTION).add({
            "message_id": message_id or "",
            "direction": direction,
            "from_phone": from_clean,
            "to_phone": to_clean,
            "contact_name": contact_name,
            "contact_phone": contact_phone,
            "conversation_key": _conv_key(contact_phone),
            "body": body or "",
            "message_type": message_type,
            "status": status,
            "metadata": metadata or {},
            "timestamp": SERVER_TIMESTAMP,
        })
    except Exception as exc:
        _invalidate_db()
        logger.warning("whatsapp_logging: %s", str(exc)[:150])


async def async_log_whatsapp_message(
    direction: str,
    message_id: str,
    from_phone: str,
    to_phone: str,
    body: str,
    message_type: str,
    status: str,
    contact_name: str = None,
    metadata: dict = None,
) -> None:
    """Async wrapper — runs Firestore write in thread. Never raises."""
    await asyncio.to_thread(
        log_whatsapp_message,
        direction, message_id, from_phone, to_phone,
        body, message_type, status, contact_name, metadata,
    )
