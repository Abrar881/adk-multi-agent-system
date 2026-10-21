"""Firestore-backed session service for ADK.

Persists chat sessions across Cloud Run instances so conversation history
survives restarts and horizontal scaling.
"""
import asyncio
import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from google.adk.events import Event
from google.adk.sessions import Session
from google.adk.sessions.base_session_service import (
    BaseSessionService,
    GetSessionConfig,
    ListSessionsResponse,
)
from google.cloud import firestore

logger = logging.getLogger(__name__)

_COLLECTION = "agent_sessions"
_SESSION_TTL_HOURS = 2
_CHAT_SESSION_TTL_DAYS = int(os.getenv("CHAT_SESSION_TTL_DAYS", "30"))
# Compacted events are pruned from the document after each compaction (see _prune_compacted_events),
# so Firestore docs stay bounded regardless of conversation length.


def _ttl_for(session_id: str) -> timedelta:
    """Return per-prefix TTL based on session_id prefix."""
    if session_id.startswith("chat-"):
        return timedelta(days=_CHAT_SESSION_TTL_DAYS)
    if session_id.startswith("sched-"):
        return timedelta(hours=8)
    return timedelta(hours=_SESSION_TTL_HOURS)


class FirestoreSessionService(BaseSessionService):

    def __init__(self, project: str | None = None):
        self._db: firestore.Client | None = None
        self._project = project
        self._lock = threading.Lock()
        self._append_lock = asyncio.Lock()  # serialise concurrent append_event calls per instance

    def _get_db(self) -> firestore.Client:
        with self._lock:
            if self._db is None:
                self._db = firestore.Client(project=self._project)
            return self._db

    def _doc_ref(self, app_name: str, user_id: str, session_id: str):
        doc_id = f"{app_name}_{user_id}_{session_id}"
        return self._get_db().collection(_COLLECTION).document(doc_id)

    async def create_session(
        self,
        *,
        app_name: str,
        user_id: str,
        state: Optional[dict[str, Any]] = None,
        session_id: Optional[str] = None,
    ) -> Session:
        import uuid

        sid = session_id or str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        session = Session(
            id=sid,
            app_name=app_name,
            user_id=user_id,
            state=state or {},
            events=[],
            last_update_time=now.timestamp(),
        )

        doc = {
            "app_name": app_name,
            "user_id": user_id,
            "session_id": sid,
            "state": json.dumps(state or {}),
            "events": "[]",
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "last_active_at": now.isoformat(),
        }
        ref = self._doc_ref(app_name, user_id, sid)
        try:
            await asyncio.to_thread(ref.set, doc)
        except Exception as exc:
            logger.error(json.dumps({
                "event": "session_create_firestore_failed",
                "session_id": sid,
                "error": str(exc)[:200],
            }))
        return session

    async def get_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        config: Optional[GetSessionConfig] = None,
    ) -> Optional[Session]:
        ref = self._doc_ref(app_name, user_id, session_id)
        try:
            snap = await asyncio.to_thread(ref.get)
        except Exception as exc:
            logger.error(json.dumps({
                "event": "session_get_firestore_failed",
                "session_id": session_id,
                "error": str(exc)[:200],
            }))
            return None
        if not snap.exists:
            return None

        data = snap.to_dict()

        last_active = datetime.fromisoformat(data["last_active_at"])
        if datetime.now(timezone.utc) - last_active > _ttl_for(session_id):
            try:
                await asyncio.to_thread(ref.delete)
            except Exception:
                pass
            return None

        state = json.loads(data.get("state", "{}"))

        events_raw = json.loads(data.get("events", "[]"))
        events = []
        for i, e in enumerate(events_raw):
            try:
                events.append(Event.model_validate(e))
            except Exception as exc:
                logger.error(json.dumps({
                    "event": "session_event_deserialize_failed",
                    "session_id": session_id,
                    "event_index": i,
                    "error": str(exc)[:200],
                    "event_keys": list(e.keys()) if isinstance(e, dict) else str(type(e)),
                }))

        if config and config.num_recent_events and len(events) > config.num_recent_events:
            events = events[-config.num_recent_events:]

        session = Session(
            id=session_id,
            app_name=app_name,
            user_id=user_id,
            state=state,
            events=events,
            last_update_time=datetime.fromisoformat(data["updated_at"]).timestamp(),
        )
        return session

    async def list_sessions(
        self, *, app_name: str, user_id: Optional[str] = None
    ) -> ListSessionsResponse:
        db = self._get_db()
        # Note: filtering by app_name + user_id + order_by requires a 3-field composite
        # index that is not yet deployed. Filter by user_id only (matches the existing
        # (user_id ASC, last_active_at DESC) index) — server.py filters chat- prefix.
        query = db.collection(_COLLECTION)
        if user_id:
            query = query.where("user_id", "==", user_id)
        query = query.order_by("last_active_at", direction=firestore.Query.DESCENDING).limit(50)

        try:
            docs = await asyncio.to_thread(lambda: list(query.stream()))
        except Exception as exc:
            logger.error(json.dumps({
                "event": "session_list_firestore_failed",
                "user_id": user_id or "all",
                "error": str(exc)[:200],
            }))
            return ListSessionsResponse(sessions=[])

        sessions = []
        for doc in docs:
            data = doc.to_dict()
            session_id = data["session_id"]

            # Filter out expired sessions (lazy delete)
            last_active = datetime.fromisoformat(data["last_active_at"])
            if datetime.now(timezone.utc) - last_active > _ttl_for(session_id):
                continue

            # Parse state and extract title metadata
            state_dict = json.loads(data.get("state", "{}"))
            stub_state = {
                "_chat_title": state_dict.get("_chat_title", ""),
                "last_active_at": data.get("last_active_at", ""),
            }

            sessions.append(Session(
                id=session_id,
                app_name=data["app_name"],
                user_id=data["user_id"],
                state=stub_state,
                events=[],
            ))
        return ListSessionsResponse(sessions=sessions)

    async def patch_state(
        self, *, app_name: str, user_id: str, session_id: str, state: dict
    ) -> None:
        """Overwrite the session state in Firestore without touching events."""
        ref = self._doc_ref(app_name, user_id, session_id)
        now = datetime.now(timezone.utc).isoformat()
        try:
            await asyncio.to_thread(ref.update, {
                "state": json.dumps(state),
                "updated_at": now,
                "last_active_at": now,
            })
        except Exception as exc:
            logger.error(json.dumps({
                "event": "session_patch_state_failed",
                "session_id": session_id,
                "error": str(exc)[:200],
            }))

    async def delete_session(
        self, *, app_name: str, user_id: str, session_id: str
    ) -> None:
        ref = self._doc_ref(app_name, user_id, session_id)
        try:
            await asyncio.to_thread(ref.delete)
        except Exception as exc:
            logger.error(json.dumps({
                "event": "session_delete_firestore_failed",
                "session_id": session_id,
                "error": str(exc)[:200],
            }))

    @staticmethod
    def _prune_compacted_events(events: list[Event]) -> list[Event]:
        """Remove raw events covered by compaction summaries.

        After ADK compaction, the summary event is appended but raw events are
        never deleted — the Firestore document grows forever. This method prunes
        raw events whose timestamps fall within a compaction range, and removes
        superseded compaction events, keeping the document bounded.
        """
        compactions = [
            e for e in events
            if e.actions and e.actions.compaction
            and e.actions.compaction.start_timestamp is not None
            and e.actions.compaction.end_timestamp is not None
            and e.actions.compaction.compacted_content is not None
        ]
        if not compactions:
            return events

        latest_end = max(c.actions.compaction.end_timestamp for c in compactions)

        # Drop compactions that are fully subsumed by a wider one
        active_compactions = []
        for c in compactions:
            c_start = c.actions.compaction.start_timestamp
            c_end = c.actions.compaction.end_timestamp
            subsumed = any(
                other is not c
                and other.actions.compaction.start_timestamp <= c_start
                and other.actions.compaction.end_timestamp >= c_end
                for other in compactions
            )
            if not subsumed:
                active_compactions.append(c)

        # Keep active compaction events + raw events after the latest compaction range
        pruned = [
            e for e in events
            if e in active_compactions
            or (not (e.actions and e.actions.compaction) and e.timestamp > latest_end)
        ]
        removed = len(events) - len(pruned)
        if removed > 0:
            logger.debug("Pruned %d compacted events from session (kept %d)", removed, len(pruned))
        return pruned

    async def append_event(self, session: Session, event: Event) -> Event:
        async with self._append_lock:
            return await self._append_event_impl(session, event)

    async def _append_event_impl(self, session: Session, event: Event) -> Event:
        event = await super().append_event(session, event)

        # Prune raw events covered by compaction summaries to keep doc under 1MB
        if event.actions and event.actions.compaction:
            session.events = self._prune_compacted_events(session.events)

        events_dicts = []
        for i, e in enumerate(session.events):
            try:
                events_dicts.append(e.model_dump(mode="json", exclude_none=True))
            except Exception as exc:
                logger.error(json.dumps({
                    "event": "session_event_serialize_failed",
                    "session_id": session.id,
                    "event_index": i,
                    "event_author": getattr(e, "author", "unknown"),
                    "error": str(exc)[:200],
                }))

        _MAX_EVENTS_BYTES = 900_000  # 900KB — stay well under Firestore's 1MB doc limit
        events_json = json.dumps(events_dicts)
        if len(events_json) > _MAX_EVENTS_BYTES:
            original_count = len(events_dicts)
            original_bytes = len(events_json)
            compaction_events = [e for e in events_dicts if e.get("actions", {}).get("compaction")]
            raw_events = [e for e in events_dicts if not e.get("actions", {}).get("compaction")]
            while raw_events and len(json.dumps(compaction_events + raw_events)) > _MAX_EVENTS_BYTES:
                raw_events.pop(0)
            events_dicts = compaction_events + raw_events
            events_json = json.dumps(events_dicts)
            logger.warning(json.dumps({
                "event": "emergency_event_prune",
                "session_id": session.id,
                "original_bytes": original_bytes,
                "pruned_bytes": len(events_json),
                "original_events": original_count,
                "pruned_events": len(events_dicts),
            }))

        now = datetime.now(timezone.utc)
        ref = self._doc_ref(session.app_name, session.user_id, session.id)
        update = {
            "events": events_json,
            "state": json.dumps(session.state),
            "updated_at": now.isoformat(),
            "last_active_at": now.isoformat(),
        }
        try:
            await asyncio.to_thread(ref.update, update)
        except Exception as exc:
            logger.error(json.dumps({
                "event": "session_append_event_firestore_failed",
                "session_id": session.id,
                "error": str(exc)[:200],
            }))
            try:
                from servers.alerting import alert_admin, HIGH
                alert_admin(HIGH, "session_persist_failed",
                            f"session {session.id[:8]}: {str(exc)[:150]}")
            except Exception:
                pass
        return event
