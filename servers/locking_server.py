import asyncio
import os
import sys
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from mcp.server.fastmcp import FastMCP
from google.api_core.exceptions import GoogleAPIError
from google.cloud import firestore

# --- CONFIGURATION ---
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT') or os.environ.get('GCLOUD_PROJECT')
if not PROJECT_ID:
    raise RuntimeError("GOOGLE_CLOUD_PROJECT env var must be set")
LOCK_COLLECTION = "task_locks"

# --- STRUCTURED LOGGING ---
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

mcp = FastMCP("AcmeSafety")

# --- LOGGING CONSTANTS ---
SERVICE_NAME = "locking_server"
AGENT_NAME = "locking_agent"


def _error_response(*, tool: str, error_code: str, message: str, latency_ms: int | None = None, **details) -> str:
    """Return a standardized machine-readable error payload."""
    payload = {
        "status": "error",
        "error_code": error_code,
        "message": message[:200],
        "tool": tool,
        "service": SERVICE_NAME,
    }
    if latency_ms is not None:
        payload["latency_ms"] = latency_ms
    payload.update(details)
    return json.dumps(payload)


logger.info(json.dumps({
    "event": "server_startup",
    "service": "locking_server",
    "project_id": PROJECT_ID
}))

_db = None


def _get_locking_db():
    global _db
    if _db is None:
        _db = firestore.Client(project=PROJECT_ID)
    return _db


def _log_event(
    *,
    event: str,
    tool: str | None = None,
    agent: str | None = None,
    status: str | None = None,
    latency_ms: int | None = None,
    **extra,
) -> None:
    payload = {
        "event": event,
        "service": "locking_server",
    }
    if tool is not None:
        payload["tool"] = tool
    if agent is not None:
        payload["agent"] = agent
    if status is not None:
        payload["status"] = status
    if latency_ms is not None:
        payload["latency_ms"] = latency_ms
    payload.update(extra)
    logger.info(json.dumps(payload))

def _lock_task_sync(task_id: str, owner: str = "finance_agent", ttl_seconds: int = 300) -> str:
    """
    Attempts to lock a task.
    Returns exactly "LOCKED" or "ALREADY_RUNNING".
    On exception, returns a JSON error string: {"status":"error","message":"..."}.
    """
    start = time.perf_counter()
    try:
        result_json = _acquire_lock_sync(
            resource_id=task_id, owner_agent=owner, ttl_seconds=ttl_seconds
        )
        result = json.loads(result_json)

        status = result.get("status")
        if status == "locked":
            _log_event(
                event="tool_complete",
                tool="lock_task",
                agent=owner,
                status="LOCKED",
                latency_ms=int((time.perf_counter() - start) * 1000),
                task_id=task_id,
                ttl_seconds=ttl_seconds,
            )
            return "LOCKED"
        if status == "already_locked":
            _log_event(
                event="tool_complete",
                tool="lock_task",
                agent=owner,
                status="ALREADY_RUNNING",
                latency_ms=int((time.perf_counter() - start) * 1000),
                task_id=task_id,
                ttl_seconds=ttl_seconds,
            )
            return "ALREADY_RUNNING"

        _log_event(
            event="tool_error",
            tool="lock_task",
            agent=owner,
            status="error",
            latency_ms=int((time.perf_counter() - start) * 1000),
            task_id=task_id,
            ttl_seconds=ttl_seconds,
            message=f"Unexpected lock status: {status}",
        )
        return _error_response(tool="lock_task", error_code="lock_error", message=f"Unexpected lock status: {status}", task_id=task_id)
    except GoogleAPIError as e:
        logger.error(
            json.dumps(
                {
                    "event": "lock_task_error",
                    "service": "locking_server",
                    "tool": "lock_task",
                    "agent": owner,
                    "status": "error",
                    "latency_ms": int((time.perf_counter() - start) * 1000),
                    "task_id": task_id,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }
            )
        )
        return _error_response(tool="lock_task", error_code="api_error", message=str(e), latency_ms=int((time.perf_counter() - start) * 1000))
    except Exception as e:
        logger.error(
            json.dumps(
                {
                    "event": "lock_task_error",
                    "service": "locking_server",
                    "tool": "lock_task",
                    "agent": owner,
                    "status": "error",
                    "latency_ms": int((time.perf_counter() - start) * 1000),
                    "task_id": task_id,
                    "owner": owner,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }
            )
        )
        return _error_response(tool="lock_task", error_code="api_error", message=str(e), latency_ms=int((time.perf_counter() - start) * 1000))

@mcp.tool()
async def lock_task(task_id: str, owner: str = "finance_agent", ttl_seconds: int = 300) -> str:
    """Attempts to lock a task. Returns 'LOCKED' or 'ALREADY_RUNNING'."""
    return await asyncio.to_thread(_lock_task_sync, task_id, owner, ttl_seconds)


def _acquire_lock_sync(resource_id: str, owner_agent: str, ttl_seconds: int = 300) -> str:
    """
    Attempts to acquire a lock on a resource.
    Returns JSON with status and details.
    """
    resource_id = resource_id.replace("/", "_")
    doc_ref = _get_locking_db().collection(LOCK_COLLECTION).document(resource_id)
    transaction = _get_locking_db().transaction()
    start = time.perf_counter()

    try:
        result = _attempt_lock_transaction(transaction, doc_ref, owner_agent, ttl_seconds)
        
        _log_event(
            event="tool_complete",
            tool="acquire_lock",
            agent=owner_agent,
            status=result.get("status"),
            latency_ms=int((time.perf_counter() - start) * 1000),
            resource_id=resource_id,
            ttl_seconds=ttl_seconds,
        )
        
        return json.dumps(result)
    except GoogleAPIError as e:
        logger.error(
            json.dumps(
                {
                    "event": "tool_error",
                    "service": "locking_server",
                    "tool": "acquire_lock",
                    "agent": owner_agent,
                    "status": "error",
                    "latency_ms": int((time.perf_counter() - start) * 1000),
                    "resource_id": resource_id,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }
            )
        )
        return _error_response(tool="acquire_lock", error_code="api_error", message=str(e), latency_ms=int((time.perf_counter() - start) * 1000), resource_id=resource_id)
    except Exception as e:
        logger.error(
            json.dumps(
                {
                    "event": "tool_error",
                    "service": "locking_server",
                    "tool": "acquire_lock",
                    "agent": owner_agent,
                    "status": "error",
                    "latency_ms": int((time.perf_counter() - start) * 1000),
                    "resource_id": resource_id,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }
            )
        )
        return _error_response(tool="acquire_lock", error_code="api_error", message=str(e), latency_ms=int((time.perf_counter() - start) * 1000), resource_id=resource_id)

@firestore.transactional
def _attempt_lock_transaction(transaction, doc_ref, owner, ttl_seconds):
    snapshot = doc_ref.get(transaction=transaction)
    now = datetime.now(timezone.utc)
    
    if snapshot.exists:
        data = snapshot.to_dict()
        expires_at = data.get("expires_at")
        if hasattr(expires_at, 'to_pydatetime'):
            expires_at = expires_at.to_pydatetime()
            
        if expires_at and expires_at > now:
            return {
                "status": "already_locked",
                "locked_by": data.get('owner'),
                "expires_at": expires_at.isoformat()
            }

    new_expiry = now + timedelta(seconds=ttl_seconds)
    transaction.set(doc_ref, {
        "owner": owner,
        "created_at": now,
        "expires_at": new_expiry
    })
    return {
        "status": "locked",
        "owner": owner,
        "expires_at": new_expiry.isoformat()
    }

@firestore.transactional
def _attempt_release_transaction(transaction, doc_ref, owner_agent: str):
    snapshot = doc_ref.get(transaction=transaction)
    now = datetime.now(timezone.utc)

    if not snapshot.exists:
        return {"status": "not_locked"}

    data = snapshot.to_dict()
    locked_by = data.get("owner")
    expires_at = data.get("expires_at")
    if hasattr(expires_at, "to_pydatetime"):
        expires_at = expires_at.to_pydatetime()

    is_expired = bool(expires_at and expires_at <= now)
    if is_expired or locked_by == owner_agent:
        transaction.delete(doc_ref)
        return {
            "status": "unlocked",
            "resource_id": doc_ref.id,
            "owner": owner_agent,
            "reason": "expired" if is_expired else "owner",
        }

    return {
        "status": "not_owner",
        "resource_id": doc_ref.id,
        "owner": owner_agent,
        "locked_by": locked_by,
        "expires_at": expires_at.isoformat() if expires_at else None,
    }

@mcp.tool()
async def acquire_lock(resource_id: str, owner_agent: str, ttl_seconds: int = 300) -> str:
    """Attempts to acquire a lock on a resource. Returns JSON with status and details."""
    return await asyncio.to_thread(_acquire_lock_sync, resource_id, owner_agent, ttl_seconds)


def _release_lock_sync(resource_id: str, owner_agent: str) -> str:
    """
    Releases a lock on a resource.
    Returns JSON with status.
    """
    resource_id = resource_id.replace("/", "_")
    start = time.perf_counter()
    try:
        doc_ref = _get_locking_db().collection(LOCK_COLLECTION).document(resource_id)
        transaction = _get_locking_db().transaction()
        result = _attempt_release_transaction(transaction, doc_ref, owner_agent)

        _log_event(
            event="tool_complete",
            tool="release_lock",
            agent=owner_agent,
            status=result.get("status"),
            latency_ms=int((time.perf_counter() - start) * 1000),
            resource_id=resource_id,
        )

        if result.get("status") == "not_locked":
            return json.dumps(
                {
                    "status": "not_locked",
                    "resource_id": resource_id,
                    "owner": owner_agent,
                }
            )

        return json.dumps(result)
    except GoogleAPIError as e:
        logger.error(
            json.dumps(
                {
                    "event": "tool_error",
                    "service": "locking_server",
                    "tool": "release_lock",
                    "agent": owner_agent,
                    "status": "error",
                    "latency_ms": int((time.perf_counter() - start) * 1000),
                    "resource_id": resource_id,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }
            )
        )
        return _error_response(tool="release_lock", error_code="api_error", message=str(e), latency_ms=int((time.perf_counter() - start) * 1000), resource_id=resource_id)
    except Exception as e:
        logger.error(
            json.dumps(
                {
                    "event": "tool_error",
                    "service": "locking_server",
                    "tool": "release_lock",
                    "agent": owner_agent,
                    "status": "error",
                    "latency_ms": int((time.perf_counter() - start) * 1000),
                    "resource_id": resource_id,
                    "error_type": type(e).__name__,
                    "error": str(e),
                }
            )
        )
        return _error_response(tool="release_lock", error_code="api_error", message=str(e), latency_ms=int((time.perf_counter() - start) * 1000), resource_id=resource_id)


@mcp.tool()
async def release_lock(resource_id: str, owner_agent: str) -> str:
    """Releases a lock on a resource. Returns JSON with status."""
    return await asyncio.to_thread(_release_lock_sync, resource_id, owner_agent)


def _cleanup_expired_locks_sync() -> str:
    """
    Delete all expired locks from the task_locks collection.
    Runs as the 03:00 lock_cleanup scheduled task.
    Deletes docs where expires_at is older than 24 hours ago to avoid
    removing very recently expired locks still in use by agents.
    Returns count of deleted locks.
    """
    start = time.perf_counter()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    try:
        expired = (
            _get_locking_db().collection(LOCK_COLLECTION)
            .where("expires_at", "<=", cutoff)
            .stream()
        )
        deleted = 0
        for doc in expired:
            doc.reference.delete()
            deleted += 1

        latency_ms = int((time.perf_counter() - start) * 1000)
        _log_event(
            event="tool_complete",
            tool="cleanup_expired_locks",
            status="success",
            latency_ms=latency_ms,
            deleted_count=deleted,
            cutoff=cutoff.isoformat(),
        )
        return json.dumps({
            "status": "success",
            "deleted_count": deleted,
            "cutoff": cutoff.isoformat(),
            "message": f"Deleted {deleted} expired lock(s) older than 24 hours.",
        })
    except Exception as e:
        latency_ms = int((time.perf_counter() - start) * 1000)
        logger.error(json.dumps({
            "event": "tool_error",
            "service": SERVICE_NAME,
            "tool": "cleanup_expired_locks",
            "status": "error",
            "latency_ms": latency_ms,
            "error_type": type(e).__name__,
            "error": str(e),
        }))
        return _error_response(
            tool="cleanup_expired_locks",
            error_code="api_error",
            message=str(e),
            latency_ms=latency_ms,
        )


@mcp.tool()
async def cleanup_expired_locks() -> str:
    """Delete all expired locks from the task_locks collection."""
    return await asyncio.to_thread(_cleanup_expired_locks_sync)


def startup_health_check() -> bool:
    """
    Phase 3 requirement: Startup health check.
    Verify Firestore connectivity.
    """
    try:
        # Test Firestore connectivity with a simple read
        test_ref = _get_locking_db().collection(LOCK_COLLECTION).document("HEALTH_CHECK_TEST")
        test_ref.get()
        
        _log_event(
            event="health_check_passed",
            tool="startup_health_check",
            status="healthy",
            project_id=PROJECT_ID,
            collection=LOCK_COLLECTION,
        )
        return True
    except Exception as e:
        logger.error(
            json.dumps(
                {
                    "event": "health_check_failed",
                    "service": "locking_server",
                    "tool": "startup_health_check",
                    "status": "critical",
                    "error_type": type(e).__name__,
                    "error": str(e),
                }
            )
        )
        return False


if __name__ == "__main__":
    logger.info(
        json.dumps(
            {
                "event": "server_start",
                "service": "locking_server",
                "message": "Locking MCP server starting",
            }
        )
    )
    
    # Phase 3 requirement: Startup health check
    if not startup_health_check():
        logger.error(
            json.dumps(
                {
                    "event": "server_startup_aborted",
                    "service": "locking_server",
                    "status": "critical",
                    "message": "Health check failed. Server will not start.",
                }
            )
        )
        sys.exit(1)
    
    logger.info(
        json.dumps(
            {
                "event": "server_ready",
                "service": "locking_server",
                "status": "ready",
            }
        )
    )
    mcp.run()