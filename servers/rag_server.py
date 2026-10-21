import os
import sys
import asyncio
import logging
import json
import time
import functools
import random
import threading
from collections import OrderedDict
from mcp.server.fastmcp import FastMCP
from google import genai
from google.cloud import firestore
from google.cloud.firestore_v1.base_vector_query import DistanceMeasure
from google.cloud.firestore_v1.vector import Vector

# --- STRUCTURED LOGGING ---
logging.basicConfig(stream=sys.stderr, level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("google").setLevel(logging.WARNING)

# --- CONFIGURATION ---
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT') or os.environ.get('GCLOUD_PROJECT')
if not PROJECT_ID:
    raise RuntimeError("GOOGLE_CLOUD_PROJECT env var must be set")
LOCATION = "europe-west2"
EMBEDDING_MODEL = "text-embedding-004"
COLLECTION_NAME = "Acme_knowledge"

mcp = FastMCP("AcmeKnowledge")

# --- LOGGING CONSTANTS ---
SERVICE_NAME = "rag_server"
AGENT_NAME = "rag_agent"


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


logger.info(json.dumps({"event": "startup", "status": "initializing", "server": "rag"}))

# --- IN-MEMORY LRU CACHE (thread-safe, with TTL) ---
_CACHE_MAXSIZE = 50
_CACHE_TTL = 300  # 5 minutes â€” ensures dashboard-added entries appear quickly
_cache_lock = threading.Lock()
_cache = OrderedDict()


def _cache_get(key: str):
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None
        value, ts = entry
        if time.time() - ts > _CACHE_TTL:
            del _cache[key]
            return None
        _cache.move_to_end(key)
        return value


def _cache_set(key: str, value: str) -> None:
    with _cache_lock:
        _cache[key] = (value, time.time())
        _cache.move_to_end(key)
        while len(_cache) > _CACHE_MAXSIZE:
            _cache.popitem(last=False)


# 1. Initialize Clients (Global Scope)
try:
    ai_client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
    db = firestore.Client(project=PROJECT_ID)
    logger.info(json.dumps({"event": "init", "status": "success", "server": "rag"}))
except Exception as e:
    logger.error(json.dumps({"event": "init", "status": "error", "server": "rag", "error": str(e)}))
    raise


def _get_ai_client():
    global ai_client
    if ai_client is None:
        ai_client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
    return ai_client


def _get_rag_db():
    global db
    if db is None:
        db = firestore.Client(project=PROJECT_ID)
    return db


def _invalidate_ai_client() -> None:
    global ai_client
    ai_client = None


def _invalidate_rag_db() -> None:
    global db
    db = None

# 2. WARM UP (The Magic Fix)
# We force a dummy call NOW, so the authentication happens here
# and not inside the delicate tool call later.
try:
    logger.info(json.dumps({"event": "warmup", "status": "starting", "server": "rag"}))
    dummy_vec = ai_client.models.embed_content(
        model=EMBEDDING_MODEL,
        contents="warmup"
    ).embeddings[0].values
    logger.info(json.dumps({"event": "warmup", "status": "complete", "server": "rag"}))
except Exception as e:
    logger.error(json.dumps({"event": "warmup", "status": "error", "server": "rag", "error": str(e)}))

# Retry decorator with exponential backoff
def retry(exceptions, tries=4, delay=1, backoff=2, jitter=0.1):
    """Retry with Exponential Backoff + Jitter to prevent 'Thundering Herd'."""
    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    _invalidate_ai_client()
                    _invalidate_rag_db()
                    sleep_time = _delay + random.uniform(-jitter, jitter)
                    logger.warning(json.dumps({
                        "event": "retry",
                        "wait_seconds": sleep_time,
                        "error": str(e),
                        "server": "rag"
                    }))
                    time.sleep(max(0, sleep_time))
                    _tries -= 1
                    _delay *= backoff
            return func(*args, **kwargs)
        return wrapper
    return deco

@retry(exceptions=(Exception,), tries=3, delay=1, backoff=2)
def get_embedding(query_text: str) -> list:
    """Return embedding vector for query_text (extracted for testability)."""
    if not query_text or not query_text.strip():
        raise ValueError("Query cannot be empty")
    if len(query_text) > 2000:
        query_text = query_text[:2000]
    result = _get_ai_client().models.embed_content(
        model=EMBEDDING_MODEL,
        contents=query_text
    )
    return result.embeddings[0].values


# Separate retry targets: embedding retries the Vertex AI call; find_nearest
# retries only the Firestore query so a Firestore hiccup doesn't waste an
# extra embedding call per attempt.
@retry(exceptions=(Exception,), tries=3, delay=1, backoff=2)
def _find_nearest(query_vector):
    collection = _get_rag_db().collection(COLLECTION_NAME)
    return collection.find_nearest(
        vector_field="embedding",
        query_vector=Vector(query_vector),
        distance_measure=DistanceMeasure.COSINE,
        limit=3,
        distance_result_field="vector_distance"
    ).get()


def _blocking_search(query_text):
    query_vector = get_embedding(query_text)
    return _find_nearest(query_vector)

@mcp.tool()
async def consult_knowledge(query: str) -> str:
    """Search the company knowledge base with confidence scores."""
    start_time = time.time()

    # Validate query input
    if not query or not query.strip():
        return json.dumps({"error": "Query cannot be empty"})
    if len(query) > 2000:
        query = query[:2000]

    try:
        cached = _cache_get(query)
        if cached is not None:
            latency_ms = int((time.time() - start_time) * 1000)
            logger.info(json.dumps({
                "event": "tool_result",
                "tool": "consult_knowledge",
                "status": "cache_hit",
                "latency_ms": latency_ms,
                "server": "rag"
            }))
            return cached

        logger.info(json.dumps({
            "event": "tool_call",
            "tool": "consult_knowledge",
            "query": query,
            "server": "rag"
        }))
        
        # Run heavy logic in thread to avoid blocking loop
        results = await asyncio.to_thread(_blocking_search, query)

        knowledge_found = []
        for doc in results:
            data = doc.to_dict()
            # Firestore returns distance via distance_result_field in the doc data
            # Firestore COSINE distance range: 0 (identical) to 2 (opposite)
            distance = float(data.pop("vector_distance", 2.0))
            confidence = max(0.0, 1.0 - distance / 2.0)
            
            raw_text = data.get('text', '')
            source = data.get('source', 'unknown')
            # Cap each chunk to 1200 chars (~300 tokens); full chunk rarely needed,
            # and RAG is called multiple times per workflow (chase, booking).
            # EXCEPTION: quote-critical sources (pricing tables, discounts,
            # surcharges, key-collection fees) must NOT be truncated â€” a cut-off
            # price band yields a wrong customer quote, which is contractual.
            CHUNK_MAX_CHARS = 1200
            _QUOTE_CRITICAL = ("pricing", "price", "discount", "surcharge", "key_collection")
            _is_quote_critical = any(k in source.lower() for k in _QUOTE_CRITICAL)
            if _is_quote_critical or len(raw_text) <= CHUNK_MAX_CHARS:
                chunk_text = raw_text
                truncated = False
            else:
                chunk_text = raw_text[:CHUNK_MAX_CHARS]
                truncated = True
            knowledge_found.append({
                "content": chunk_text,
                "source": source,
                "distance": round(float(distance), 6),
                "confidence": round(confidence, 3),
                "truncated": truncated,
            })
        
        latency_ms = int((time.time() - start_time) * 1000)
        
        if not knowledge_found:
            logger.info(json.dumps({
                "event": "tool_result",
                "tool": "consult_knowledge",
                "status": "no_results",
                "latency_ms": latency_ms,
                "server": "rag"
            }))
            out = json.dumps({"status": "no_results", "results": []})
            _cache_set(query, out)
            return out
        
        logger.info(json.dumps({
            "event": "tool_result",
            "tool": "consult_knowledge",
            "status": "success",
            "count": len(knowledge_found),
            "latency_ms": latency_ms,
            "server": "rag"
        }))
        
        out = json.dumps({"status": "success", "results": knowledge_found})
        _cache_set(query, out)
        return out

    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "event": "tool_error",
            "tool": "consult_knowledge",
            "status": "error",
            "error": str(e),
            "latency_ms": latency_ms,
            "server": "rag"
        }))
        return _error_response(tool="consult_knowledge", error_code="api_error", message=str(e), latency_ms=latency_ms)


def startup_health_check() -> bool:
    """
    Phase 3 requirement: Startup health check.
    Verify Vertex AI and Firestore connectivity. Retries once after 5s
    to tolerate transient connectivity blips on respawn.
    """
    import time as _time
    for attempt in range(2):
        try:
            collection = _get_rag_db().collection(COLLECTION_NAME)
            list(collection.limit(1).stream())
            logger.info(json.dumps({
                "event": "health_check_passed",
                "tool": "startup_health_check",
                "status": "healthy",
                "server": "rag",
                "attempt": attempt + 1,
                "project_id": PROJECT_ID,
                "collection": COLLECTION_NAME,
                "embedding_model": EMBEDDING_MODEL
            }))
            return True
        except Exception as e:
            logger.error(json.dumps({
                "event": "health_check_failed",
                "tool": "startup_health_check",
                "status": "critical" if attempt == 1 else "retrying",
                "server": "rag",
                "attempt": attempt + 1,
                "error_type": type(e).__name__,
                "error": str(e)
            }))
            if attempt == 0:
                _time.sleep(5)
    return False


if __name__ == "__main__":
    logger.info(json.dumps({
        "event": "server_start",
        "server": "rag",
        "message": "RAG MCP server starting"
    }))
    
    # Phase 3 requirement: Startup health check
    if not startup_health_check():
        logger.error(json.dumps({
            "event": "server_startup_aborted",
            "server": "rag",
            "status": "critical",
            "message": "Health check failed. Server will not start."
        }))
        sys.exit(1)
    
    logger.info(json.dumps({
        "event": "server_ready",
        "server": "rag",
        "status": "ready"
    }))
    try:
        mcp.run()
    except Exception as e:
        logger.error(json.dumps({
            "event": "server_crash",
            "server": "rag",
            "error_type": type(e).__name__,
            "error": str(e),
            "message": "mcp.run() raised â€” server exiting"
        }))
        raise