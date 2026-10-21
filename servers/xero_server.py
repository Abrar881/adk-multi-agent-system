import sys
import os
import json
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import secrets
import base64
import webbrowser
import urllib.parse
import logging
import functools
import asyncio
import random
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
from mcp.server.fastmcp import FastMCP
import httpx
from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists

# Load .env if python-dotenv is available (keeps local secrets out of code)
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    # If dotenv isn't installed, fall back to existing environment variables.
    pass

# --- CONFIGURATION ---
CLIENT_ID = os.environ.get("XERO_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("XERO_CLIENT_SECRET", "")

REDIRECT_URI = "http://localhost:8080/callback"
_HERE = os.path.dirname(os.path.abspath(__file__))
_TOKEN_DIR = os.path.realpath(os.path.join(_HERE, '..', 'tokens'))

def _safe_token_path(env_var: str, default_name: str) -> str:
    raw = os.getenv(env_var, os.path.join(_TOKEN_DIR, default_name))
    resolved = os.path.realpath(raw)
    if not resolved.startswith(_TOKEN_DIR):
        raise ValueError(f"{env_var} must resolve within {_TOKEN_DIR}, got {resolved}")
    return resolved

TOKEN_FILE = _safe_token_path('XERO_TOKEN_FILE', 'xero_token.json')

# Detect headless/Cloud Run environment (C2)
IS_CLOUD_RUN = bool(os.getenv('K_SERVICE') or os.getenv('XERO_TOKEN_JSON'))

# Tenant ID cache (M4) â€” Xero tenant rarely changes; 1-hour TTL avoids /connections on every call
_tenant_cache: dict = {"tenant_id": "", "expires_at": 0.0, "pinned_for": None}
_TENANT_CACHE_TTL = 3600  # 1 hour

_pinned_tenant_cache: dict = {"value": None, "expires_at": 0.0}
_PINNED_TENANT_CACHE_TTL = 300  # 5 minutes â€” dashboard org-pin changes apply without a redeploy
_PINNED_TENANT_RETRY_TTL = 15  # short backoff after a failed lookup, keeps last-known-good value

# Firestore idempotency locks for invoice creation
TASK_LOCKS_COLLECTION = "task_locks"

_invoice_firestore_client: "firestore.Client | None" = None
_invoice_firestore_lock = __import__("threading").Lock()

def _get_invoice_firestore() -> firestore.Client:
    """Lazy-initialised Firestore client for invoice idempotency locks."""
    global _invoice_firestore_client
    if _invoice_firestore_client is None:
        with _invoice_firestore_lock:
            if _invoice_firestore_client is None:
                _invoice_firestore_client = firestore.Client()
    return _invoice_firestore_client


def _invalidate_invoice_firestore() -> None:
    global _invoice_firestore_client
    _invoice_firestore_client = None


_auto_approve_cache: tuple[bool, float] = (False, 0.0)
_AUTO_APPROVE_CACHE_TTL = 60.0


def _is_xero_auto_approve() -> bool:
    """Read xero_invoice_auto_approve from Firestore dashboard_config/settings. Fail-safe: returns False."""
    global _auto_approve_cache
    now = time.monotonic()
    val, expiry = _auto_approve_cache
    if now < expiry:
        return val
    try:
        doc = _get_invoice_firestore().collection("dashboard_config").document("settings").get()
        enabled = doc.to_dict().get("xero_invoice_auto_approve", False) if doc.exists else False
        _auto_approve_cache = (bool(enabled), now + _AUTO_APPROVE_CACHE_TTL)
        return bool(enabled)
    except Exception:
        _invalidate_invoice_firestore()
        _auto_approve_cache = (False, now + _AUTO_APPROVE_CACHE_TTL)
        return False


# Cloud Run: inject token JSON via Secret Manager env var
XERO_TOKEN_JSON = os.getenv('XERO_TOKEN_JSON')
if XERO_TOKEN_JSON:
    import tempfile
    _fd, TOKEN_FILE = tempfile.mkstemp(suffix='.json', prefix='xero_token_')
    try:
        with os.fdopen(_fd, 'w') as _f:
            _f.write(XERO_TOKEN_JSON)
        os.chmod(TOKEN_FILE, 0o600)
    except Exception:
        try:
            os.unlink(TOKEN_FILE)
        except OSError:
            pass
        raise

# --- STRUCTURED LOGGING ---
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

mcp = FastMCP("AcmeFinance")

SERVICE_NAME = "xero_server"

# Module-level HTTP client â€” reuses TCP connections instead of opening a new
# TLS connection per call (~100-200ms saved per Xero API request).
# Lazily constructed so stale connections after Cloud Run idle are discarded.
_http_client: httpx.AsyncClient | None = None


def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(timeout=30.0)
    return _http_client


def _invalidate_http_client() -> None:
    global _http_client
    _http_client = None

# Lock to serialise token refresh â€” Xero refresh tokens are single-use;
# concurrent refreshes waste the token and break the second caller.
_token_lock = asyncio.Lock()


def _read_token_file() -> dict:
    """Sync helper for reading token from disk â€” called via asyncio.to_thread."""
    with open(TOKEN_FILE, 'r') as f:
        return json.load(f)


def _write_token_file(token: dict) -> None:
    """Sync helper for writing token to disk â€” called via asyncio.to_thread."""
    with open(TOKEN_FILE, 'w') as f:
        json.dump(token, f)

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

class XeroTransientError(Exception):
    """Raised on Xero 429/5xx so @retry can catch and back off."""


try:
    from servers.circuit_breaker import CircuitBreaker as _CircuitBreaker
except ModuleNotFoundError:
    from circuit_breaker import CircuitBreaker as _CircuitBreaker


def _xero_cb_alert(name, failures):
    try:
        from servers.alerting import alert_admin, CRITICAL
    except ModuleNotFoundError:
        from alerting import alert_admin, CRITICAL
    alert_admin(CRITICAL, f"circuit_breaker_{name}_open",
                f"Xero API circuit breaker opened after {failures} consecutive failures")


_xero_breaker = _CircuitBreaker("xero", failure_threshold=5, recovery_timeout=120.0,
                                on_open_callback=_xero_cb_alert)


def _safe_create_task(coro, name: str = "unnamed") -> "asyncio.Task":
    """Create asyncio task with exception logging to prevent silent task failures."""
    task = asyncio.create_task(coro, name=name)
    def _log_exc(t):
        if not t.cancelled() and t.exception() is not None:
            logger.error(json.dumps({
                "event": "background_task_exception",
                "task_name": name,
                "error": str(t.exception())[:300],
            }))
    task.add_done_callback(_log_exc)
    return task


# --- RETRY DECORATOR ---
def retry(exceptions=(Exception,), tries=4, delay=1, backoff=2, jitter=0.1):
    """Retry with exponential backoff + jitter to prevent thundering herd."""
    def deco(func):
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    sleep_time = _delay + random.uniform(-jitter, jitter)
                    logger.warning(json.dumps({
                        "event": "retry",
                        "service": "xero_server",
                        "wait_seconds": sleep_time,
                        "error": str(e),
                        "remaining_tries": _tries - 1
                    }))
                    time.sleep(max(0, sleep_time))
                    _tries -= 1
                    _delay *= backoff
            return func(*args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    _invalidate_http_client()
                    sleep_time = _delay + random.uniform(-jitter, jitter)
                    logger.warning(json.dumps({
                        "event": "retry",
                        "service": "xero_server",
                        "wait_seconds": sleep_time,
                        "error": str(e),
                        "remaining_tries": _tries - 1
                    }))
                    await asyncio.sleep(max(0, sleep_time))
                    _tries -= 1
                    _delay *= backoff
            return await func(*args, **kwargs)

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return deco


@retry(exceptions=(XeroTransientError, httpx.TransportError), tries=3, delay=1, backoff=2)
async def _xero_http_request(method: str, url: str, **kwargs) -> httpx.Response:
    """Single Xero HTTP call with retry on transient failures. Raises XeroTransientError on 429/5xx."""
    if not _xero_breaker.allow_request():
        raise XeroTransientError("xero circuit breaker open â€” API unavailable")
    try:
        response = await _get_http_client().request(method, url, **kwargs)
        if response.status_code in (429, 500, 502, 503, 504):
            _xero_breaker.record_failure()
            raise XeroTransientError(f"Xero transient error (HTTP {response.status_code}): {response.text[:200]}")
        _xero_breaker.record_success()
        return response
    except XeroTransientError:
        raise
    except httpx.TransportError:
        _xero_breaker.record_failure()
        raise


async def _fire_xero_auth_alert(reason: str) -> None:
    """Best-effort WhatsApp admin alert when Xero auth expires. Never raises."""
    try:
        from servers.alerting import alert_admin, CRITICAL
        await asyncio.to_thread(alert_admin, CRITICAL, "xero_auth_expired", reason[:300])
    except Exception:
        pass


# --- OAUTH STATE (C1: CSRF fix) ---
# Per-request state tokens with a 5-minute TTL. Validated and consumed on callback.
_pending_states: dict[str, float] = {}
_STATE_TTL = 300  # seconds

def _generate_oauth_state() -> str:
    """Generate a cryptographically random OAuth state token and register it."""
    state = secrets.token_urlsafe(32)
    _pending_states[state] = time.time() + _STATE_TTL
    return state

def _validate_oauth_state(state: str) -> bool:
    """Validate and consume a state token. Returns False if missing or expired."""
    now = time.time()
    # Sweep expired entries to prevent unbounded growth
    expired = [k for k, v in list(_pending_states.items()) if now > v]
    for k in expired:
        del _pending_states[k]
    if len(_pending_states) > 1000:
        _pending_states.clear()
    expiry = _pending_states.pop(state, None)
    if expiry is None:
        return False
    return now <= expiry

# --- OAUTH SERVER (local dev only, --auth flag only) ---
auth_code = None
_oauth_error = None

_OAUTH_PORT = int(os.getenv('XERO_OAUTH_PORT', '8080'))


class _ReuseAddrHTTPServer(HTTPServer):
    """HTTPServer with SO_REUSEADDR so --auth retries don't hit EADDRINUSE."""
    allow_reuse_address = True


class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code, _oauth_error
        if "/callback" in self.path:
            query = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(query)
            # Validate CSRF state (C1)
            returned_state = params.get("state", [None])[0]
            if not returned_state or not _validate_oauth_state(returned_state):
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Invalid request state. Please restart the authorization flow.")
                _oauth_error = "OAuth state validation failed"
                return
            if "code" in params:
                auth_code = params["code"][0]
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"Login Successful! You can close this window.")
            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Login Failed.")

    def log_message(self, format, *args):
        pass  # Suppress default HTTP server access logs

def get_new_token():
    """Launches browser to get a new token. Local development only. (C2)"""
    global auth_code, _oauth_error
    if IS_CLOUD_RUN:
        raise RuntimeError(
            "Browser-based OAuth is not available on Cloud Run. "
            "Re-authenticate locally with --auth and update the "
            "'xero-token-json' secret in Secret Manager."
        )

    server = _ReuseAddrHTTPServer(('localhost', _OAUTH_PORT), OAuthHandler)
    state = _generate_oauth_state()

    auth_url = (
        "https://login.xero.com/identity/connect/authorize?"
        f"response_type=code&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
        f"&scope=offline_access accounting.invoices accounting.contacts.read"
        f"&state={state}"
    )
    logger.info(json.dumps({
        "event": "oauth_start",
        "service": "xero_server",
        "message": "Opening browser for Xero login"
    }))
    webbrowser.open(auth_url)

    auth_code = None
    _oauth_error = None
    server.handle_request()
    server.server_close()

    if _oauth_error:
        raise Exception(f"OAuth error: {_oauth_error}")

    if not auth_code:
        logger.error(json.dumps({
            "event": "oauth_failed",
            "service": "xero_server",
            "message": "Failed to get auth code"
        }))
        raise Exception("Failed to get auth code.")

    token_url = "https://identity.xero.com/connect/token"
    basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {
        "Authorization": f"Basic {basic}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": REDIRECT_URI
    }

    # Sync HTTP is fine here -- this only runs in local dev, never in async context
    import requests as _req
    res = _req.post(token_url, headers=headers, data=data)
    if res.status_code != 200:
        logger.error(json.dumps({
            "event": "token_exchange_failed",
            "service": "xero_server",
            "status": res.status_code,
        }))
        raise Exception(f"Token Exchange Failed (HTTP {res.status_code})")

    token_data = res.json()
    token_data["expires_at"] = time.time() + token_data["expires_in"]

    with open(TOKEN_FILE, 'w') as f:
        json.dump(token_data, f)

    logger.info(json.dumps({
        "event": "token_acquired",
        "service": "xero_server",
        "message": "Successfully obtained new token"
    }))
    return token_data

def _persist_token_to_secret_manager(token_data: dict) -> None:
    """Write refreshed token JSON back to Secret Manager. Cloud Run only. (C6)"""
    if not IS_CLOUD_RUN:
        return
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('GCLOUD_PROJECT')
    if not project_id:
        logger.warning(json.dumps({
            "event": "token_persist_skipped",
            "service": "xero_server",
            "message": "GOOGLE_CLOUD_PROJECT not set; skipping Secret Manager writeback"
        }))
        return
    try:
        from google.cloud import secretmanager_v1
        client = secretmanager_v1.SecretManagerServiceClient()
        secret_name = f"projects/{project_id}/secrets/xero-token-json"
        payload = json.dumps(token_data).encode("utf-8")
        client.add_secret_version(
            request={"parent": secret_name, "payload": {"data": payload}}
        )
        logger.info(json.dumps({
            "event": "token_persisted",
            "service": "xero_server",
            "message": "Xero token persisted to Secret Manager"
        }))
    except Exception as e:
        logger.warning(json.dumps({
            "event": "token_persist_failed",
            "service": "xero_server",
            "error": str(e)
        }))


def _load_latest_token_from_secret_manager() -> dict | None:
    """Read the latest xero-token-json from Secret Manager. Cloud Run only.

    Used to self-heal when a dashboard OAuth reconnect wrote a fresh token after this
    instance started â€” Cloud Run env vars are resolved at startup only, so the
    in-memory token goes stale while the secret holds a good one.
    Returns the parsed dict on success, None otherwise.
    """
    if not IS_CLOUD_RUN:
        return None
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('GCLOUD_PROJECT')
    if not project_id:
        return None
    try:
        from google.cloud import secretmanager_v1
        client = secretmanager_v1.SecretManagerServiceClient()
        secret_version = f"projects/{project_id}/secrets/xero-token-json/versions/latest"
        response = client.access_secret_version(request={"name": secret_version})
        token = json.loads(response.payload.data.decode("utf-8"))
        logger.info(json.dumps({
            "event": "token_reloaded_from_secret",
            "service": "xero_server",
            "message": "Loaded latest xero-token-json from Secret Manager"
        }))
        return token
    except Exception as e:
        logger.warning(json.dumps({
            "event": "token_reload_failed",
            "service": "xero_server",
            "error": str(e)
        }))
        return None

def _get_pinned_tenant_id() -> str | None:
    """Return the Xero organisation (tenantId) pinned by the dashboard org picker.

    Checks the `XERO_TENANT_ID` env var first (local/dev + override path). On
    Cloud Run, falls back to the `xero-tenant-id` Secret Manager secret. Returns
    None if nothing is pinned â€” callers must fail loud rather than guess. Result
    is cached in-process with a short TTL so a dashboard-side pin change takes
    effect within a few minutes without a redeploy.

    A failed lookup (transient Secret Manager error, missing project id) does
    NOT overwrite a previously cached value â€” it would otherwise turn a blip
    into a real "ambiguous_tenant" outage for up to the full TTL, in exactly the
    multi-org scenario this pin exists to protect. Failures instead keep the
    last-known-good value and retry again soon.
    """
    env_value = os.getenv("XERO_TENANT_ID")
    if env_value:
        return env_value

    if time.time() < _pinned_tenant_cache["expires_at"]:
        return _pinned_tenant_cache["value"]

    if not IS_CLOUD_RUN:
        _pinned_tenant_cache["value"] = None
        _pinned_tenant_cache["expires_at"] = time.time() + _PINNED_TENANT_CACHE_TTL
        return None

    project_id = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('GCLOUD_PROJECT')
    if not project_id:
        logger.warning(json.dumps({
            "event": "pinned_tenant_load_skipped",
            "service": "xero_server",
            "message": "GOOGLE_CLOUD_PROJECT not set; cannot read xero-tenant-id secret"
        }))
        _pinned_tenant_cache["expires_at"] = time.time() + _PINNED_TENANT_RETRY_TTL
        return _pinned_tenant_cache["value"]

    try:
        from google.cloud import secretmanager_v1
        client = secretmanager_v1.SecretManagerServiceClient()
        secret_version = f"projects/{project_id}/secrets/xero-tenant-id/versions/latest"
        response = client.access_secret_version(request={"name": secret_version})
        decoded = response.payload.data.decode("utf-8").strip()
        value = decoded or None
    except Exception as e:
        logger.warning(json.dumps({
            "event": "pinned_tenant_load_failed",
            "service": "xero_server",
            "error": str(e)
        }))
        _pinned_tenant_cache["expires_at"] = time.time() + _PINNED_TENANT_RETRY_TTL
        return _pinned_tenant_cache["value"]

    _pinned_tenant_cache["value"] = value
    _pinned_tenant_cache["expires_at"] = time.time() + _PINNED_TENANT_CACHE_TTL
    return value


async def _persist_token_logged(token):
    """Async wrapper for token persistence with error logging."""
    try:
        await asyncio.to_thread(_persist_token_to_secret_manager, token)
    except Exception as exc:
        logger.error(json.dumps({
            "event": "token_persist_failed",
            "service": "xero_server",
            "error": str(exc)[:300]
        }))

async def get_xero_headers() -> dict:
    """Gets valid Xero API headers, refreshing token if needed. Fully async. (C3)"""
    async with _token_lock:
        if not os.path.exists(TOKEN_FILE):
            raise RuntimeError(
                "No Xero token file found. "
                "Re-authenticate locally with `python servers/xero_server.py --auth` "
                "and update the 'xero-token-json' secret in Secret Manager."
            )
        token = await asyncio.to_thread(_read_token_file)

        # Refresh if expired
        if time.time() > token.get("expires_at", 0):
            logger.info(json.dumps({
                "event": "token_refresh",
                "service": "xero_server",
                "message": "Refreshing Xero token"
            }))
            token_url = "https://identity.xero.com/connect/token"
            basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
            req_headers = {
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            req_data = {
                "grant_type": "refresh_token",
                "refresh_token": token["refresh_token"]
            }

            res = await _get_http_client().post(token_url, headers=req_headers, data=req_data)

            if res.status_code != 200:
                logger.warning(json.dumps({
                    "event": "refresh_failed",
                    "service": "xero_server",
                    "status": res.status_code,
                    "message": "Token refresh failed; attempting reload from Secret Manager"
                }))
                # Cloud Run env vars are resolved at instance startup â€” a dashboard
                # OAuth reconnect writes a newer secret version that this process never
                # sees. Try loading the latest version directly from Secret Manager and
                # retry the refresh once with its refresh_token before giving up.
                reloaded = _load_latest_token_from_secret_manager()
                if reloaded and reloaded.get("refresh_token") != token.get("refresh_token"):
                    # Newer token exists; adopt it and retry refresh
                    token.update(reloaded)
                    await asyncio.to_thread(_write_token_file, token)
                    if time.time() < token.get("expires_at", 0):
                        # Reloaded token is still valid â€” use it directly, skip refresh
                        logger.info(json.dumps({
                            "event": "token_reloaded_valid",
                            "service": "xero_server",
                            "message": "Reloaded token is still valid; skipping refresh"
                        }))
                    else:
                        # Reloaded token is also expired â€” retry refresh once
                        retry_data = {"grant_type": "refresh_token", "refresh_token": token["refresh_token"]}
                        retry_res = await _get_http_client().post(token_url, headers=req_headers, data=retry_data)
                        if retry_res.status_code != 200:
                            _safe_create_task(_fire_xero_auth_alert("token refresh failed after Secret Manager reload"))
                            raise RuntimeError(
                                "Xero token refresh failed after Secret Manager reload. "
                                "Re-authenticate via the dashboard Integrations tab."
                            )
                        new_data = retry_res.json()
                        token.update(new_data)
                        token["expires_at"] = time.time() + token["expires_in"]
                        await asyncio.to_thread(_write_token_file, token)
                        _safe_create_task(_persist_token_logged(token))
                        logger.info(json.dumps({
                            "event": "token_refreshed_after_reload",
                            "service": "xero_server",
                            "message": "Token refreshed successfully after Secret Manager reload"
                        }))
                else:
                    _safe_create_task(_fire_xero_auth_alert("token refresh failed and no newer Secret Manager token"))
                    raise RuntimeError(
                        "Xero token refresh failed. Re-authenticate via the dashboard "
                        "Integrations tab (Reconnect Xero) or locally with "
                        "`python servers/xero_server.py --auth`."
                    )
            else:
                new_data = res.json()
                token.update(new_data)
                token["expires_at"] = time.time() + token["expires_in"]
                await asyncio.to_thread(_write_token_file, token)
                _safe_create_task(_persist_token_logged(token))
                logger.info(json.dumps({
                    "event": "token_refreshed",
                    "service": "xero_server",
                    "message": "Token successfully refreshed"
                }))

    # Tenant ID â€” pin to a single Xero org so we never guess-pick the wrong one.
    # Resolve the pin BEFORE checking the cache so a corrected pin invalidates a
    # stale cached selection immediately instead of waiting out the 1h TTL.
    desired = await asyncio.to_thread(_get_pinned_tenant_id)
    if (
        _tenant_cache["tenant_id"]
        and time.time() < _tenant_cache["expires_at"]
        and _tenant_cache.get("pinned_for") == desired
    ):
        tenant_id = _tenant_cache["tenant_id"]
    else:
        connections_url = "https://api.xero.com/connections"
        conn_res = await _get_http_client().get(
            connections_url,
            headers={"Authorization": f"Bearer {token['access_token']}"},
        )

        if conn_res.status_code != 200:
            logger.error(json.dumps({
                "event": "connections_failed",
                "service": "xero_server",
                "status": conn_res.status_code,
                "response": conn_res.text
            }))
            raise Exception("Failed to get Xero Connections")

        connections = conn_res.json()
        if not isinstance(connections, list) or not connections:
            logger.error(json.dumps({
                "event": "connections_failed",
                "service": "xero_server",
                "status": "no_connections",
                "message": "No Xero connections returned"
            }))
            raise Exception("No Xero connections returned")

        if desired:
            match = next((c for c in connections if isinstance(c, dict) and c.get('tenantId') == desired), None)
            if not match:
                names = [c.get('tenantName') for c in connections if isinstance(c, dict)]
                logger.error(json.dumps({
                    "event": "connections_failed",
                    "service": "xero_server",
                    "status": "pinned_tenant_not_found",
                    "pinned_tenant_id": desired,
                    "available_orgs": names,
                }))
                raise Exception(f"Pinned Xero organisation {desired} is not in the connected orgs {names} â€” reconnect/reselect in the dashboard")
            tenant_id = desired
        else:
            if len(connections) > 1:
                names = [c.get('tenantName') for c in connections if isinstance(c, dict)]
                logger.error(json.dumps({
                    "event": "connections_failed",
                    "service": "xero_server",
                    "status": "ambiguous_tenant",
                    "available_orgs": names,
                }))
                raise Exception(f"Multiple Xero organisations connected ({names}) but none is pinned â€” select one in the dashboard Settings > Integrations > Xero")
            tenant_id = connections[0].get('tenantId') if isinstance(connections[0], dict) else None
            if not tenant_id:
                logger.error(json.dumps({
                    "event": "connections_failed",
                    "service": "xero_server",
                    "status": "missing_tenant_id",
                    "message": "Xero connection missing tenantId"
                }))
                raise Exception("Xero connection missing tenantId")

        _tenant_cache["tenant_id"] = tenant_id
        _tenant_cache["expires_at"] = time.time() + _TENANT_CACHE_TTL
        _tenant_cache["pinned_for"] = desired
        logger.info(json.dumps({
            "event": "tenant_id_cached",
            "service": "xero_server",
            "ttl_seconds": _TENANT_CACHE_TTL
        }))

    return {
        "Authorization": f"Bearer {token['access_token']}",
        "Xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }

# --- TOOLS ---

@mcp.tool()
async def list_unpaid_invoices() -> str:
    """Lists the top 5 unpaid invoices (AUTHORISED status)."""
    start_time = time.time()
    try:
        headers = await get_xero_headers()
        url = "https://api.xero.com/api.xro/2.0/Invoices?Statuses=AUTHORISED&where=AmountDue>0&order=Date DESC&pageSize=5"

        res = await _xero_http_request("GET", url, headers=headers)
        latency_ms = int((time.time() - start_time) * 1000)

        if res.status_code != 200:
            logger.error(json.dumps({
                "tool": "list_unpaid_invoices",
                "service": "xero_server",
                "status": "error",
                "http_status": res.status_code,
                "latency_ms": latency_ms
            }))
            return _error_response(tool="list_unpaid_invoices", error_code="api_error", message=f"Xero API Error: {res.text}", latency_ms=latency_ms, http_status=res.status_code)

        invoices = res.json().get("Invoices", [])
        if not invoices:
            logger.info(json.dumps({
                "tool": "list_unpaid_invoices",
                "service": "xero_server",
                "status": "success",
                "count": 0,
                "latency_ms": latency_ms
            }))
            return json.dumps({
                "status": "success",
                "invoices": [],
                "count": 0,
                "summary": "No unpaid invoices found.",
                "latency_ms": latency_ms
            })

        parsed_invoices = []
        summary_lines = []
        for inv in invoices[:5]:
            contact = inv.get('Contact', {}).get('Name')
            due = inv.get('AmountDue')
            due_date = inv.get('DueDateString')
            invoice_number = inv.get('InvoiceNumber')
            parsed_invoices.append({
                "invoice_number": invoice_number,
                "contact_name": contact,
                "amount_due": due,
                "due_date": due_date,
                "status": inv.get('Status')
            })
            summary_lines.append(
                f"Inv #{invoice_number} | To: {contact} | Due: Â£{due} | On: {due_date}"
            )

        logger.info(json.dumps({
            "tool": "list_unpaid_invoices",
            "service": "xero_server",
            "status": "success",
            "count": len(parsed_invoices),
            "latency_ms": latency_ms
        }))
        return json.dumps({
            "status": "success",
            "invoices": parsed_invoices,
            "count": len(parsed_invoices),
            "summary": "\n".join(summary_lines),
            "latency_ms": latency_ms
        })

    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "tool": "list_unpaid_invoices",
            "service": "xero_server",
            "status": "error",
            "error": str(e),
            "latency_ms": latency_ms
        }))
        return _error_response(tool="list_unpaid_invoices", error_code="auth_error" if isinstance(e, RuntimeError) else "api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def get_invoice_details(invoice_number: str) -> str:
    """Gets full details of a specific invoice."""
    start_time = time.time()
    try:
        headers = await get_xero_headers()
        url = f'https://api.xero.com/api.xro/2.0/Invoices?where=InvoiceNumber=="{invoice_number}"'

        res = await _xero_http_request("GET", url, headers=headers)
        latency_ms = int((time.time() - start_time) * 1000)

        if res.status_code != 200:
            logger.error(json.dumps({
                "tool": "get_invoice_details",
                "service": "xero_server",
                "status": "error",
                "http_status": res.status_code,
                "invoice_number": invoice_number,
                "latency_ms": latency_ms
            }))
            return _error_response(tool="get_invoice_details", error_code="api_error", message=f"Xero API Error: {res.text}", latency_ms=latency_ms, http_status=res.status_code)

        invoices = res.json().get("Invoices", [])

        if not invoices:
            logger.warning(json.dumps({
                "tool": "get_invoice_details",
                "service": "xero_server",
                "status": "not_found",
                "invoice_number": invoice_number,
                "latency_ms": latency_ms
            }))
            return json.dumps({
                "status": "not_found",
                "message": f"Invoice {invoice_number} not found."
            })

        inv = invoices[0]
        line_items = []
        summary_lines = []
        for line in inv.get('LineItems', []) or []:
            desc = line.get('Description')
            amt = line.get('LineAmount')
            line_items.append({"description": desc, "line_amount": amt})
            summary_lines.append(f"- {desc}: Â£{amt}")

        logger.info(json.dumps({
            "tool": "get_invoice_details",
            "service": "xero_server",
            "status": "success",
            "invoice_number": invoice_number,
            "latency_ms": latency_ms
        }))

        invoice_payload = {
            "invoice_number": inv.get('InvoiceNumber'),
            "contact_name": inv.get('Contact', {}).get('Name'),
            "total": inv.get('Total'),
            "amount_due": inv.get('AmountDue'),
            "status": inv.get('Status'),
            "line_items": line_items
        }
        summary = "\n".join([
            f"Invoice: {invoice_payload['invoice_number']}",
            f"To: {invoice_payload['contact_name']}",
            f"Total: Â£{invoice_payload['total']}",
            f"Due: Â£{invoice_payload['amount_due']}",
            f"Status: {invoice_payload['status']}",
            "Items:",
            "\n".join(summary_lines)
        ])
        return json.dumps({
            "status": "success",
            "invoice": invoice_payload,
            "summary": summary,
            "latency_ms": latency_ms
        })
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(json.dumps({
            "tool": "get_invoice_details",
            "service": "xero_server",
            "status": "error",
            "error": str(e),
            "invoice_number": invoice_number,
            "latency_ms": latency_ms
        }))
        return _error_response(tool="get_invoice_details", error_code="auth_error" if isinstance(e, RuntimeError) else "api_error", message=str(e), latency_ms=latency_ms)

@mcp.tool()
async def create_invoice(
    customer_name: str,
    customer_email: str,
    line_item_description: str,
    amount: float,
    currency: str = "GBP",
    vat_applies: bool = True,
    monday_item_id: str | None = None,
    thread_id: str | None = None,
    booking_date: str | None = None,
) -> dict:
    """Creates a new invoice in Xero with idempotency guarantees via Firestore locks."""
    t_start = time.monotonic()
    online_url = None
    sent = False
    invoice_id = None
    invoice_number = None

    # 1. Kill switch: if invoice creation is not enabled, return disabled status
    if os.getenv("INVOICE_CREATION_ENABLED") != "true":
        return {"status": "disabled"}

    # 2. Server-side validation
    # Validate amount is numeric and > 0
    try:
        amount_val = float(amount)
        if amount_val <= 0:
            return {"status": "error", "reason": "amount_must_be_positive"}
    except (TypeError, ValueError):
        return {"status": "error", "reason": "amount_not_numeric"}

    # Check amount against ceiling
    max_gbp = float(os.getenv("INVOICE_MAX_GBP", "2500"))
    if amount_val > max_gbp:
        return {"status": "hold_review", "reason": "amount_exceeds_ceiling"}

    # Validate currency is GBP
    if currency != "GBP":
        return {"status": "error", "reason": "currency_not_gbp"}

    # Validate email with basic regex
    email_pattern = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
    if not re.match(email_pattern, customer_email):
        return {"status": "error", "reason": "invalid_email_format"}

    # Validate line_item_description is non-empty and <= 500 chars
    if not line_item_description or len(line_item_description) > 500:
        return {"status": "error", "reason": "description_invalid"}

    # 3. Idempotency lock â€” FAILS CLOSED
    lock_key = None
    lock_ref = None

    if monday_item_id:
        lock_key = f"invoice:item:{monday_item_id}"
    elif thread_id and booking_date:
        lock_key = f"invoice:{thread_id}:{booking_date}"

    # Guard for None lock_key â€” idempotency fails CLOSED
    if lock_key is None:
        return {"status": "error", "reason": "missing_idempotency_key"}

    if lock_key:
        try:
            db = _get_invoice_firestore()
            lock_ref = db.collection(TASK_LOCKS_COLLECTION).document(lock_key)
            now = time.time()
            ttl_seconds = 48 * 3600  # 48 hours
            expires_at = now + ttl_seconds

            try:
                lock_ref.create({
                    "created_at": now,
                    "expires_at": expires_at,
                    "status": "in_progress"
                })
            except AlreadyExists:
                # Use a transaction to atomically check expiry and reclaim if needed.
                # Non-transactional get+set allows two concurrent processes to both see
                # an expired lock and both overwrite it, creating duplicate invoices.
                @firestore.transactional
                def _try_reclaim_lock(txn, ref, _now, _expires_at):
                    snap = ref.get(transaction=txn)
                    if not snap.exists:
                        txn.set(ref, {"created_at": _now, "expires_at": _expires_at, "status": "in_progress"})
                        return True
                    ldata = snap.to_dict() or {}
                    expired = _now > ldata.get("expires_at", 0)
                    failed = ldata.get("status") == "failed"
                    if expired or failed:
                        txn.set(ref, {"created_at": _now, "expires_at": _expires_at, "status": "in_progress"})
                        return True
                    return False, ldata  # valid lock held by another process

                _reclaim_result = _try_reclaim_lock(db.transaction(), lock_ref, now, expires_at)
                if _reclaim_result is False or (isinstance(_reclaim_result, tuple) and not _reclaim_result[0]):
                    _held_data = _reclaim_result[1] if isinstance(_reclaim_result, tuple) else {}
                    return {
                        "status": "skipped_duplicate",
                        "invoice_number": _held_data.get("invoice_number"),
                        "invoice_id": _held_data.get("invoice_id"),
                    }
        except Exception as e:
            _invalidate_invoice_firestore()
            # Any Firestore exception (connection error, auth, etc.) â€” FAIL CLOSED
            logger.warning(json.dumps({
                "event": "create_invoice",
                "phase": "lock_creation_failed",
                "service": "xero_server",
                "lock_key": lock_key,
                "error": str(e)
            }))
            return {"status": "error", "reason": "lock_unavailable"}

    # 4. Xero invoice status flag (Firestore toggle overrides env var when auto-approve is ON)
    xero_status = "AUTHORISED" if _is_xero_auto_approve() else os.getenv("XERO_INVOICE_STATUS", "DRAFT")

    # 5. POST /Invoices
    try:
        headers = await get_xero_headers()
        headers["Content-Type"] = "application/json"
        if lock_key:
            headers["Idempotency-Key"] = lock_key

        today_london = datetime.now(ZoneInfo("Europe/London")).date()
        try:
            inv_date = datetime.strptime(booking_date, "%Y-%m-%d").date() if booking_date else today_london
        except (ValueError, TypeError):
            inv_date = today_london
        due_date = max(inv_date - timedelta(days=1), today_london)

        body = {
            "Type": "ACCREC",
            "Status": xero_status,
            "Contact": {"Name": customer_name, "EmailAddress": customer_email},
            "Date": inv_date.isoformat(),
            "DueDate": due_date.isoformat(),
            "LineItems": [{
                "Description": line_item_description,
                "Quantity": 1,
                "UnitAmount": amount_val,
                "AccountCode": os.getenv("XERO_ACCOUNT_CODE", "200"),
                "TaxType": os.getenv("XERO_VAT_TAXTYPE", "OUTPUT2") if vat_applies else "NONE",
            }],
            "LineAmountTypes": "Exclusive",
            "CurrencyCode": currency,
            "Reference": monday_item_id or thread_id,
        }

        url = "https://api.xero.com/api.xro/2.0/Invoices"
        response = await _xero_http_request("POST", url, headers=headers, json={"Invoices": [body]})

        if response.status_code != 200:
            if lock_ref:
                try:
                    lock_ref.update({"status": "failed"})
                except Exception:
                    pass
            logger.error(json.dumps({
                "event": "create_invoice",
                "phase": "xero_http_error",
                "service": "xero_server",
                "status_code": response.status_code,
                "xero_response": response.text[:2000],
            }))
            return {
                "status": "error",
                "reason": "xero_error",
                "detail": response.text[:2000]
            }

        response_data = response.json()
        invoices = response_data.get("Invoices", [])

        if not invoices or not invoices[0].get("InvoiceID"):
            if lock_ref:
                try:
                    lock_ref.update({"status": "failed"})
                except Exception:
                    pass
            logger.error(json.dumps({
                "event": "create_invoice",
                "phase": "xero_validation_error",
                "service": "xero_server",
                "xero_response": str(response_data)[:2000],
            }))
            return {
                "status": "error",
                "reason": "xero_error",
                "detail": str(response_data)[:2000]
            }

        data = invoices[0]
        invoice_id = data.get("InvoiceID")
        invoice_number = data.get("InvoiceNumber")

    except Exception as e:
        if lock_ref:
            try:
                lock_ref.update({"status": "failed"})
            except Exception:
                pass
        latency_ms = int((time.monotonic() - t_start) * 1000)
        logger.error(json.dumps({
            "event": "create_invoice",
            "phase": "xero_post_failed",
            "service": "xero_server",
            "error": str(e),
            "latency_ms": latency_ms
        }))
        return {"status": "error", "reason": "xero_error", "detail": str(e)[:500]}

    # 6. Send email (ONLY if xero_status == "AUTHORISED")
    if xero_status == "AUTHORISED":
        try:
            email_url = f"https://api.xero.com/api.xro/2.0/Invoices/{invoice_id}/Email"
            email_response = await _get_http_client().post(email_url, headers=await get_xero_headers())
            sent = email_response.status_code == 200
        except Exception:
            sent = False

        # Best-effort: get online invoice URL
        try:
            online_url_endpoint = f"https://api.xero.com/api.xro/2.0/Invoices/{invoice_id}/OnlineInvoice"
            online_response = await _get_http_client().get(online_url_endpoint, headers=await get_xero_headers())
            if online_response.status_code == 200:
                online_data = online_response.json()
                online_url = online_data.get("OnlineInvoices", [{}])[0].get("OnlineInvoiceUrl")
        except Exception:
            pass

    # 7. Measure latency
    latency_ms = int((time.monotonic() - t_start) * 1000)

    # 8. Audit log
    logger.info(json.dumps({
        "event": "invoice_created",
        "source_role": os.getenv("SOURCE_ROLE", "unknown"),
        "thread_id": thread_id,
        "monday_item_id": monday_item_id,
        "amount": amount_val,
        "currency": currency,
        "invoice_id": invoice_id,
        "invoice_number": invoice_number,
        "xero_status": xero_status,
        "sent": sent,
        "latency_ms": latency_ms,
    }))

    # 9. Update Firestore lock (best-effort)
    if lock_ref:
        try:
            lock_ref.update({
                "invoice_id": invoice_id,
                "invoice_number": invoice_number,
                "sent": sent,
                "status": "done"
            })
        except Exception:
            pass

    # 10. Return success
    return {
        "status": "success",
        "invoice_id": invoice_id,
        "invoice_number": invoice_number,
        "amount": amount_val,
        "currency": currency,
        "xero_status": xero_status,
        "sent": sent,
        "online_url": online_url,
        "latency_ms": latency_ms,
    }

async def startup_health_check():
    """Validate Xero token exists and is not expired at startup (M7)."""
    try:
        if not os.path.exists(TOKEN_FILE):
            if IS_CLOUD_RUN:
                logger.critical(json.dumps({
                    "event": "startup_health_check",
                    "service": "xero_server",
                    "status": "critical",
                    "error": "No Xero token file on Cloud Run â€” set XERO_TOKEN_JSON secret"
                }))
            else:
                logger.warning(json.dumps({
                    "event": "startup_health_check",
                    "service": "xero_server",
                    "status": "warning",
                    "message": "No Xero token file found â€” run with --auth to authenticate"
                }))
            return

        with open(TOKEN_FILE, 'r') as f:
            token = json.load(f)

        is_expired = time.time() > token.get("expires_at", 0)
        has_refresh = bool(token.get("refresh_token"))

        logger.info(json.dumps({
            "event": "startup_health_check",
            "service": "xero_server",
            "status": "success",
            "token_exists": True,
            "has_refresh_token": has_refresh,
            "is_expired": is_expired,
        }))
    except Exception as e:
        logger.error(json.dumps({
            "event": "startup_health_check",
            "service": "xero_server",
            "status": "error",
            "error": str(e),
        }))


# --- MAIN ENTRY POINT ---
if __name__ == "__main__":
    if "--auth" in sys.argv:
        get_new_token()
    else:
        asyncio.run(startup_health_check())
        logger.info(json.dumps({
            "event": "server_start",
            "service": "xero_server",
            "message": "Xero MCP server starting"
        }))
        mcp.run()
