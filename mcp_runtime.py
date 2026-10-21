"""Serialized, role-safe-caching MCP toolset wrapper.

Why this exists
----------------
Production root cause (GCP log analysis, 2026-07-09 â€” see docs/plans/multi-agent-system
crash fix): multi-agent-system shares one ``McpToolset`` instance per subprocess across
every request that needs it (e.g. ``finance_monday_ts`` is reused by every
``finance_agent`` invocation, concurrently, across every request the Cloud Run
instance is serving â€” Cloud Run's default concurrency allows many in-flight
requests per instance). GCP logs show native crashes (``double free or
corruption``, SIGSEGV/SIGABRT) inside an MCP subprocess correlating exactly
with bursts of concurrent tool calls against it, immediately followed by
``Connection closed`` / ``BrokenResourceError`` in the parent process.

``SerializedMcpToolset`` mitigates this several ways:

1. Serializes calls: only one tool call is ever in flight against a given
   subprocess at a time (an ``asyncio.Lock`` per instance == per subprocess).
   Different subprocesses still run fully in parallel â€” only same-subprocess
   calls queue, which matches a stdio pipe's inherently serial nature anyway.
2. Fails fast: each call is wrapped in ``asyncio.wait_for(call_timeout)``.
   ADK reuses ``StdioConnectionParams.timeout`` as the MCP *read* timeout for
   EVERY request, not just the initial connection (see
   ``google/adk/tools/mcp_tool/session_context.py``, where it's passed as
   ``ClientSession(read_timeout_seconds=timedelta(seconds=timeout))``) â€” so
   with the project's ``timeout=60`` (kept for rag_server's slow Vertex
   warmup), a hung/dead subprocess otherwise blocks every tool call for the
   full 60s. ``call_timeout`` here defaults to well under that.
3. Triggers recovery immediately from the failing call itself, via an
   ``on_dead`` callback â€” NOT by waiting for the exception to reach the
   outer route handler. ``ReflectAndRetryToolPlugin`` (registered on the app)
   intercepts tool exceptions at the ADK call site and only re-raises once
   the *same tool* has failed twice in one turn; the common case (one failure,
   LLM tries something else) never propagates far enough for a route-handler
   ``except`` block to see it at all. Calling ``on_dead`` here means recovery
   doesn't depend on that propagation.
4. Refuses calls on a toolset that has already been closed/replaced (see
   ``_closed``) â€” a stale reference retained by an in-flight coroutine from
   before a respawn would otherwise silently spin up a second, unmanaged
   subprocess for the same server instead of failing loudly.

It also caches the *unfiltered* tool list fetched from the subprocess (the
expensive ``ListToolsRequest`` round trip â€” production logs showed 7+ of
these per single LLM turn, one per toolset). Filtering by role
(``RoleAwareToolFilter`` / ``AgentScopedToolFilter`` in
``security/tool_predicate.py``) is re-applied fresh on every call using the
caller's ``readonly_context`` â€” that filtering is a Layer-1 RBAC control
(hides tools from customer-facing sessions) and MUST NOT be cached, or every
session after the first warmup call would inherit whatever role saw the
toolset first.
"""

from __future__ import annotations

import asyncio
import logging
import types as _types
from typing import Callable, Optional

from anyio import BrokenResourceError
from mcp.shared.exceptions import McpError

from google.adk.agents.readonly_context import ReadonlyContext
from google.adk.tools.base_tool import BaseTool
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset

logger = logging.getLogger("multi_agent_system.mcp_runtime")

# Fails fast well before the 60s MCP read timeout (StdioConnectionParams.timeout)
# so a dead/hung subprocess surfaces in seconds instead of wedging a request for a minute.
DEFAULT_CALL_TIMEOUT = 30.0

# Exceptions that indicate the MCP subprocess/pipe is genuinely dead â€” trigger on_dead.
# asyncio.TimeoutError (== built-in TimeoutError in Python 3.11+) is intentionally
# excluded: a call that exceeds call_timeout may belong to a slow-but-alive subprocess
# (e.g. rag_server Vertex warmup). Triggering a respawn would kill a healthy process
# and waste a full rag warmup re-embed. The timeout still propagates to ADK as-is.
_FAILURE_TYPES = (
    BrokenPipeError, ConnectionResetError, ConnectionAbortedError, EOFError,
    McpError, BrokenResourceError,
)


class SerializedMcpToolset(McpToolset):
    """McpToolset that serializes per-subprocess calls, times out fast, caches
    the unfiltered tool list (role filtering stays live per call), and
    triggers immediate recovery on a dead subprocess.
    """

    def __init__(
        self,
        *args,
        call_timeout: float = DEFAULT_CALL_TIMEOUT,
        on_dead: Optional[Callable[["SerializedMcpToolset"], None]] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._call_lock = asyncio.Lock()
        self._call_timeout = call_timeout
        # Fires (fire-and-forget, must not raise) the moment a call to this
        # subprocess fails in a way that suggests it's dead/hung â€” lets the
        # caller (main.py) respawn immediately instead of waiting for the
        # exception to surface past ReflectAndRetryToolPlugin (see module
        # docstring point 3 â€” that plugin often absorbs the first failure).
        self._on_dead = on_dead
        # Set once close() is called (e.g. by a respawn). A stale reference
        # to this instance held by an in-flight coroutine must fail loudly
        # rather than silently spinning up a second, unmanaged subprocess.
        self._closed = False
        # Tracks whether on_dead has been fired for the current closed state â€”
        # prevents repeated on_dead calls from the fast-path dict return.
        self._closed_notified = False
        # Set by _respawn_ts in main.py after a successful respawn of this
        # instance's subprocess. Lets in-flight tool calls that hit _closed=True
        # wait for recovery (~6-10s) instead of returning an immediate error.
        self._respawn_ready: asyncio.Event = asyncio.Event()
        # Unfiltered, run_async-wrapped tools fetched from the subprocess once.
        # Re-filtered by role on every get_tools() call â€” never cache post-filter.
        self._raw_tools_cache: Optional[list[BaseTool]] = None
        # Monotonic timestamp of the last failed get_tools() attempt (when cache
        # is still None). Throttles retries so a stuck subprocess doesn't flood
        # logs every agent turn with the same cancel-scope error.
        self._get_tools_failure_time: Optional[float] = None

        # NOTE: _call_lock is bound to whichever loop is running when first
        # awaited. This project drives every request through one persistent
        # background loop (server.py's _loop), so that's a non-issue today.
        # If ADK's RunConfig.tool_thread_pool_config is ever enabled, MCPTool
        # calls can run via asyncio.run() in a *new* loop on a worker thread
        # (google/adk/flows/llm_flows/functions.py) â€” reusing this same Lock
        # object across loops/threads would be unsafe. Not currently wired up
        # anywhere in this project; flag this comment for review if it ever is.

    async def close(self) -> None:
        # Hold _call_lock across the whole teardown so no in-flight tool call
        # is mid read/write on the subprocess stdio pipe when the transport is
        # freed (that race is a native SIGSEGV, not a Python exception). The
        # idempotency guard prevents a second close() â€” e.g. the respawn retry
        # path â€” from double-freeing the transport fd / corrupting epoll state.
        # No deadlock: on_dead only schedules the respawn (never awaits it)
        # before releasing _call_lock, and close() runs later in the respawn
        # task holding no other lock. _call_lock is bounded by _call_timeout.
        async with self._call_lock:
            if self._closed:
                return
            self._closed = True
            await super().close()

    async def get_tools(
        self, readonly_context: Optional[ReadonlyContext] = None
    ) -> list[BaseTool]:
        """Return this toolset's tools, filtered for the caller's role.

        Splits the base ``McpToolset.get_tools`` into a cached fetch (the
        network round trip to the subprocess) and a fresh per-call filter
        (the cheap, role-dependent, security-relevant part).

        The fetch is done by calling ``super().get_tools(readonly_context=None)``
        â€” both ``RoleAwareToolFilter`` and ``AgentScopedToolFilter``
        (security/tool_predicate.py) already special-case ``readonly_context is
        None`` as "expose all tools" (their documented startup-warmup
        behavior), so this reuses that existing convention to get an
        unfiltered baseline instead of reimplementing ADK's fetch/construct
        logic here. Real per-call filtering is then re-applied fresh below via
        ``self._is_tool_selected`` â€” this MUST happen every call; caching the
        post-filter result would leak whatever role saw the toolset first to
        every later session.

        Caveat: this equivalence relies on none of this project's toolsets
        using SSE/StreamableHTTP or a header_provider (both would make tool
        visibility/session identity vary with readonly_context in ways this
        cache can't see) â€” all 13 are plain stdio. If that ever changes for
        a given toolset, this class isn't safe to use for it as-is.
        """
        if self._raw_tools_cache is None:
            # Fast-path throttle (no lock): if we failed recently, return []
            # immediately. This stops the cancel-scope error from flooding logs
            # every agent turn when the subprocess is stuck. _on_dead fires on
            # the first failure so respawn is already in progress.
            _now = asyncio.get_event_loop().time()
            if (
                self._get_tools_failure_time is not None
                and (_now - self._get_tools_failure_time) < 60.0
            ):
                return []
            async with self._call_lock:
                # Double-checked: another coroutine may have filled the cache
                # while we waited for the lock (ListToolsRequest is concurrent-unsafe
                # on the subprocess stdio pipe â€” same crash class as CallToolRequest).
                if self._raw_tools_cache is None:
                    # Re-check throttle inside the lock â€” another coroutine may
                    # have just failed (updating _get_tools_failure_time) while
                    # we were waiting for the lock.
                    _now = asyncio.get_event_loop().time()
                    if (
                        self._get_tools_failure_time is not None
                        and (_now - self._get_tools_failure_time) < 60.0
                    ):
                        return []
                    try:
                        raw_tools = await super().get_tools(readonly_context=None)
                        for tool in raw_tools:
                            self._wrap_run_async(tool)
                        self._raw_tools_cache = raw_tools
                        self._get_tools_failure_time = None  # clear on success
                    except Exception as exc:
                        self._get_tools_failure_time = asyncio.get_event_loop().time()
                        logger.exception(
                            "get_tools failed; returning [] and throttling retries for 60s: %s", exc
                        )
                        if not self._closed and self._on_dead is not None:
                            try:
                                self._on_dead(self)
                            except Exception:
                                logger.exception("on_dead raised from get_tools error path")
                        return []

        return [
            tool
            for tool in self._raw_tools_cache
            if self._is_tool_selected(tool, readonly_context)
        ]

    def _wrap_run_async(self, tool: BaseTool) -> None:
        """Monkey-patch one tool instance's run_async to serialize + timeout
        + trigger recovery on failure.

        Each MCPTool is freshly constructed here (ADK doesn't cache them
        itself), so patching the instance â€” not the class â€” cannot leak
        across subprocesses or toolsets.
        """
        # Strip ADK's @retry_on_errors decorator from _run_async_impl on this
        # instance. That decorator catches BrokenPipe/McpError and retries by
        # calling create_session() â€” which spawns a new subprocess â€” without
        # holding _call_lock. If asyncio.wait_for cancels the call mid-way
        # through create_session(), native stdio resources are partially freed
        # and the next access causes SIGSEGV. Without retry, failures propagate
        # immediately to our except _FAILURE_TYPES block â†’ on_dead â†’ respawn.
        class_fn = getattr(type(tool), '_run_async_impl', None)
        if class_fn is not None:
            unwrapped = getattr(class_fn, '__wrapped__', None)
            if unwrapped is not None:
                tool._run_async_impl = _types.MethodType(unwrapped, tool)
            else:
                logger.warning(
                    "retry_on_errors on %s._run_async_impl lacks __wrapped__; "
                    "retry stripping skipped â€” crash risk remains",
                    type(tool).__name__,
                )

        original_run_async = tool.run_async
        lock = self._call_lock
        timeout = self._call_timeout

        async def _serialized_run_async(*, args, tool_context):
            if self._closed:
                # Return a graceful error dict rather than raising. Raising causes
                # reflect_retry_tool_plugin to retry (also fails â€” toolset still
                # closed) then re-raise â†’ handle_event_error â†’ silent tool drop.
                # A dict return is fed to the LLM as a tool response so it can
                # reason about the failure and continue the conversation.
                # Also fire on_dead exactly once so the respawn path triggers â€”
                # without this, _closed=True is a permanent failure state (on_dead
                # only fires on _FAILURE_TYPES exceptions, not on dict returns).
                if not self._closed_notified and self._on_dead is not None:
                    self._closed_notified = True
                    try:
                        self._on_dead(self)
                    except Exception:
                        logger.exception("on_dead callback raised from _closed fast-path")
                # Wait up to 15s for the respawn to complete rather than returning
                # an immediate error. _patch_toolset_everywhere runs before
                # _respawn_ready fires, so by the time we return the LLM's retry
                # will hit the new live toolset via the patched tool.run_async.
                try:
                    await asyncio.wait_for(self._respawn_ready.wait(), timeout=15.0)
                    return {"status": "restarting", "error": "MCP server restarted; please retry the tool call."}
                except asyncio.TimeoutError:
                    return {"error": "MCP server is restarting; please try again in a moment."}
            async with lock:
                if self._closed:
                    # Re-check under the lock: a coroutine that passed the
                    # fast-path check above can be overtaken by close() before
                    # acquiring the lock. Without this, it would call into a
                    # torn-down stdio transport (native SIGSEGV, not an
                    # exception). Return the same graceful dict as the outer check.
                    return {"error": "MCP server was restarted mid-session; tool temporarily unavailable."}
                try:
                    return await asyncio.wait_for(
                        original_run_async(args=args, tool_context=tool_context),
                        timeout=timeout,
                    )
                except _FAILURE_TYPES:
                    if self._on_dead is not None:
                        try:
                            self._on_dead(self)
                        except Exception:
                            logger.exception("on_dead callback raised")
                    raise

        tool.run_async = _serialized_run_async
