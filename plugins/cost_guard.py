"""Cost guard plugin — enforces per-session tool call and transfer limits."""
import datetime
import json
import logging
import os
from google.adk.plugins import BasePlugin
import system_state as _system_state

# ---------------------------------------------------------------------------
# Model pricing: GBP per 1M UNCACHED input tokens (Vertex AI Flex PayGo, 2026-07-18, ~£0.79/$).
# Flex = 50% of Standard PayGo — identical model weights, lower serving priority (irrelevant for async).
# gemini-3.1-flash-lite = $0.125/M = £0.099/M  (Standard was $0.25/M = £0.198/M)
# gemini-3.5-flash      = $0.565/M = £0.448/M  (Standard was $1.13/M = £0.896/M, verified via BigQuery)
# Unknown models billed at the higher flash rate (conservative overestimate).
# ---------------------------------------------------------------------------
_GBP_PER_M_TOKENS: dict[str, float] = {
    "gemini-3.1-flash-lite": 0.099,
    "gemini-3.5-flash": 0.448,
    "gemini-3.7-flash": 0.448,
    "gemini-3.8-flash": 0.448,
}
_GBP_FALLBACK_RATE = 0.448  # flex flash rate — safe overestimate for unknown models

# ---------------------------------------------------------------------------
# Model pricing: GBP per 1M OUTPUT tokens (Vertex AI Flex PayGo, 2026-07-18, ~£0.79/$).
# Flex = 50% of Standard PayGo.
# gemini-3.1-flash-lite = $0.56/M  = £0.442/M  (Standard was $1.12/M = £0.883/M)
# gemini-3.5-flash      = $3.40/M  = £2.688/M  (Standard was $6.80/M = £5.375/M)
# Unknown models billed at the higher flash rate (conservative overestimate).
# ---------------------------------------------------------------------------
_GBP_PER_M_OUTPUT_TOKENS: dict[str, float] = {
    "gemini-3.1-flash-lite": 0.442,
    "gemini-3.5-flash": 2.688,
    "gemini-3.7-flash": 2.688,
    "gemini-3.8-flash": 2.688,
}
_GBP_OUTPUT_FALLBACK_RATE = 2.688  # flex flash rate — safe overestimate for unknown models

logger = logging.getLogger(__name__)

# Sentinel strings used in RuntimeError raises here and matched in main.py /
# server.py. Centralised so a rename can't silently break the friendly-message
# branch (which uses substring `in` matching to survive ADK's plugin-exception
# wrapping: "Error in plugin '<name>' during '<callback>' callback: <orig>").
ERR_MAX_ITERATIONS = "max_iterations_exceeded"
ERR_TRANSFER_LOOP = "transfer_loop_abort"
ERR_INPUT_CEILING = "input_token_ceiling_exceeded"
ERR_SPEND_CAP = "spend_cap_exceeded"


class CostGuardPlugin(BasePlugin):
    """Raises RuntimeError when per-turn tool, transfer, or input-token limits are exceeded.

    Replaces the old _before_tool callback. Fires before every tool call so the
    CostGuardPlugin must be registered BEFORE ReflectAndRetryToolPlugin to prevent
    runaway retry loops from circumventing the cap.

    max_input_tokens guards against context blow-ups that stay under the tool-call cap —
    e.g. large RAG payloads accumulating in history and being resent on every round-trip.
    Ceiling is measured on UNCACHED input only (prompt_token_count minus
    cached_content_token_count) so cheap cached re-sends from the static_instruction/
    ContextCacheConfig layer don't count against it — only genuinely fresh/billable tokens do.
    Default 2.5M uncached tokens ≈ ~£1.00 worst-case (flash) per turn; adjust via
    COST_GUARD_MAX_INPUT_TOKENS env var or by passing max_input_tokens explicitly.
    Observed production loads: 1.2-1.5M uncached for multi-item finance tasks;
    2.5M gives ~65% headroom while staying well under the session (15M) and daily
    (£9) limits. The hub/orchestrator runs uncached so multi-step conversational
    turns accumulate quickly — 2.5M is a runaway backstop, not a per-turn target.
    """

    def __init__(
        self,
        max_tool_calls: int = 50,
        max_transfers: int = 5,
        max_input_tokens: int | None = None,
        daily_gbp_limit: float | None = None,
        max_session_input_tokens: int | None = None,
    ):
        super().__init__(name="cost_guard")
        self._max_tool_calls = max_tool_calls
        self._max_transfers = max_transfers
        if max_input_tokens is None:
            max_input_tokens = int(os.environ.get("COST_GUARD_MAX_INPUT_TOKENS", 2_500_000))
        self._max_input_tokens = max_input_tokens
        self._warn_threshold = int(max_tool_calls * 0.8)
        if daily_gbp_limit is None:
            daily_gbp_limit = float(os.environ.get("COST_GUARD_DAILY_GBP", 25.0))
        self._daily_gbp_limit = daily_gbp_limit
        # Session-cumulative token ceiling — 15M uncached ≈ £6 at flash rates.
        if max_session_input_tokens is None:
            max_session_input_tokens = int(os.environ.get("COST_GUARD_MAX_SESSION_INPUT_TOKENS", 15_000_000))
        self._max_session_input_tokens = max_session_input_tokens

    async def before_tool_callback(self, *, tool, tool_args, tool_context):
        state = getattr(tool_context, "state", None)
        if state is None:
            return None

        state["tool_call_count"] = state.get("tool_call_count", 0) + 1
        count = state["tool_call_count"]

        if count > self._max_tool_calls:
            logger.warning(json.dumps({
                "event": "max_iterations_exceeded",
                "count": count,
                "uncached_input_tokens_so_far": state.get("input_token_count", 0),
            }))
            raise RuntimeError(ERR_MAX_ITERATIONS)

        if count == self._warn_threshold:
            logger.warning(json.dumps({
                "event": "tool_call_count_warning",
                "count": count,
                "limit": self._max_tool_calls,
            }))

        if tool.name == "transfer_to_agent":
            state["transfer_count"] = state.get("transfer_count", 0) + 1
            if state["transfer_count"] > self._max_transfers:
                logger.warning(json.dumps({
                    "event": ERR_TRANSFER_LOOP,
                    "count": state["transfer_count"],
                }))
                raise RuntimeError(ERR_TRANSFER_LOOP)

        return None

    async def before_run_callback(self, *, invocation_context):
        """Reset counters at the start of each user turn so limits are per-request, not per-session.

        Without this, state["tool_call_count"] accumulates across the whole session lifetime
        (persisted in Firestore) and only resets on compaction — bricking any long-lived session
        once it crosses max_tool_calls.

        Also checks the global spend cap — blocks all runs if the system has been disabled
        (either by exceeding the cap or by a billing-budget alert). Fail-open: if the
        system_state Firestore read itself errors, the run is allowed through (the billing
        backstop still protects via Layer 2).
        """
        # --- Global spend-cap gate ---
        try:
            state_doc = _system_state.get_state()
            if not state_doc.get("enabled", True):
                reason = state_doc.get("disabled_reason", "unknown")
                logger.warning(json.dumps({
                    "event": ERR_SPEND_CAP,
                    "disabled_reason": reason,
                }))
                raise RuntimeError(ERR_SPEND_CAP)
        except RuntimeError:
            raise  # re-raise the spend-cap error; don't swallow it
        except Exception as exc:
            # Firestore read failed — fail-open, log loudly
            logger.error(json.dumps({
                "event": "system_state_check_failed",
                "error": str(exc)[:200],
                "action": "fail_open_allowing_run",
            }))
            try:
                from servers.alerting import alert_admin, HIGH
                alert_admin(HIGH, "cost_guard_state_read_failed", str(exc)[:200])
            except Exception as _alert_exc:
                logger.critical(json.dumps({
                    "event": "cost_guard_alert_double_failure",
                    "original_error": str(exc)[:200],
                    "alert_error": str(_alert_exc)[:200],
                }))

        # --- Per-turn counter reset ---
        session = getattr(invocation_context, "session", None)
        if session is not None and session.state is not None:
            session.state["tool_call_count"] = 0
            session.state["transfer_count"] = 0
            session.state["input_token_count"] = 0
        return None

    async def before_model_callback(self, *, callback_context, llm_request):
        """Abort before calling the LLM if the per-turn uncached token ceiling is already met.

        after_model_callback enforces the ceiling AFTER the call (the call is already billed).
        This pre-check fires BEFORE the call and catches runs that accumulated tokens over the
        ceiling on a previous round-trip in the same turn — saving one wasted full-price call.
        It only reads state["input_token_count"] which is updated by after_model_callback, so
        the first call in a turn always passes (count == 0 from before_run_callback's reset).
        """
        state = getattr(callback_context, "state", None)
        if state is None:
            return None
        current = state.get("input_token_count", 0)
        if current >= self._max_input_tokens:
            logger.warning(json.dumps({
                "event": "input_token_ceiling_precheck",
                "uncached_input_tokens": current,
                "limit": self._max_input_tokens,
            }))
            raise RuntimeError(ERR_INPUT_CEILING)
        # Daily cost hard block — prevent new LLM calls once today's spend exceeds the limit.
        # after_model_callback sets app:cost_gbp_today; this pre-check blocks the *next* call.
        today = datetime.date.today().isoformat()
        if state.get("app:cost_date") == today:
            daily_total = state.get("app:cost_gbp_today", 0.0)
            if daily_total >= self._daily_gbp_limit:
                logger.warning(json.dumps({
                    "event": "daily_cost_limit_blocked",
                    "cost_gbp_today": round(daily_total, 4),
                    "limit_gbp": self._daily_gbp_limit,
                }))
                raise RuntimeError(ERR_SPEND_CAP)
        return None

    async def after_model_callback(self, *, callback_context, llm_response):
        """Accumulate uncached input tokens per turn; abort if ceiling exceeded.

        Counts only uncached input (prompt_token_count minus cached_content_token_count) so
        the ceiling tracks real cost — cheap cached re-sends of email-thread/RAG history from
        the static_instruction/ContextCacheConfig layer (commit c9e87c9) are excluded.
        prompt_token_count INCLUDES cached tokens per the genai SDK (types.py:7770-7772), so
        naive summation across round-trips causes false trips when caching is active.
        Token count resets each turn via before_run_callback so this is a per-request guard.
        """
        # Streaming can yield partial chunks with incomplete/absent usage_metadata before the
        # final aggregated response — skip them so cost/ceiling accounting only fires once per
        # real call. Mirrors the same guard in activity_log.py's after_model_callback. Not
        # currently reachable (this app never sets StreamingMode.SSE on a config that reaches
        # this plugin), but left in place defensively so a future streaming path can't silently
        # under- or double-count spend.
        if getattr(llm_response, "partial", False):
            return None

        state = getattr(callback_context, "state", None)
        if state is None:
            return None

        usage = getattr(llm_response, "usage_metadata", None)
        if usage is None:
            return None

        prompt_tokens = getattr(usage, "prompt_token_count", 0) or 0
        cached_tokens = getattr(usage, "cached_content_token_count", 0) or 0
        # prompt_token_count INCLUDES cached tokens — subtract to get the fresh/
        # full-rate-billable portion only. Clamp in case of reporting anomalies.
        uncached_tokens = max(0, prompt_tokens - cached_tokens)
        # candidates_token_count is only the VISIBLE completion; thoughts_token_count is a
        # separate Vertex-billed bucket (billed at the same output rate) used whenever
        # thinking_budget > 0 — true for dispatcher/finance/ops, i.e. most of this system's
        # traffic. Omitting it would still undercount real spend despite this fix.
        output_tokens = (getattr(usage, "candidates_token_count", 0) or 0) \
            + (getattr(usage, "thoughts_token_count", 0) or 0)
        state["input_token_count"] = state.get("input_token_count", 0) + uncached_tokens

        # Session-cumulative counters — accumulate across all turns (observability only for llm_call_count).
        session_calls = state.get("session:llm_call_count", 0) + 1
        state["session:llm_call_count"] = session_calls
        session_input = state.get("session:uncached_input_total", 0) + uncached_tokens
        state["session:uncached_input_total"] = session_input

        logger.debug(json.dumps({
            "event": "input_token_accumulate",
            "prompt_tokens": prompt_tokens,
            "cached_tokens": cached_tokens,
            "uncached_tokens": uncached_tokens,
            "running_uncached": state["input_token_count"],
            "session_llm_calls": session_calls,
            "session_uncached_total": session_input,
        }))

        if state["input_token_count"] > self._max_input_tokens:
            logger.warning(json.dumps({
                "event": "input_token_ceiling_exceeded",
                "uncached_input_tokens": state["input_token_count"],
                "limit": self._max_input_tokens,
            }))
            raise RuntimeError(ERR_INPUT_CEILING)

        if session_input > self._max_session_input_tokens:
            logger.warning(json.dumps({
                "event": "session_input_ceiling_exceeded",
                "session_uncached_input": session_input,
                "limit": self._max_session_input_tokens,
            }))
            raise RuntimeError(ERR_INPUT_CEILING)

        # --- Daily cost meter (approximate; concurrent sessions may undercount slightly) ---
        # Accumulates in app-level session state (shared across all sessions via app: prefix).
        # Resets at UTC midnight. Used for budget visibility + a log-based alert.
        cost_gbp = (uncached_tokens / 1_000_000) * self._gbp_rate(llm_response) \
            + (output_tokens / 1_000_000) * self._gbp_output_rate(llm_response)
        # Emitted at INFO (not DEBUG) WITH per-agent attribution so per-agent cache-hit
        # and uncached-cost are measurable directly from Cloud Logging. This is the
        # instrumentation the finance/ops static-instruction padding decision needs
        # (audit H5): measure the real per-call cost + cache hit before trimming the
        # knowledge-file padding — the sub-agent padding revert (30b19da) showed the
        # cache-floor math can go either way, so decide from data, not assumption.
        logger.info(json.dumps({
            "event": "token_cost",
            "agent": getattr(callback_context, "agent_name", "unknown"),
            "prompt_tokens": prompt_tokens,
            "cached_tokens": cached_tokens,
            "uncached_tokens": uncached_tokens,
            "output_tokens": output_tokens,
            "cost_gbp": round(cost_gbp, 6),
        }))

        # Persist to global Firestore counters (atomic Increment — concurrency-safe).
        # Latches system off if cumulative spend crosses cap_gbp.
        if cost_gbp > 0:
            try:
                _system_state.add_spend(cost_gbp)
            except Exception as exc:
                logger.error(json.dumps({
                    "event": "system_state_spend_update_failed",
                    "cost_gbp": cost_gbp,
                    "error": str(exc)[:200],
                }))
                try:
                    from servers.alerting import alert_admin, HIGH
                    alert_admin(HIGH, "cost_guard_spend_update_failed", f"add_spend({cost_gbp:.4f} GBP) failed: {str(exc)[:200]}")
                except Exception as _alert_exc:
                    logger.critical(json.dumps({
                        "event": "cost_guard_alert_double_failure",
                        "original_error": str(exc)[:200],
                        "alert_error": str(_alert_exc)[:200],
                        "cost_gbp": cost_gbp,
                    }))

        today = datetime.date.today().isoformat()
        if state.get("app:cost_date") != today:
            state["app:cost_date"] = today
            state["app:cost_gbp_today"] = 0.0

        prev_daily = state.get("app:cost_gbp_today", 0.0)

        # Write to Firestore daily counter and use the global total so concurrent
        # sessions across instances don't each undercount their share. On Firestore
        # error (returns 0.0), fall back to local session accumulation.
        if cost_gbp > 0:
            global_daily = _system_state.add_daily_spend(cost_gbp)
            daily_total = global_daily if global_daily > 0 else prev_daily + cost_gbp
        else:
            daily_total = prev_daily
        state["app:cost_gbp_today"] = daily_total

        # Fire alert once when threshold is first crossed (not on every subsequent call).
        if daily_total >= self._daily_gbp_limit > prev_daily:
            logger.warning(json.dumps({
                "event": "daily_cost_alert",
                "cost_gbp_today": round(daily_total, 4),
                "limit_gbp": self._daily_gbp_limit,
                "action": "review_spend_or_increase_COST_GUARD_DAILY_GBP",
            }))

        return None

    def _gbp_rate(self, llm_response) -> float:
        """Return GBP per 1M input tokens for the model that produced this response."""
        model = (
            getattr(llm_response, "model_version", None)
            or getattr(llm_response, "model", None)
            or ""
        )
        model_str = str(model)
        for alias, rate in _GBP_PER_M_TOKENS.items():
            if alias in model_str:
                return rate
        logger.warning(json.dumps({
            "event": "cost_guard_rate_fallback",
            "model_version": model_str,
            "using_fallback_rate_gbp_per_m": _GBP_FALLBACK_RATE,
            "note": "add alias to _GBP_PER_M_TOKENS to fix",
        }))
        return _GBP_FALLBACK_RATE

    def _gbp_output_rate(self, llm_response) -> float:
        """Return GBP per 1M output tokens for the model that produced this response."""
        model = (
            getattr(llm_response, "model_version", None)
            or getattr(llm_response, "model", None)
            or ""
        )
        model_str = str(model)
        for alias, rate in _GBP_PER_M_OUTPUT_TOKENS.items():
            if alias in model_str:
                return rate
        return _GBP_OUTPUT_FALLBACK_RATE

    async def on_event_callback(self, *, invocation_context, event):
        """No-op after compaction.

        Previously zeroed tool_call_count on compaction events so long sessions
        wouldn't hit the cap. That was the primary hole: a runaway turn that
        triggered compaction got its 50-call budget wiped and could run
        indefinitely. Per-turn counter reset now happens ONLY in before_run_callback
        (once per user message). Session-cumulative counters (session:llm_call_count,
        session:uncached_input_total) in after_model_callback provide the backstop.
        """
        return None
