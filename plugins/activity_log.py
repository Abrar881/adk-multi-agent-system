"""Activity log plugin — structured audit logging for all tool calls."""
import json
import logging
from datetime import datetime, timezone
from typing import Callable, Optional
from google.adk.plugins import BasePlugin

logger = logging.getLogger(__name__)


class ActivityLogPlugin(BasePlugin):
    """Logs tool call entry/exit to stderr and optionally Firestore.

    Replaces the old _before_tool/_after_tool callbacks. Pass ``firestore_log_func``
    (the main.py ``log_event`` function) to get Firestore persistence; omit it for
    stderr-only logging (useful in tests).
    """

    def __init__(self, firestore_log_func: Optional[Callable] = None):
        super().__init__(name="activity_log")
        self._log = firestore_log_func

    async def before_tool_callback(self, *, tool, tool_args, tool_context):
        arg_summary = {
            "arg_count": len(tool_args or {}),
            "keys": sorted((tool_args or {}).keys()),
        }
        if self._log:
            self._log("system", "tool_call", tool=tool.name, arguments=arg_summary)
        else:
            logger.info(json.dumps({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "agent": "system",
                "event": "tool_call",
                "tool": tool.name,
                "arguments": arg_summary,
            }))
        return None

    async def after_tool_callback(self, *, tool, tool_args, tool_context, result):
        success = not (isinstance(result, dict) and "error" in result)
        if self._log:
            self._log("system", "tool_result", tool=tool.name, success=success)
        else:
            logger.info(json.dumps({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "agent": "system",
                "event": "tool_result",
                "tool": tool.name,
                "success": success,
            }))
        return None

    async def after_model_callback(self, *, callback_context, llm_response):
        if getattr(llm_response, 'partial', False):
            return llm_response
        usage = getattr(llm_response, 'usage_metadata', None)
        if usage:
            agent_name = getattr(callback_context, 'agent_name', 'unknown')
            # model: prefer model_version (populated by Vertex after the call) then model field.
            # Fallback for agents using FlexGemini where Vertex Flex tier omits model_version.
            _MODEL_FALLBACK = {
                "hub": "gemini-3.1-flash-lite",
                "dispatcher": "gemini-3.8-flash",
                "finance": "gemini-3.8-flash",
                "ops": "gemini-3.8-flash",
            }
            model = (
                getattr(llm_response, 'model_version', None)
                or getattr(llm_response, 'model', None)
                or _MODEL_FALLBACK.get(agent_name, "")
            )
            invocation_id = getattr(callback_context, 'invocation_id', None) or ""
            _input = getattr(usage, 'prompt_token_count', 0) or 0
            _cached = getattr(usage, 'cached_content_token_count', 0) or 0
            entry = {
                "event": "llm_usage",
                "agent": agent_name,
                "model": model,
                "invocation_id": invocation_id,
                "input_tokens": _input,
                "output_tokens": getattr(usage, 'candidates_token_count', 0),
                "cached_tokens": _cached,
                "cache_hit_rate": round(_cached / _input, 3) if _input > 0 else 0.0,
                "total_tokens": getattr(usage, 'total_token_count', 0),
            }
            if self._log:
                self._log("system", "llm_usage",
                          llm_agent=agent_name,
                          model=model,
                          invocation_id=invocation_id,
                          input_tokens=entry["input_tokens"],
                          output_tokens=entry["output_tokens"],
                          cached_tokens=entry["cached_tokens"],
                          total_tokens=entry["total_tokens"])
            else:
                logger.info(json.dumps({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    **entry,
                }))
        return llm_response
