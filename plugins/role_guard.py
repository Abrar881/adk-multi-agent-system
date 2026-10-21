"""Role-based tool access guard plugin.

Layer 2 of RBAC: blocks tool execution for calls not permitted for the
current source role. Registered FIRST in the plugin list so blocked
calls are never counted toward CostGuardPlugin's tool-call limit.

Returns an error dict that the LLM sees as a tool failure, prompting
it to use an alternative approach (e.g. text response instead of action).
"""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Optional

from google.adk.plugins import BasePlugin

from security.tool_permissions import is_tool_allowed, RESTRICTED_ROLES

logger = logging.getLogger("google_adk.role_guard")


class RoleGuardPlugin(BasePlugin):
    """Enforces role-based tool access via before_tool_callback."""

    def __init__(self, firestore_log_func: Optional[Callable] = None):
        super().__init__(name="role_guard")
        self._log = firestore_log_func

    async def before_tool_callback(
        self,
        *,
        tool: Any,
        tool_args: dict[str, Any],
        tool_context: Any,
    ) -> Optional[dict]:
        state = getattr(tool_context, "state", None)
        if state is None:
            return None

        source_role = state.get("_source_role", "_unknown")

        if source_role == "_unknown":
            logger.warning(json.dumps({"event": "missing_source_role", "layer": "L2_guard", "tool": tool.name}))
            return {"error": "Tool access denied: session role is unknown. Ensure _source_role is set."}

        # Fast path: unrestricted roles skip the check entirely.
        if source_role not in RESTRICTED_ROLES:
            return None

        if not is_tool_allowed(tool.name, source_role):
            entry = {
                "event": "tool_blocked_by_role",
                "tool": tool.name,
                "source_role": source_role,
            }
            logger.warning(json.dumps(entry))
            if self._log:
                try:
                    self._log("system", "tool_blocked_by_role",
                              tool=tool.name, source_role=source_role)
                except Exception:
                    pass
            return {
                "error": (
                    f"Tool '{tool.name}' is not available for "
                    f"{source_role} interactions. Provide a text "
                    f"response or use an allowed tool instead."
                )
            }

        return None
