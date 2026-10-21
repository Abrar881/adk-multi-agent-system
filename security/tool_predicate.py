"""Context-aware ToolPredicate for McpToolset.tool_filter.

Layer 1 of RBAC: hides blocked tools from LLM function declarations
entirely for customer sessions. The LLM never sees them, so it can't
attempt to call them.

Layer 2 (execution-time enforcement) is in plugins/role_guard.py.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from google.adk.tools.base_tool import BaseTool
from google.adk.agents.readonly_context import ReadonlyContext

from security.tool_permissions import is_tool_allowed, UNRESTRICTED_ROLES

logger = logging.getLogger(__name__)


class RoleAwareToolFilter:
    """ToolPredicate that filters tools based on session _source_role.

    Satisfies the ToolPredicate protocol:
        __call__(tool, readonly_context) -> bool

    Returns True (show tool) when:
    - readonly_context is None (startup warmup — allow all)
    - source role is unrestricted
    - tool is in the customer allowlist for restricted roles
    """

    def __call__(
        self,
        tool: BaseTool,
        readonly_context: Optional[ReadonlyContext] = None,
    ) -> bool:
        if readonly_context is None:
            # No context during toolset warmup — expose all tools.
            return True

        state = getattr(readonly_context, "state", None)
        if state is None:
            return True

        source_role = state.get("_source_role", "_unknown")
        if source_role == "_unknown":
            logger.warning(json.dumps({"event": "missing_source_role", "layer": "L1_predicate", "tool": tool.name}))
        return is_tool_allowed(tool.name, source_role)


class AgentScopedToolFilter:
    """Role-aware filter with an additional per-agent tool allowlist.

    For unrestricted roles (system, scheduled_task, etc.) tools are further
    restricted to allowed_tools — reducing the Monday schema from 20 tools
    to 5-11 per agent.  Restricted roles (email_customer, whatsapp_customer)
    are unaffected: CUSTOMER_ALLOWED_TOOLS already limits them.
    """

    def __init__(self, allowed_tools: frozenset) -> None:
        self._allowed = allowed_tools

    def __call__(
        self,
        tool: BaseTool,
        readonly_context: Optional[ReadonlyContext] = None,
    ) -> bool:
        if readonly_context is None:
            return True  # startup warmup — expose all

        state = getattr(readonly_context, "state", None)
        if state is None:
            return True

        source_role = state.get("_source_role", "_unknown")
        if source_role == "_unknown":
            logger.warning(json.dumps({"event": "missing_source_role", "layer": "L1_scoped", "tool": tool.name}))
        if not is_tool_allowed(tool.name, source_role):
            return False
        # For unrestricted roles, additionally enforce the agent-specific allowlist
        if source_role in UNRESTRICTED_ROLES:
            return tool.name in self._allowed
        return True
