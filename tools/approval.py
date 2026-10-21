"""Human-in-the-loop approval tool (Wave 4).

Wraps the approval request as a FunctionTool with require_confirmation=True.
ADK pauses execution and surfaces the request for human approval via the
dashboard. Requires ResumabilityConfig(is_resumable=True) on the App.

Finance agent uses this for invoices above the configured threshold.
"""
import os
import logging
import json
from google.adk.tools import FunctionTool
from google.adk.tools.tool_context import ToolContext

logger = logging.getLogger(__name__)


def _get_threshold() -> float:
    try:
        from config.dashboard_settings import get_approval_threshold
        return get_approval_threshold()
    except Exception:
        return float(os.environ.get("APPROVAL_THRESHOLD_GBP", "500"))


def request_approval(
    action: str,
    details: str,
    amount_gbp: float = 0.0,
    tool_context: ToolContext = None,
) -> str:
    """Request human approval before a high-value financial action.

    Args:
        action: Short description of the action (e.g. "Send invoice #INV-123").
        details: Full details of what will happen.
        amount_gbp: Value in GBP — triggers approval if above APPROVAL_THRESHOLD_GBP.

    Returns:
        "approved" | "rejected" | "auto_approved" (below threshold).
    """
    if amount_gbp > 0 and amount_gbp <= _get_threshold():
        logger.info(json.dumps({
            "event": "approval_auto_approved",
            "action": action,
            "amount_gbp": amount_gbp,
            "threshold": _get_threshold(),
        }))
        return "auto_approved"

    logger.info(json.dumps({
        "event": "approval_requested",
        "action": action,
        "amount_gbp": amount_gbp,
    }))
    return f"Approval requested for: {action}. Details: {details}"


approval_tool = FunctionTool(func=request_approval, require_confirmation=True)
