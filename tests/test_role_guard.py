"""Tests for RBAC: tool_permissions, RoleAwareToolFilter, and RoleGuardPlugin."""
import pytest
from unittest.mock import AsyncMock, MagicMock

from security.tool_permissions import is_tool_allowed, CUSTOMER_ALLOWED_TOOLS, CUSTOMER_BLOCKED_TOOLS
from security.tool_predicate import RoleAwareToolFilter
from plugins.role_guard import RoleGuardPlugin


# ---------------------------------------------------------------------------
# is_tool_allowed
# ---------------------------------------------------------------------------

class TestIsToolAllowed:
    def test_admin_unrestricted(self):
        for tool in CUSTOMER_BLOCKED_TOOLS:
            assert is_tool_allowed(tool, "dashboard_admin") is True

    def test_scheduled_unrestricted(self):
        assert is_tool_allowed("send_whatsapp_message", "scheduled_task") is True
        assert is_tool_allowed("duplicate_board", "scheduled_task") is True

    def test_monday_webhook_unrestricted(self):
        assert is_tool_allowed("update_column_value", "monday_webhook") is True

    def test_system_unrestricted(self):
        assert is_tool_allowed("send_admin_alert", "system") is True

    def test_email_customer_allowed_tools(self):
        for tool in CUSTOMER_ALLOWED_TOOLS:
            assert is_tool_allowed(tool, "email_customer") is True, f"Expected {tool} allowed"

    def test_email_customer_blocked_tools(self):
        for tool in CUSTOMER_BLOCKED_TOOLS:
            assert is_tool_allowed(tool, "email_customer") is False, f"Expected {tool} blocked"

    def test_whatsapp_customer_same_as_email(self):
        assert is_tool_allowed("send_whatsapp_message", "whatsapp_customer") is False
        assert is_tool_allowed("consult_knowledge", "whatsapp_customer") is True

    def test_unknown_role_fails_closed(self):
        assert is_tool_allowed("consult_knowledge", "unknown_role") is False
        assert is_tool_allowed("send_admin_alert", "unknown_role") is False

    def test_specific_customer_allowed(self):
        assert is_tool_allowed("create_inventory_booking", "email_customer") is True
        assert is_tool_allowed("create_lead", "email_customer") is True
        assert is_tool_allowed("draft_reply", "email_customer") is True
        assert is_tool_allowed("consult_knowledge", "email_customer") is True

    def test_specific_customer_blocked(self):
        assert is_tool_allowed("send_whatsapp_message", "email_customer") is False
        assert is_tool_allowed("update_column_value", "email_customer") is False
        assert is_tool_allowed("archive_item", "email_customer") is False
        assert is_tool_allowed("request_approval", "email_customer") is False
        assert is_tool_allowed("list_unpaid_invoices", "email_customer") is False


# ---------------------------------------------------------------------------
# RoleAwareToolFilter (ToolPredicate / L1)
# ---------------------------------------------------------------------------

class TestRoleAwareToolFilter:
    def _make_tool(self, name):
        tool = MagicMock()
        tool.name = name
        return tool

    def _make_context(self, source_role):
        ctx = MagicMock()
        ctx.state = {"_source_role": source_role}
        return ctx

    def test_no_context_allows_all(self):
        f = RoleAwareToolFilter()
        tool = self._make_tool("send_whatsapp_message")
        assert f(tool, None) is True

    def test_admin_sees_blocked_tools(self):
        f = RoleAwareToolFilter()
        tool = self._make_tool("send_whatsapp_message")
        ctx = self._make_context("dashboard_admin")
        assert f(tool, ctx) is True

    def test_customer_hides_blocked_tools(self):
        f = RoleAwareToolFilter()
        tool = self._make_tool("send_whatsapp_message")
        ctx = self._make_context("email_customer")
        assert f(tool, ctx) is False

    def test_customer_shows_allowed_tools(self):
        f = RoleAwareToolFilter()
        tool = self._make_tool("consult_knowledge")
        ctx = self._make_context("email_customer")
        assert f(tool, ctx) is True

    def test_context_with_no_state_allows_all(self):
        f = RoleAwareToolFilter()
        tool = self._make_tool("send_whatsapp_message")
        ctx = MagicMock(spec=[])  # no .state attribute
        assert f(tool, ctx) is True

    def test_missing_source_role_defaults_to_unknown(self):
        f = RoleAwareToolFilter()
        tool = self._make_tool("send_whatsapp_message")
        ctx = MagicMock()
        ctx.state = {}  # no _source_role key
        assert f(tool, ctx) is False  # _unknown is fail-closed


# ---------------------------------------------------------------------------
# RoleGuardPlugin (L2)
# ---------------------------------------------------------------------------

class TestRoleGuardPlugin:
    def _make_tool(self, name):
        tool = MagicMock()
        tool.name = name
        return tool

    def _make_context(self, source_role):
        ctx = MagicMock()
        ctx.state = {"_source_role": source_role}
        return ctx

    @pytest.mark.asyncio
    async def test_admin_passes_blocked_tool(self):
        plugin = RoleGuardPlugin()
        tool = self._make_tool("send_whatsapp_message")
        ctx = self._make_context("dashboard_admin")
        result = await plugin.before_tool_callback(tool=tool, tool_args={}, tool_context=ctx)
        assert result is None

    @pytest.mark.asyncio
    async def test_customer_blocked_tool_returns_error(self):
        plugin = RoleGuardPlugin()
        tool = self._make_tool("send_whatsapp_message")
        ctx = self._make_context("email_customer")
        result = await plugin.before_tool_callback(tool=tool, tool_args={}, tool_context=ctx)
        assert result is not None
        assert "error" in result
        assert "send_whatsapp_message" in result["error"]
        assert "email_customer" in result["error"]

    @pytest.mark.asyncio
    async def test_customer_allowed_tool_passes(self):
        plugin = RoleGuardPlugin()
        tool = self._make_tool("consult_knowledge")
        ctx = self._make_context("email_customer")
        result = await plugin.before_tool_callback(tool=tool, tool_args={}, tool_context=ctx)
        assert result is None

    @pytest.mark.asyncio
    async def test_no_state_passes(self):
        plugin = RoleGuardPlugin()
        tool = self._make_tool("send_whatsapp_message")
        ctx = MagicMock(spec=[])  # no .state attribute
        result = await plugin.before_tool_callback(tool=tool, tool_args={}, tool_context=ctx)
        assert result is None

    @pytest.mark.asyncio
    async def test_scheduled_task_passes_blocked_tool(self):
        plugin = RoleGuardPlugin()
        tool = self._make_tool("duplicate_board")
        ctx = self._make_context("scheduled_task")
        result = await plugin.before_tool_callback(tool=tool, tool_args={}, tool_context=ctx)
        assert result is None

    @pytest.mark.asyncio
    async def test_firestore_log_called_on_block(self):
        log_fn = MagicMock()
        plugin = RoleGuardPlugin(firestore_log_func=log_fn)
        tool = self._make_tool("archive_item")
        ctx = self._make_context("email_customer")
        await plugin.before_tool_callback(tool=tool, tool_args={}, tool_context=ctx)
        log_fn.assert_called_once()
        call_kwargs = log_fn.call_args
        assert "tool_blocked_by_role" in str(call_kwargs)

    @pytest.mark.asyncio
    async def test_whatsapp_customer_blocked(self):
        plugin = RoleGuardPlugin()
        tool = self._make_tool("update_status")
        ctx = self._make_context("whatsapp_customer")
        result = await plugin.before_tool_callback(tool=tool, tool_args={}, tool_context=ctx)
        assert result is not None
        assert "error" in result

    @pytest.mark.asyncio
    async def test_create_booking_allowed_for_customer(self):
        plugin = RoleGuardPlugin()
        tool = self._make_tool("create_inventory_booking")
        ctx = self._make_context("email_customer")
        result = await plugin.before_tool_callback(tool=tool, tool_args={}, tool_context=ctx)
        assert result is None

    @pytest.mark.asyncio
    async def test_missing_source_role_blocked(self):
        plugin = RoleGuardPlugin()
        tool = self._make_tool("consult_knowledge")
        ctx = MagicMock()
        ctx.state = {}  # no _source_role key → _unknown
        result = await plugin.before_tool_callback(tool=tool, tool_args={}, tool_context=ctx)
        assert result is not None
        assert "error" in result
