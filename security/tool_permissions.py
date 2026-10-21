"""Tool access control for role-based authorization.

Source roles:
  - "email_customer"   : inbound email via Gmail push webhook
  - "whatsapp_customer": inbound WhatsApp message via Twilio webhook
  - "dashboard_admin"  : admin using password-protected dashboard
  - "scheduled_task"   : Cloud Scheduler automated task
  - "monday_webhook"   : Monday.com webhook event
  - "system"           : CLI / internal testing
"""

# Roles that bypass all tool restrictions (full access).
UNRESTRICTED_ROLES: frozenset[str] = frozenset({
    "dashboard_admin",
    "scheduled_task",
    "monday_webhook",
    "system",
})

# Roles whose tool access is limited to CUSTOMER_ALLOWED_TOOLS.
RESTRICTED_ROLES: frozenset[str] = frozenset({
    "email_customer",
    "whatsapp_customer",
})

# Allowlist: tools customers are permitted to call.
# Everything NOT in this set is blocked for restricted roles.
CUSTOMER_ALLOWED_TOOLS: frozenset[str] = frozenset({
    # RAG / knowledge
    "consult_knowledge",
    # Gmail reads
    "list_unread_emails",
    "read_email_body",
    "list_sent_emails",
    # Gmail attachment reading (image vision + document text extraction)
    "describe_email_attachment",
    "read_email_attachment",
    # Gmail drafts (saves to Drafts folder, never auto-sends)
    "draft_reply",
    "send_email",
    # Gmail label (mark processed without drafting)
    "mark_email_processed",
    # Monday reads
    "list_boards",
    "get_board_items",
    "get_board_summary",
    "search_items",
    "search_items_paginated",
    "get_board_schema",
    "get_item",
    "get_working_schedule",
    "check_availability",
    # Monday safe writes (time-sensitive; auto-create confirmed OK by admin)
    "create_inventory_booking",
    "create_lead",
    # Locking (internal coordination)
    "lock_task",
    "acquire_lock",
    "release_lock",
    # Coverage gate (pure in-memory postcode classifier, no external I/O)
    "classify_coverage",
    # Reporting (read-only generation)
    "generate_board_health_report",
    "generate_daily_summary",
    "generate_weekly_summary",
    "generate_invoice_audit_report",
    "detect_missing_fields",
    "calculate_completion_rate",
    "format_report_for_email",
    # Ops reads (Google Maps queries)
    "calculate_travel_time",
    "check_job_feasibility",
    "optimize_route",
    # ADK agent routing (required for hub → specialist transfers)
    "transfer_to_agent",
    # Xero finance
    "record_pending_invoice",  # booking flow — writes pending_invoices doc only
    "create_invoice",          # booking flow — creates a draft Xero invoice
    "set_invoice_number",      # booking flow — writes invoice number back to Monday item
    # Booking confirmation sub-agent (AgentTool — bypasses Layer 1 filter, must be
    # explicitly allowed here so Layer 2 doesn't block it and trigger retry loops).
    # Safe: booking_confirmation_loop only drafts + extracts pricing, no destructive writes.
    # Dispatcher instruction gates it to post-booking-success only.
    "booking_confirmation_loop",
})

# Blocked tools — documented here for auditability; enforcement is via
# the allowlist above (anything not in CUSTOMER_ALLOWED_TOOLS is blocked).
CUSTOMER_BLOCKED_TOOLS: frozenset[str] = frozenset({
    # WhatsApp sends — immediate, irreversible
    "send_whatsapp_message",
    "send_whatsapp_template",
    "send_contractor_status_check",
    "send_admin_alert",
    "get_whatsapp_status",
    # Monday destructive writes
    "update_column_value",
    "update_multiple_columns",
    "update_status",
    "create_task_item",
    "create_inventory_task",
    "create_item_update",
    "archive_item",
    "move_item_to_group",
    "duplicate_board",
    # Xero financial reads (not customer-facing)
    "list_unpaid_invoices",
    "get_invoice_details",
    # Config / admin tools
    "reload_monday_config",
    "cleanup_expired_locks",
    # HITL approval (admin-only finance workflow)
    "request_approval",
})


def is_tool_allowed(tool_name: str, source_role: str) -> bool:
    """Return True if the tool is permitted for the given source role.

    Unknown roles fail closed (blocked). This is intentional — any new
    entry point that forgets to set _source_role gets the safe default.
    """
    if source_role in UNRESTRICTED_ROLES:
        return True
    if source_role in RESTRICTED_ROLES:
        return tool_name in CUSTOMER_ALLOWED_TOOLS
    # Unknown role → fail closed
    return False


# ---------------------------------------------------------------------------
# Per-agent Monday tool allowlists — scopes the 20-tool Monday schema to only
# the tools each agent actually calls. Reduces input tokens by ~50% per call
# for unrestricted roles (system, scheduled_task, dashboard_admin, webhook_monday).
# Restricted roles (email_customer, whatsapp_customer) are unaffected —
# CUSTOMER_ALLOWED_TOOLS already limits them.
# ---------------------------------------------------------------------------
DISPATCHER_MONDAY_TOOLS: frozenset[str] = frozenset({
    "list_boards",
    "get_board_items",
    "get_board_summary",
    "get_item",
    "create_inventory_booking",
    "create_task_item",
    "create_lead",
    "search_items",
    "search_items_paginated",
    "update_column_value",
    "update_multiple_columns",
    "set_invoice_number",      # booking flow — writes Xero invoice number to Monday item
    "amend_booking",           # amendment flow — updates existing item, sets hold/re-confirm
    "check_availability",      # deterministic slot check for client availability queries
})

FINANCE_MONDAY_TOOLS: frozenset[str] = frozenset({
    "search_items",
    "search_items_paginated",
    "get_board_items",
    "update_column_value",
    "update_multiple_columns",
    "create_inventory_task",
    "list_pending_invoices",   # invoice_sweep: pending_invoice_reader queries Firestore
    "record_pending_invoice",  # Finance agent: log invoice after booking confirmation
    "set_invoice_number",      # invoice_sweep: invoice_creator writes invoice # to Monday
})

OPS_MONDAY_TOOLS: frozenset[str] = frozenset({
    "list_boards",
    "get_board_items",
    "search_items",
    "search_items_paginated",
    "create_inventory_booking",
    "create_inventory_task",
    "create_task_item",
    "update_column_value",
    "update_multiple_columns",
    "duplicate_board",
    "get_working_schedule",   # clerk availability board — timetable + en-route check planning
    "check_availability",     # deterministic slot check for scheduling decisions
})

# ---------------------------------------------------------------------------
# Locking tools available to all agents.
# cleanup_expired_locks is excluded — it is an admin maintenance task and
# agents should never call it autonomously.
# ---------------------------------------------------------------------------
AGENT_LOCKING_TOOLS: frozenset[str] = frozenset({
    "lock_task",
    "acquire_lock",
    "release_lock",
})

# ---------------------------------------------------------------------------
# Reporting tools scoped to finance agent.
# Dispatcher uses the full reporting_ts (all 7 tools).
# Finance only needs invoice-specific + formatting tools.
# ---------------------------------------------------------------------------
FINANCE_REPORTING_TOOLS: frozenset[str] = frozenset({
    "generate_invoice_audit_report",
    "generate_weekly_summary",
    "format_report_for_email",
})

# ---------------------------------------------------------------------------
# Per-agent Xero tool allowlists — scopes the 3-tool Xero schema per agent.
# Dispatcher only touches Xero during the booking confirmation flow (create_invoice).
# Finance needs full read/write access for reconciliation and chasing.
# ---------------------------------------------------------------------------
DISPATCHER_XERO_TOOLS: frozenset[str] = frozenset({
    "create_invoice",           # booking confirmation flow — creates a draft Xero invoice
})

FINANCE_XERO_TOOLS: frozenset[str] = frozenset({
    "list_unpaid_invoices",     # reconciliation_pipeline + chase_pipeline: find outstanding
    "get_invoice_details",      # reconciliation_pipeline: inspect individual invoice
    "create_invoice",           # finance agent: manual invoice creation
})

# ---------------------------------------------------------------------------
# Per-agent WhatsApp tool allowlists — scopes the 5-tool WhatsApp schema per agent.
# Dispatcher handles customer-facing messaging; ops handles contractor/admin messaging.
# ---------------------------------------------------------------------------
DISPATCHER_WHATSAPP_TOOLS: frozenset[str] = frozenset({
    "send_whatsapp_message",    # customer-facing free-text messages
    "send_whatsapp_template",   # customer-facing template messages
    "get_whatsapp_status",      # rate-limit / delivery status checks
    "send_admin_alert",         # admin notifications (NEEDS_REVIEW, missing_confirmation)
})

OPS_WHATSAPP_TOOLS: frozenset[str] = frozenset({
    "send_contractor_status_check",  # contractor job-status pings
    "send_admin_alert",              # internal admin notifications
    "get_whatsapp_status",           # rate-limit / delivery status checks
    "send_clerk_timetable",          # 8 PM next-day timetable to each clerk
})
