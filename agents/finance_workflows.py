"""Finance SequentialAgent pipelines (Wave 3).

Factory functions â€” call after toolsets and AGENT_MODEL are defined in main.py.
Each pipeline breaks complex multi-step workflows into discrete locked sub-agents
so failures in one step don't silently cascade, and each step is independently observable.
"""
import json
import logging
import os

from google.adk.agents import LlmAgent, SequentialAgent
from google.genai import types
from config.monday_config import get_board_id
from agents._knowledge import _load_knowledge

_log = logging.getLogger(__name__)


def _fetch_and_check_pending(callback_context) -> types.Content | None:
    """Replaces pending_invoice_reader LlmAgent.

    Reads Firestore directly (same query as list_pending_invoices MCP tool),
    writes result into state["pending_invoices_list"], then either skips
    invoice_creator (if empty) or returns None to let it run.
    Eliminates one LLM call per invoice sweep.
    """
    try:
        from google.cloud import firestore as _fs
        db = _fs.Client(project=os.environ.get("GOOGLE_CLOUD_PROJECT", "YOUR_GCP_PROJECT_ID"))
        docs = db.collection("pending_invoices").where("status", "==", "pending").limit(50).stream()
        results = [doc.to_dict() for doc in docs]
        result_json = json.dumps({"status": "success", "pending_invoices": results, "count": len(results)})
    except Exception as exc:
        _log.warning("_fetch_and_check_pending: Firestore error: %s", exc)
        result_json = json.dumps({"status": "error", "reason": str(exc), "pending_invoices": []})

    callback_context.state["pending_invoices_list"] = result_json

    try:
        data = json.loads(result_json)
        if not data.get("pending_invoices"):
            return types.Content(role="model", parts=[types.Part(text="No pending invoices to process")])
    except Exception:
        pass
    return None

# Loaded once at import time â€” same 4 files as finance_agent's static_instruction.
# Appended to recon_update / chase_draft / invoice_creator to push their combined
# prompt (instruction + tool schemas) above Vertex's 4096-token implicit cache floor.
# invoice_creator's instruction explicitly promises "pricing reference below" â€” this
# fulfils that promise.
_WORKFLOW_KNOWLEDGE_REF = _load_knowledge(
    "Detailed pricing tables.md",
    "discounts.txt",
    "key_collection.txt",
    "terms_and_conditions.txt",
)


def _skip_unless_locked(callback_context) -> types.Content | None:
    if "LOCKED" not in callback_context.state.get("lock_result", ""):
        return types.Content(role="model", parts=[types.Part(text="SKIPPED - lock not acquired")])
    return None


def _skip_unless_match(callback_context) -> types.Content | None:
    match = str(callback_context.state.get("temp:match_result", ""))
    if not match or "SKIPPED" in match:
        return types.Content(role="model", parts=[types.Part(text="SKIPPED - no match result")])
    return None


def _skip_unless_chase_lock(callback_context) -> types.Content | None:
    result = str(callback_context.state.get("temp:chase_lock_result", "")).strip()
    if not result or result in ("[]", "null"):
        return types.Content(role="model", parts=[types.Part(text="SKIPPED - no invoices locked")])
    return None


def _recon_update_data(ctx) -> str:
    match = ctx.state.get("temp:match_result", "")
    return f"Match results from reconciliation fetch:\n{match}" if match else ""


def _chase_draft_data(ctx) -> str:
    locked = ctx.state.get("temp:chase_lock_result", "")
    return f"Locked invoice IDs for chasing:\n{locked}" if locked else ""


def _invoice_creator_data(ctx) -> str:
    invoices = ctx.state.get("pending_invoices_list", "")
    return f"Pending invoices to process:\n{invoices}" if invoices else ""


def create_finance_workflows(
    locking_ts, xero_ts, monday_ts, gmail_ts, rag_ts, reporting_ts,
    agent_model, lite_model=None,
):
    """Return (reconciliation_pipeline, chase_pipeline, invoice_sweep) SequentialAgents."""

    # Mechanical lock/fetch steps run on flash-lite with thinking off â€” they
    # acquire locks and fetch data, no prose or reasoning needed.
    # Client-facing steps (recon_update, chase_draft) stay on agent_model (flash).
    _step_model = lite_model or agent_model
    _no_think = types.GenerateContentConfig(
        max_output_tokens=512,
        thinking_config=types.ThinkingConfig(thinking_budget=0),
    )

    # â”€â”€ Reconciliation pipeline â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    recon_lock = LlmAgent(
        model=_step_model,
        generate_content_config=_no_think,
        name="recon_lock",
        description="Step 1: Acquires the daily reconciliation lock.",
        static_instruction=(
            "Acquire the reconciliation lock for today.\n"
            "Call lock_task(task_id='reconciliation_' + today's date in YYYY-MM-DD format).\n"
            "If result is 'LOCKED': output 'LOCKED'.\n"
            "If result is 'ALREADY_RUNNING': retry once, then output 'SKIPPED'.\n"
            "Store your result in lock_result."
        ),
        tools=[locking_ts],
        output_key="lock_result",
        include_contents="none",
    )

    recon_fetch = LlmAgent(
        model=_step_model,
        generate_content_config=_no_think,
        name="recon_fetch",
        description="Step 2: Fetches unpaid invoices and matches payments.",
        before_agent_callback=_skip_unless_locked,
        static_instruction=(
            "1. Call list_unpaid_invoices() to retrieve all outstanding invoices.\n"
            "2. For each invoice call get_invoice_details() to check payment status.\n"
            "3. Record which invoices have matching payments.\n"
            "Store the matched payment data as JSON in match_result."
        ),
        tools=[xero_ts, monday_ts],
        output_key="temp:match_result",
        include_contents="none",
    )

    recon_update = LlmAgent(
        model=agent_model,
        generate_content_config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=512),
        ),
        name="recon_update",
        description="Step 3: Updates Monday board and drafts reconciliation summary.",
        before_agent_callback=_skip_unless_match,
        include_contents="none",
        static_instruction=(
            "Review the match results provided below.\n"
            "For matched payments where the Xero invoice status is 'PAID' or amount_due is 0:\n"
            "1. Call update_multiple_columns ONCE per board item (batch all column changes for that item into a single call â€” do NOT make separate calls per column).\n"
            "   Set the Payment status column to label 'Payment received' for ALL matched items in one pass.\n"
            "   Limit to 10 items per run; if more than 10 are matched, process the first 10 and note the remainder in the summary.\n"
            "2. Draft ONE reconciliation summary email to admin via draft_reply covering all matched payments.\n"
            "3. Call generate_invoice_audit_report ONCE for the full run (not per invoice).\n\n"
            "EFFICIENCY RULE: Make the minimum number of tool calls needed. Do NOT loop item-by-item â€” "
            "gather all matched items first, then make one update call per board item, then one email, then one report.\n\n"
            "XERO INVOICE STATUS REFERENCE:\n"
            "- PAID: Full payment received, amount_due = 0.00 â€” reconcile and update board\n"
            "- AUTHORISED: Approved but unpaid â€” do not reconcile\n"
            "- SUBMITTED: Sent to customer, awaiting payment â€” do not reconcile\n"
            "- VOIDED: Cancelled, no longer due â€” skip\n"
            "- DRAFT: Not yet finalised â€” skip\n\n"
            "RECONCILIATION RULES:\n"
            "- Match by invoice number first (exact match required)\n"
            "- If no invoice number: match by amount + customer name\n"
            "- Partial payments: update amount_due on Xero side, note in summary, keep AUTHORISED\n"
            "- Overpayments: flag for manual review, do NOT auto-reconcile\n"
            "- Credit notes: net against outstanding balance before matching\n\n"
            "LANGUAGE: Use British English spelling, grammar, and conventions in all responses "
            "(e.g. organise, colour, whilst, centre, recognise, travelling, licence/license distinction). "
            "TIMEZONE: All times are UTC internally. Display in Europe/London timezone for all "
            "human-facing messages."
            + _WORKFLOW_KNOWLEDGE_REF
        ),
        tools=[monday_ts, gmail_ts, reporting_ts],
        instruction=_recon_update_data,
    )

    reconciliation_pipeline = SequentialAgent(
        name="reconciliation_pipeline",
        description=(
            "Full reconciliation workflow: lock â†’ fetch unpaid invoices â†’ update board. "
            "Use for 'reconciliation' and 'payment matching' tasks."
        ),
        sub_agents=[recon_lock, recon_fetch, recon_update],
    )

    # â”€â”€ Invoice chase pipeline â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    chase_lock = LlmAgent(
        model=_step_model,
        generate_content_config=_no_think,
        name="chase_lock",
        description="Step 1: Lists unpaid invoices and acquires per-invoice chase locks.",
        instruction=(
            "1. Call list_unpaid_invoices() to get all outstanding invoices.\n"
            "2. For each invoice call lock_task(task_id='chase_' + invoice_id + '_' + today_date, ttl_seconds=43200)\n"
            "   where today_date is today's date in YYYY-MM-DD format (e.g. '2025-03-14').\n"
            "   This prevents the same invoice being chased twice in one day.\n"
            "3. Record which invoice IDs were successfully locked (result='LOCKED').\n"
            "   If lock_task returns 'ALREADY_LOCKED', skip that invoice â€” it was already chased today.\n"
            "Store the list of locked invoice IDs as JSON in chase_lock_result."
        ),
        tools=[locking_ts, xero_ts],
        output_key="temp:chase_lock_result",
        include_contents="none",
    )

    chase_draft = LlmAgent(
        model=agent_model,
        generate_content_config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=512),
        ),
        name="chase_draft",
        description="Step 2: Drafts chase emails for each locked invoice.",
        before_agent_callback=_skip_unless_chase_lock,
        include_contents="none",
        static_instruction=(
            "Review the locked invoice IDs provided below.\n"
            "For each locked invoice:\n"
            "1. Call get_invoice_details() to get days overdue and client contact.\n"
            "2. Call consult_knowledge to retrieve the appropriate chase template.\n"
            "3. Draft the chase email via draft_reply (saved to Gmail Drafts for review):\n"
            "   - 1â€“7 days overdue: gentle reminder\n"
            "   - 8â€“30 days overdue: firm chase\n"
            "   - 30+ days overdue: final notice, create Monday NEEDS_REVIEW task\n"
            "Do NOT send emails directly â€” all go to Drafts for human review.\n"
            "4. After each successful draft_reply, call mark_chase_drafted(message_id) using the\n"
            "   message_id returned in the draft_reply response. This labels the draft AI_CHASE_DRAFTED\n"
            "   in Gmail so ops can review all chase drafts in one place.\n\n"
            "CHASE EMAIL GUIDELINES:\n"
            "- Subject: 'Payment Reminder â€” Invoice [number]' (gentle/firm) or "
            "'FINAL NOTICE â€” Invoice [number]'\n"
            "- Always include: invoice number, amount due, original due date, days overdue\n"
            "- Gentle (1-7 days): polite reminder, assume oversight, offer payment methods\n"
            "- Firm (8-30 days): state overdue period, reference payment terms, request immediate payment\n"
            "- Final (30+ days): warn of service suspension, create NEEDS_REVIEW Monday task for admin\n"
            "- Sign off as 'Acme Property Services Accounts Team'\n\n"
            "LANGUAGE: Use British English spelling, grammar, and conventions in all responses "
            "(e.g. organise, colour, whilst, centre, recognise, travelling, licence/license distinction). "
            "TIMEZONE: All times are UTC internally. Display in Europe/London timezone for all "
            "human-facing messages."
            + _WORKFLOW_KNOWLEDGE_REF
        ),
        tools=[xero_ts, gmail_ts, rag_ts, monday_ts],
        instruction=_chase_draft_data,
    )

    chase_pipeline = SequentialAgent(
        name="chase_pipeline",
        description=(
            "Systematic invoice chasing: lock invoices â†’ draft chase emails. "
            "Use for 'chase_morning', 'chase_afternoon', and ad-hoc invoice chasing tasks."
        ),
        sub_agents=[chase_lock, chase_draft],
    )

    # â”€â”€ Invoice sweep pipeline â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # pending_invoice_reader was an LlmAgent that called list_pending_invoices via MCP.
    # Replaced by _fetch_and_check_pending before_agent_callback on invoice_creator â€”
    # pure Firestore read needs no LLM, saving one LLM call per sweep.

    invoice_creator = LlmAgent(
        model=agent_model,
        generate_content_config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget=512),
        ),
        name="invoice_creator",
        description="Creates Xero invoices for each pending invoice (fetches list from Firestore first).",
        before_agent_callback=_fetch_and_check_pending,
        include_contents="none",
        static_instruction=(
            "Process each pending invoice from the list provided below.\n"
            "For each invoice in the list:\n"
            "1. Call create_invoice with:\n"
            "   - customer_name: invoice.customer_name\n"
            "   - customer_email: invoice.customer_email\n"
            "   - line_item_description: invoice.line_item_description if present and non-empty (use exactly as stored, do not invent); if absent or empty, use 'Inventory service' as the fallback\n"
            "   - amount: invoice.amount\n"
            "   - currency: invoice.currency\n"
            "   - vat_applies: invoice.vat_applies\n"
            "   - monday_item_id: invoice.item_id\n"
            "   - thread_id: invoice.thread_id\n"
            "   - booking_date: invoice.booking_date\n"
            f"2. Handle the result:\n"
            f"   - status 'success': call set_invoice_number(board_id={get_board_id('inventories')}, item_id=invoice.item_id, invoice_number=result.invoice_number, pay_link=result.online_url, amount=result.amount, xero_status=result.xero_status, invoice_id=result.invoice_id) to update Monday\n"
            "   - status 'skipped_duplicate': log and skip (already invoiced)\n"
            "   - status 'hold_review': create a Monday update on item_id saying 'Invoice needs review â€” amount may exceed ceiling. Manual action required.'\n"
            "   - status 'disabled': log and stop processing (kill switch is on)\n"
            "   - status 'error': log and skip (will retry next sweep)\n"
            "3. Log results and summary of all invoices processed.\n"
            "Note: Firestore pending_invoices doc remains status='pending' but idempotency lock in task_locks prevents re-creation.\n\n"
            "INVOICE VALIDATION:\n"
            "- Cross-check invoice amount against pricing reference below before creating\n"
            "- If amount exceeds expected price by >20%, flag as hold_review\n"
            "- VAT at 20% applies to all standard services unless explicitly exempt\n\n"
            "LANGUAGE: Use British English spelling, grammar, and conventions in all responses "
            "(e.g. organise, colour, whilst, centre, recognise, travelling, licence/license distinction). "
            "TIMEZONE: All times are UTC internally. Display in Europe/London timezone for all "
            "human-facing messages."
            + _WORKFLOW_KNOWLEDGE_REF
        ),
        tools=[xero_ts, monday_ts, gmail_ts],
        instruction=_invoice_creator_data,
    )

    invoice_sweep = SequentialAgent(
        name="invoice_sweep",
        description=(
            "Automatic invoice sweep: fetch pending invoices from Firestore â†’ create Xero invoices â†’ update Monday board. "
            "Use for automated 'invoice_sweep' scheduled task (15-minute intervals)."
        ),
        sub_agents=[invoice_creator],
    )

    return reconciliation_pipeline, chase_pipeline, invoice_sweep
