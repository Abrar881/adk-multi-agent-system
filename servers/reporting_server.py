import sys
import os
import json
import time
import random
import functools
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any
from mcp.server.fastmcp import FastMCP

# --- STRUCTURED LOGGING ---
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
# This server orchestrates reporting but delegates actual API calls to other MCP servers
# It does NOT make direct API calls to Monday/Xero/Gmail

SERVICE_NAME = "reporting_server"
AGENT_NAME = "reporting_server"

mcp = FastMCP("AcmeReporting")

def _log(
    level: str,
    *,
    tool: str,
    event: str,
    status: str,
    latency_ms: int | None = None,
    **fields: Any,
) -> None:
    """Emit a structured JSON log line (no prints)."""
    payload: Dict[str, Any] = {
        "agent": AGENT_NAME,
        "tool": tool,
        "event": event,
        "service": SERVICE_NAME,
        "status": status,
    }
    if latency_ms is not None:
        payload["latency_ms"] = latency_ms
    payload.update(fields)
    getattr(logger, level)(json.dumps(payload))


def _error_response(
    *,
    tool: str,
    error_code: str,
    message: str,
    latency_ms: int | None = None,
    **details: Any,
) -> str:
    """Return a machine-readable error payload for agent/tool consumers."""
    payload: Dict[str, Any] = {
        "status": "error",
        "error_code": error_code,
        "message": message[:200],
        "agent": AGENT_NAME,
        "tool": tool,
        "service": SERVICE_NAME,
    }
    if latency_ms is not None:
        payload["latency_ms"] = latency_ms
    if details:
        payload["details"] = details
    return json.dumps(payload)


_log(
    "info",
    tool="server",
    event="server_startup",
    status="initialized",
)

# --- RETRY DECORATOR ---
def retry(exceptions=(Exception,), tries=4, delay=1, backoff=2, jitter=0.1):
    """Retry with exponential backoff + jitter."""
    def deco(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    sleep_time = _delay + random.uniform(-jitter, jitter)
                    _log(
                        "warning",
                        tool="retry",
                        event="retry",
                        status="retrying",
                        wait_seconds=sleep_time,
                        error=str(e),
                    )
                    await asyncio.sleep(max(0, sleep_time))
                    _tries -= 1
                    _delay *= backoff
            return await func(*args, **kwargs)
        
        def sync_wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    sleep_time = _delay + random.uniform(-jitter, jitter)
                    _log(
                        "warning",
                        tool="retry",
                        event="retry",
                        status="retrying",
                        wait_seconds=sleep_time,
                        error=str(e),
                    )
                    time.sleep(max(0, sleep_time))
                    _tries -= 1
                    _delay *= backoff
            return func(*args, **kwargs)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return deco

# --- HELPER FUNCTIONS ---

def format_report_as_text(report_data: Dict[str, Any], report_type: str) -> str:
    """
    Format report data as human-readable text for email.
    """
    lines = []
    lines.append(f"=== {report_type.upper()} ===")
    lines.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("")
    
    if report_type == "board_health":
        lines.append(f"Total Items Analyzed: {report_data.get('total_items', 0)}")
        lines.append(f"Items with Missing Fields: {report_data.get('items_with_issues', 0)}")
        lines.append("")
        
        if report_data.get('issues'):
            lines.append("ISSUES FOUND:")
            for issue in report_data['issues']:
                lines.append(f"  - Item #{issue.get('item_id')}: {issue.get('item_name')}")
                lines.append(f"    Missing: {', '.join(issue.get('missing_fields', []))}")
                lines.append("")
    
    elif report_type == "daily_summary":
        lines.append(f"Jobs Completed: {report_data.get('completed_count', 0)}")
        lines.append(f"Jobs Pending: {report_data.get('pending_count', 0)}")
        lines.append(f"Jobs In Progress: {report_data.get('in_progress_count', 0)}")
        lines.append(f"Items Needing Review: {report_data.get('needs_review_count', 0)}")
        lines.append("")
        
        if report_data.get('alerts'):
            lines.append("ALERTS:")
            for alert in report_data['alerts']:
                lines.append(f"  - {alert}")
            from config.dashboard_settings import get_admin_phones
            lines.append(f"\nALERTS SENT TO ADMIN: {', '.join(get_admin_phones())}")
            lines.append("")
    
    elif report_type == "weekly_summary":
        lines.append(f"Week: {report_data.get('week_start')} to {report_data.get('week_end')}")
        lines.append(f"Total Jobs Completed: {report_data.get('total_completed', 0)}")
        # Revenue and outstanding invoices excluded â€” requires Xero server integration (not yet available to reporting)
        lines.append("")
    
    elif report_type == "invoice_audit":
        lines.append(f"Jobs Completed (Last 24h): {report_data.get('completed_jobs', 0)}")
        lines.append(f"Invoices Found: {report_data.get('invoices_found', 0)}")
        lines.append(f"Missing Invoices: {report_data.get('missing_invoices', 0)}")
        lines.append("")
        
        if report_data.get('missing_invoice_items'):
            lines.append("JOBS WITHOUT INVOICES:")
            for item in report_data['missing_invoice_items']:
                lines.append(f"  - {item.get('item_name')} (ID: {item.get('item_id')})")
            lines.append("")
    
    lines.append("---")
    return "\n".join(lines)

def calculate_report_metrics(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate metrics from board items for reporting.
    """
    metrics = {
        "total_items": len(items),
        "completed_count": 0,
        "pending_count": 0,
        "in_progress_count": 0,
        "needs_review_count": 0,
        "items_with_issues": 0,
        "issues": []
    }
    
    required_fields = ["property_address", "access_type", "contractor"]
    
    for item in items:
        # Count by status
        status = item.get("status", {}).get("text", "").lower()
        if "complete" in status or "done" in status:
            metrics["completed_count"] += 1
        elif "progress" in status or "working" in status:
            metrics["in_progress_count"] += 1
        elif "review" in status or "stuck" in status:
            metrics["needs_review_count"] += 1
        else:
            metrics["pending_count"] += 1
        
        # Check for missing fields
        missing_fields = []
        column_values = item.get("column_values", [])
        
        for field in required_fields:
            # Check if field exists and has a value
            field_found = False
            for col in column_values:
                if field.lower() in col.get("id", "").lower() or field.lower() in col.get("title", "").lower():
                    if col.get("text") and col.get("text").strip():
                        field_found = True
                        break
            
            if not field_found:
                missing_fields.append(field)
        
        if missing_fields:
            metrics["items_with_issues"] += 1
            metrics["issues"].append({
                "item_id": item.get("id"),
                "item_name": item.get("name"),
                "missing_fields": missing_fields
            })
    
    return metrics

# --- MCP TOOLS ---

@mcp.tool()
def generate_board_health_report(board_items_json: str) -> str:
    """
    Analyzes Monday board items for missing/incomplete fields.
    
    Args:
        board_items_json: JSON string containing board items from monday_server.get_board_items
    
    Returns:
        JSON report with health metrics and issues found
    """
    tool_name = "generate_board_health_report"
    start_time = time.time()
    
    try:
        items = json.loads(board_items_json)
        
        if not isinstance(items, list):
            latency_ms = int((time.time() - start_time) * 1000)
            _log(
                "warning",
                tool=tool_name,
                event=tool_name,
                status="invalid_input",
                latency_ms=latency_ms,
                message="Expected list of items",
            )
            return _error_response(
                tool=tool_name,
                error_code="INVALID_INPUT_TYPE",
                message="Expected list of items",
                latency_ms=latency_ms,
            )
        
        metrics = calculate_report_metrics(items)
        
        report = {
            "status": "success",
            "report_type": "board_health",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "metrics": metrics,
            "summary": f"Analyzed {metrics['total_items']} items, found {metrics['items_with_issues']} with missing fields"
        }
        
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "info",
            tool=tool_name,
            event=tool_name,
            status="success",
            latency_ms=latency_ms,
            items_analyzed=metrics["total_items"],
            issues_found=metrics["items_with_issues"],
        )
        
        return json.dumps(report)
        
    except json.JSONDecodeError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="json_error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INVALID_JSON",
            message=f"Invalid JSON: {str(e)}",
            latency_ms=latency_ms,
        )
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INTERNAL_ERROR",
            message=str(e),
            latency_ms=latency_ms,
        )

@mcp.tool()
def generate_daily_summary(board_items_json: str) -> str:
    """
    Creates daily operational summary (jobs completed, pending, issues).
    
    Args:
        board_items_json: JSON string containing board items from monday_server.get_board_items
    
    Returns:
        JSON report with daily metrics
    """
    tool_name = "generate_daily_summary"
    start_time = time.time()
    
    try:
        items = json.loads(board_items_json)
        
        if not isinstance(items, list):
            latency_ms = int((time.time() - start_time) * 1000)
            _log(
                "warning",
                tool=tool_name,
                event=tool_name,
                status="invalid_input",
                latency_ms=latency_ms,
                message="Expected list of items",
            )
            return _error_response(
                tool=tool_name,
                error_code="INVALID_INPUT_TYPE",
                message="Expected list of items",
                latency_ms=latency_ms,
            )
        
        metrics = calculate_report_metrics(items)
        
        # Generate alerts
        alerts = []
        if metrics['needs_review_count'] > 0:
            alerts.append(f"{metrics['needs_review_count']} items need human review")
        if metrics['items_with_issues'] > 5:
            alerts.append(f"{metrics['items_with_issues']} items have missing required fields")
        
        report = {
            "status": "success",
            "report_type": "daily_summary",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "completed_count": metrics['completed_count'],
            "pending_count": metrics['pending_count'],
            "in_progress_count": metrics['in_progress_count'],
            "needs_review_count": metrics['needs_review_count'],
            "alerts": alerts,
            "summary": f"Daily Summary: {metrics['completed_count']} completed, {metrics['pending_count']} pending, {metrics['in_progress_count']} in progress"
        }
        
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "info",
            tool=tool_name,
            event=tool_name,
            status="success",
            latency_ms=latency_ms,
            completed=metrics["completed_count"],
            pending=metrics["pending_count"],
            alerts=len(alerts),
        )
        
        return json.dumps(report)
        
    except json.JSONDecodeError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="json_error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INVALID_JSON",
            message=f"Invalid JSON: {str(e)}",
            latency_ms=latency_ms,
        )
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INTERNAL_ERROR",
            message=str(e),
            latency_ms=latency_ms,
        )

@mcp.tool()
def generate_weekly_summary(board_items_json: str, week_start: str, week_end: str) -> str:
    """
    Creates weekly operational summary.
    
    Args:
        board_items_json: JSON string containing board items
        week_start: Week start date (YYYY-MM-DD)
        week_end: Week end date (YYYY-MM-DD)
    
    Returns:
        JSON report with weekly metrics
    """
    tool_name = "generate_weekly_summary"
    start_time = time.time()
    
    try:
        items = json.loads(board_items_json)
        
        if not isinstance(items, list):
            latency_ms = int((time.time() - start_time) * 1000)
            _log(
                "warning",
                tool=tool_name,
                event=tool_name,
                status="invalid_input",
                latency_ms=latency_ms,
                message="Expected list of items",
            )
            return _error_response(
                tool=tool_name,
                error_code="INVALID_INPUT_TYPE",
                message="Expected list of items",
                latency_ms=latency_ms,
            )
        
        metrics = calculate_report_metrics(items)
        
        report = {
            "status": "success",
            "report_type": "weekly_summary",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "week_start": week_start,
            "week_end": week_end,
            "total_completed": metrics['completed_count'],
            "total_items": metrics['total_items'],
            "completion_rate": f"{(metrics['completed_count'] / metrics['total_items'] * 100):.1f}%" if metrics['total_items'] > 0 else "0%",
            "items_with_issues": metrics['items_with_issues'],
            "summary": f"Week {week_start} to {week_end}: {metrics['completed_count']} jobs completed"
        }
        
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "info",
            tool=tool_name,
            event=tool_name,
            status="success",
            latency_ms=latency_ms,
            week_start=week_start,
            week_end=week_end,
            completed=metrics["completed_count"],
        )
        
        return json.dumps(report)
        
    except json.JSONDecodeError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="json_error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INVALID_JSON",
            message=f"Invalid JSON: {str(e)}",
            latency_ms=latency_ms,
        )
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INTERNAL_ERROR",
            message=str(e),
            latency_ms=latency_ms,
        )

@mcp.tool()
def detect_missing_fields(board_items_json: str, required_fields: str = "property_address,access_type,contractor") -> str:
    """
    Scans board items for required field gaps.
    
    Args:
        board_items_json: JSON string containing board items
        required_fields: Comma-separated list of required field names
    
    Returns:
        JSON with list of items missing required fields
    """
    tool_name = "detect_missing_fields"
    start_time = time.time()
    
    try:
        items = json.loads(board_items_json)
        required_field_list = [f.strip() for f in required_fields.split(",")]
        
        if not isinstance(items, list):
            latency_ms = int((time.time() - start_time) * 1000)
            _log(
                "warning",
                tool=tool_name,
                event=tool_name,
                status="invalid_input",
                latency_ms=latency_ms,
                message="Expected list of items",
            )
            return _error_response(
                tool=tool_name,
                error_code="INVALID_INPUT_TYPE",
                message="Expected list of items",
                latency_ms=latency_ms,
            )
        
        items_with_missing = []
        
        for item in items:
            missing_fields = []
            column_values = item.get("column_values", [])
            
            for field in required_field_list:
                field_found = False
                for col in column_values:
                    col_id = col.get("id", "").lower()
                    col_title = col.get("title", "").lower()
                    col_text = col.get("text", "")
                    
                    if field.lower() in col_id or field.lower() in col_title:
                        if col_text and col_text.strip():
                            field_found = True
                            break
                
                if not field_found:
                    missing_fields.append(field)
            
            if missing_fields:
                items_with_missing.append({
                    "item_id": item.get("id"),
                    "item_name": item.get("name"),
                    "missing_fields": missing_fields,
                    "action": "NEEDS_REVIEW"
                })
        
        report = {
            "status": "success",
            "total_items_checked": len(items),
            "items_with_missing_fields": len(items_with_missing),
            "items": items_with_missing
        }
        
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "info",
            tool=tool_name,
            event=tool_name,
            status="success",
            latency_ms=latency_ms,
            items_checked=len(items),
            items_with_missing=len(items_with_missing),
        )
        
        return json.dumps(report)
        
    except json.JSONDecodeError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="json_error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INVALID_JSON",
            message=f"Invalid JSON: {str(e)}",
            latency_ms=latency_ms,
        )
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INTERNAL_ERROR",
            message=str(e),
            latency_ms=latency_ms,
        )

@mcp.tool()
def generate_invoice_audit_report(completed_jobs_json: str, invoices_json: str) -> str:
    """
    Cross-references completed jobs with Xero invoices (Finance Sentinel requirement).
    
    Args:
        completed_jobs_json: JSON string with completed Monday items (last 24h)
        invoices_json: JSON string with Xero invoices
    
    Returns:
        JSON report identifying jobs without invoices
    """
    tool_name = "generate_invoice_audit_report"
    start_time = time.time()
    
    try:
        completed_jobs = json.loads(completed_jobs_json)
        invoices = json.loads(invoices_json)
        
        if not isinstance(completed_jobs, list):
            completed_jobs = []
        if not isinstance(invoices, list):
            invoices = []
        
        # Create lookup of invoice references
        invoice_refs = set()
        for inv in invoices:
            # Extract reference/invoice number
            ref = inv.get("InvoiceNumber") or inv.get("Reference") or inv.get("invoice_number")
            if ref:
                invoice_refs.add(str(ref).strip())
        
        # Check each completed job for matching invoice
        missing_invoice_items = []
        for job in completed_jobs:
            job_id = str(job.get("id", ""))
            job_name = job.get("name", "")
            
            # Check if job ID or name appears in invoice references
            has_invoice = False
            for ref in invoice_refs:
                if job_id in ref or job_name.lower() in ref.lower():
                    has_invoice = True
                    break
            
            if not has_invoice:
                missing_invoice_items.append({
                    "item_id": job_id,
                    "item_name": job_name,
                    "action": "CREATE_INVOICE"
                })
        
        report = {
            "status": "success",
            "report_type": "invoice_audit",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "completed_jobs": len(completed_jobs),
            "invoices_found": len(invoices),
            "missing_invoices": len(missing_invoice_items),
            "missing_invoice_items": missing_invoice_items,
            "summary": f"Invoice Audit: {len(missing_invoice_items)} completed jobs without invoices"
        }
        
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "info",
            tool=tool_name,
            event=tool_name,
            status="success",
            latency_ms=latency_ms,
            completed_jobs=len(completed_jobs),
            missing_invoices=len(missing_invoice_items),
        )
        
        return json.dumps(report)
        
    except json.JSONDecodeError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="json_error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INVALID_JSON",
            message=f"Invalid JSON: {str(e)}",
            latency_ms=latency_ms,
        )
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INTERNAL_ERROR",
            message=str(e),
            latency_ms=latency_ms,
        )

@mcp.tool()
def format_report_for_email(report_json: str) -> str:
    """
    Formats a JSON report as human-readable text suitable for email.
    
    Args:
        report_json: JSON string containing report data
    
    Returns:
        JSON with formatted text and metadata for email sending
    """
    tool_name = "format_report_for_email"
    start_time = time.time()
    
    try:
        report_data = json.loads(report_json)
        report_type = report_data.get("report_type", "unknown")
        
        formatted_text = format_report_as_text(report_data, report_type)
        
        # Generate email subject
        subject_map = {
            "board_health": "Board Health Report",
            "daily_summary": f"Daily Summary - {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
            "weekly_summary": f"Weekly Summary - {report_data.get('week_start', 'N/A')}",
            "invoice_audit": "Invoice Audit Report"
        }
        subject = subject_map.get(report_type, "Acme Property Services Report")
        
        result = {
            "status": "success",
            "email_subject": subject,
            "email_body": formatted_text,
            "report_type": report_type
        }
        
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "info",
            tool=tool_name,
            event=tool_name,
            status="success",
            latency_ms=latency_ms,
            report_type=report_type,
        )
        
        return json.dumps(result)
        
    except json.JSONDecodeError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="json_error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INVALID_JSON",
            message=f"Invalid JSON: {str(e)}",
            latency_ms=latency_ms,
        )
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INTERNAL_ERROR",
            message=str(e),
            latency_ms=latency_ms,
        )

@mcp.tool()
def calculate_completion_rate(board_items_json: str, days: int = 7) -> str:
    """
    Calculate job completion rate over a time period.
    
    Args:
        board_items_json: JSON string containing board items
        days: Number of days to analyze (default: 7)
    
    Returns:
        JSON with completion metrics
    """
    tool_name = "calculate_completion_rate"
    start_time = time.time()
    
    try:
        items = json.loads(board_items_json)
        
        if not isinstance(items, list):
            latency_ms = int((time.time() - start_time) * 1000)
            _log(
                "warning",
                tool=tool_name,
                event=tool_name,
                status="invalid_input",
                latency_ms=latency_ms,
                message="Expected list of items",
            )
            return _error_response(
                tool=tool_name,
                error_code="INVALID_INPUT_TYPE",
                message="Expected list of items",
                latency_ms=latency_ms,
            )

        def _extract_best_effort_date_utc(item: Dict[str, Any]) -> datetime | None:
            """Try to infer a date from common Monday-style fields."""
            column_values = item.get("column_values", []) or []
            for col in column_values:
                col_id = str(col.get("id", "")).lower()
                if "date" not in col_id and "day" not in col_id:
                    continue
                text = str(col.get("text", "")).strip()
                if not text:
                    continue
                # Common date formats seen in Monday "date" columns.
                for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
                    try:
                        dt = datetime.strptime(text, fmt)
                        return dt.replace(tzinfo=timezone.utc)
                    except ValueError:
                        continue
            return None

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        any_dates = False
        filtered_items: List[Dict[str, Any]] = []
        for item in items:
            item_dt = _extract_best_effort_date_utc(item)
            if item_dt is None:
                continue
            any_dates = True
            if item_dt >= cutoff:
                filtered_items.append(item)

        items_for_metrics = filtered_items if any_dates else items
        metrics = calculate_report_metrics(items_for_metrics)
        
        total = metrics['total_items']
        completed = metrics['completed_count']
        completion_rate = (completed / total * 100) if total > 0 else 0
        
        report = {
            "status": "success",
            "period_days": days,
            "date_filter_applied": any_dates,
            "items_considered": len(items_for_metrics),
            "total_items": total,
            "completed_items": completed,
            "completion_rate_percent": round(completion_rate, 1),
            "in_progress": metrics['in_progress_count'],
            "pending": metrics['pending_count'],
            "needs_review": metrics['needs_review_count']
        }
        if not any_dates:
            report["warning"] = (
                "No date-like column detected in items. "
                "Computed completion rate across all provided items."
            )
        
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "info",
            tool=tool_name,
            event=tool_name,
            status="success",
            latency_ms=latency_ms,
            completion_rate=completion_rate,
            period_days=days,
            date_filter_applied=any_dates,
            items_considered=len(items_for_metrics),
        )
        
        return json.dumps(report)
        
    except json.JSONDecodeError as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="json_error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INVALID_JSON",
            message=f"Invalid JSON: {str(e)}",
            latency_ms=latency_ms,
        )
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        _log(
            "error",
            tool=tool_name,
            event=tool_name,
            status="error",
            latency_ms=latency_ms,
            error=str(e),
        )
        return _error_response(
            tool=tool_name,
            error_code="INTERNAL_ERROR",
            message=str(e),
            latency_ms=latency_ms,
        )

# --- STARTUP HEALTH CHECK ---
def startup_health_check() -> bool:
    """
    Phase 3 requirement: Startup health check.
    Verify server can start successfully.
    """
    try:
        # Basic sanity checks
        test_data = json.dumps([{"id": "1", "name": "test", "status": {"text": "Done"}, "column_values": []}])
        result = detect_missing_fields(test_data, "test_field")
        result_data = json.loads(result)
        
        if result_data.get("status") != "success":
            _log(
                "error",
                tool="startup_health_check",
                event="health_check_failed",
                status="critical",
                error="Tool test failed",
            )
            return False
        
        _log(
            "info",
            tool="startup_health_check",
            event="health_check_passed",
            status="healthy",
        )
        return True
        
    except Exception as e:
        _log(
            "error",
            tool="startup_health_check",
            event="health_check_failed",
            status="critical",
            error=str(e),
        )
        return False

if __name__ == "__main__":
    _log(
        "info",
        tool="server",
        event="server_start",
        status="starting",
        message="Reporting MCP server starting",
    )
    
    # Phase 3 requirement: Startup health check
    if not startup_health_check():
        _log(
            "error",
            tool="server",
            event="server_startup_aborted",
            status="critical",
            message="Health check failed. Server will not start.",
        )
        sys.exit(1)
    
    _log(
        "info",
        tool="server",
        event="server_ready",
        status="ready",
    )
    mcp.run()
