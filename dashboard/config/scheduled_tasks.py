"""Scheduled task registry + cron helpers for the dashboard.

Defines the 19 client-editable business tasks (18 LLM + 1 direct) and provides two-way conversion
between the UI schedule spec (time+days OR interval) and Cloud Scheduler cron strings.

System jobs (lock_cleanup, db_hygiene, renew_gmail_watch) are intentionally
excluded from EDITABLE_TASKS so PUT /api/scheduled-tasks rejects them.
"""
import os

# ---------------------------------------------------------------------------
# Cloud Scheduler connection constants
# ---------------------------------------------------------------------------
PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "YOUR_GCP_PROJECT_ID")
LOCATION = os.environ.get("CLOUD_SCHEDULER_LOCATION", "europe-west2")
TIMEZONE = "Europe/London"

# ---------------------------------------------------------------------------
# Validation sets
# ---------------------------------------------------------------------------
ALLOWED_MIN: frozenset[int] = frozenset({15, 30})
ALLOWED_HR: frozenset[int] = frozenset({1, 2, 4, 6, 12})

# UI day indices: 0=Mon â€¦ 6=Sun  â†’  cron dow: Mon=1 â€¦ Sat=6, Sun=0
_UI_TO_CRON_DOW: dict[int, int] = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 0}
_CRON_TO_UI_DOW: dict[int, int] = {v: k for k, v in _UI_TO_CRON_DOW.items()}
_UI_DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# ---------------------------------------------------------------------------
# Editable task registry (18 business tasks)
# Keys must match task_name values in main.py run_scheduled_task() and
# _TASK_AGENT_MAP. Job names must match Cloud Scheduler job IDs.
# ---------------------------------------------------------------------------
EDITABLE_TASKS: dict[str, dict] = {
    "morning_roll_call": {
        "job": "morning-roll-call",
        "label": "Morning Inspector Check-In",
        "agent": "ops",
        "description": "Checks which inspectors are en route to their jobs. Alerts admin if anyone hasn't confirmed by their expected start time.",
        "default": {"mode": "time", "time": "10:00", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": True,
        "min_interval_minutes": 15,
    },
    "preflight_check": {
        "job": "preflight-check",
        "label": "Tomorrow's Payment Check",
        "agent": "finance",
        "description": "Scans jobs scheduled for tomorrow and flags any where payment hasn't been received (24-hour payment rule).",
        "default": {"mode": "time", "time": "09:00", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": False,
        "min_interval_minutes": 60,
    },
    "chase_morning": {
        "job": "chase-morning",
        "label": "Chase Unpaid Invoices (Morning)",
        "agent": "finance",
        "description": "Reviews all outstanding invoices and drafts chaser emails at the right level of urgency for your review.",
        "default": {"mode": "time", "time": "09:30", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": False,
        "min_interval_minutes": 60,
    },
    "reconciliation": {
        "job": "reconciliation",
        "label": "Match Payments to Board",
        "agent": "finance",
        "description": "Checks Xero for new payments received and updates the Monday board to mark those jobs as paid.",
        "default": {"mode": "time", "time": "10:30", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": False,
        "min_interval_minutes": 60,
    },
    "verification_emails": {
        "job": "verification-emails",
        "label": "Booking Confirmation Check",
        "agent": "dispatcher",
        "description": "Confirms that all booking confirmation emails for today's jobs have been sent to clients.",
        "default": {"mode": "time", "time": "16:00", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": False,
        "min_interval_minutes": 60,
    },
    "chase_afternoon": {
        "job": "chase-afternoon",
        "label": "Chase Unpaid Invoices (Afternoon)",
        "agent": "finance",
        "description": "Second daily chase for landlord invoices and any remaining outstanding balances.",
        "default": {"mode": "time", "time": "15:00", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": False,
        "min_interval_minutes": 60,
    },
    "logistics_first_check": {
        "job": "logistics-first-check",
        "label": "Schedule Feasibility Check",
        "agent": "ops",
        "description": "Reviews tomorrow's timetable for any clashes, missing contractors, or jobs that can't be covered.",
        "default": {"mode": "time", "time": "15:30", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": True,
        "min_interval_minutes": 15,
    },
    "finance_sweep": {
        "job": "finance-sweep",
        "label": "Accounts Email Check",
        "agent": "finance",
        "description": "Scans the accounts inbox for payment queries or finance-related emails and drafts a response.",
        "default": {"mode": "time", "time": "17:00", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": False,
        "min_interval_minutes": 60,
    },
    "logistics_final_check": {
        "job": "logistics-final-check",
        "label": "Schedule Final Sign-Off",
        "agent": "ops",
        "description": "Final check of tomorrow's timetable to confirm all contractors are in place and routes are clear.",
        "default": {"mode": "time", "time": "17:30", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": True,
        "min_interval_minutes": 15,
    },
    "inbox_sweep": {
        "job": "inbox-sweep",
        "label": "End-of-Day Email Catch-Up",
        "agent": "dispatcher",
        "description": "Drafts replies to any emails that weren't handled during the day. End-of-day safety net.",
        "default": {"mode": "time", "time": "17:35", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": False,
        "min_interval_minutes": 60,
    },
    "capacity_planning": {
        "job": "capacity-planning",
        "label": "Fill Scheduling Gaps",
        "agent": "ops",
        "description": "Identifies gaps in upcoming jobs and finds contractor cover to maximise revenue.",
        "default": {"mode": "time", "time": "18:30", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": True,
        "min_interval_minutes": 15,
    },
    "financial_close": {
        "job": "financial-close",
        "label": "End-of-Day Invoice Check",
        "agent": "finance",
        "description": "Verifies all invoices for today's completed jobs have been raised; creates any that are missing.",
        "default": {"mode": "time", "time": "19:00", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": False,
        "min_interval_minutes": 60,
    },
    "route_verification": {
        "job": "route-verification",
        "label": "Verify Tomorrow's Access & Keys",
        "agent": "ops",
        "description": "Checks key arrangements, entry codes, and access notes are in place for tomorrow's jobs.",
        "default": {"mode": "time", "time": "19:45", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": True,
        "min_interval_minutes": 15,
    },
    "weekly_board_dup": {
        "job": "weekly-board-dup",
        "label": "Create Next Week's Board",
        "agent": "ops",
        "description": "Duplicates the Daily Tasks board ready for the coming week (runs every Sunday evening).",
        "default": {"mode": "time", "time": "20:30", "days": [6]},  # Sun only
        "allow_interval": True,
        "min_interval_minutes": 15,
    },
    "weekly_prep": {
        "job": "weekly-prep",
        "label": "Prepare Tomorrow's Schedule",
        "agent": "ops",
        "description": "Reviews and prepares the following day's schedule, flagging any gaps or issues to resolve.",
        "default": {"mode": "time", "time": "20:30", "days": [0, 1, 2, 3, 4, 5]},  # Monâ€“Sat
        "allow_interval": True,
        "min_interval_minutes": 15,
    },
    "send_clerk_timetables": {
        "job": "send-clerk-timetables",
        "label": "Send Clerk Timetables",
        "agent": "ops",
        "description": "Sends tomorrow's job schedule to each clerk via WhatsApp at the end of the day.",
        "default": {"mode": "time", "time": "20:00", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": False,
        "min_interval_minutes": 60,
    },
    "invoice_sweep": {
        "job": "invoice-sweep",
        "label": "Create Pending Invoices",
        "agent": "finance",
        "description": "Creates any Xero invoices that weren't generated automatically during booking confirmation.",
        "default": {"mode": "interval", "interval_value": 4, "interval_unit": "hours"},
        "allow_interval": True,
        "min_interval_minutes": 60,
    },
    "inbox_check": {
        "job": "inbox-check",
        "label": "Inbox Safety Net",
        "agent": "dispatcher",
        "description": "Periodic check of email and WhatsApp for any messages missed by the real-time handler.",
        "default": {"mode": "interval", "interval_value": 12, "interval_unit": "hours"},
        "allow_interval": True,
        "min_interval_minutes": 60,
    },
    "contractor_evening_reminder": {
        "job": "contractor-evening-reminder",
        "label": "Contractor Evening Reminder",
        "agent": "ops",
        "description": "Sends WhatsApp confirmation requests to contractors for tomorrow's jobs.",
        "default": {"mode": "time", "time": "18:15", "days": [0, 1, 2, 3, 4, 5, 6]},
        "allow_interval": False,
        "min_interval_minutes": 60,
    },
}

# Fast membership set for PUT guard
JOB_NAMES: frozenset[str] = frozenset(t["job"] for t in EDITABLE_TASKS.values())


# ---------------------------------------------------------------------------
# Cron helpers
# ---------------------------------------------------------------------------

def schedule_to_cron(spec: dict, task_cfg: dict | None = None) -> str:
    """Convert a UI schedule spec to a Cloud Scheduler cron string.

    spec fields:
      mode: "time" | "interval"
      -- time mode --
      time: "HH:MM"
      days: list of UI day indices (0=Mon â€¦ 6=Sun); all 7 â†’ "*"
      -- interval mode --
      interval_value: int (in ALLOWED_MIN or ALLOWED_HR)
      interval_unit: "minutes" | "hours"

    task_cfg: optional task config dict with allow_interval and min_interval_minutes keys.
              If provided, validates interval constraints for this specific task.

    Returns a 5-field cron string.
    Raises ValueError on invalid input.
    """
    mode = spec.get("mode")

    if mode == "interval":
        val = int(spec["interval_value"])
        unit = spec["interval_unit"]
        if task_cfg is not None:
            if not task_cfg.get("allow_interval", True):
                raise ValueError("This automation can only run at fixed times")
            min_minutes = task_cfg.get("min_interval_minutes", 0)
            if min_minutes > 0:
                if unit == "minutes":
                    requested_minutes = val
                else:  # hours
                    requested_minutes = val * 60
                if requested_minutes < min_minutes:
                    raise ValueError(
                        f"Minimum interval for this automation is every "
                        f"{min_minutes} minutes"
                    )
        if unit == "minutes":
            if val not in ALLOWED_MIN:
                raise ValueError(f"interval_value must be one of {sorted(ALLOWED_MIN)} for minutes")
            return f"*/{val} * * * *"
        elif unit == "hours":
            if val not in ALLOWED_HR:
                raise ValueError(f"interval_value must be one of {sorted(ALLOWED_HR)} for hours")
            if val == 1:
                return "0 * * * *"
            return f"0 */{val} * * *"
        else:
            raise ValueError(f"interval_unit must be 'minutes' or 'hours', got '{unit}'")

    elif mode == "time":
        time_str = spec.get("time", "")
        parts = time_str.split(":")
        if len(parts) != 2:
            raise ValueError(f"time must be HH:MM, got '{time_str}'")
        hour, minute = int(parts[0]), int(parts[1])
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"time out of range: {time_str}")

        days: list[int] = spec.get("days", list(range(7)))
        if not days:
            raise ValueError("days must have at least one day selected")

        # Normalise and validate
        ui_days = sorted(set(int(d) for d in days))
        for d in ui_days:
            if d not in _UI_TO_CRON_DOW:
                raise ValueError(f"invalid day index {d} (must be 0â€“6)")

        if len(ui_days) == 7:
            dow = "*"
        else:
            cron_days = sorted(_UI_TO_CRON_DOW[d] for d in ui_days)
            dow = ",".join(str(d) for d in cron_days)

        return f"{minute} {hour} * * {dow}"

    else:
        raise ValueError(f"mode must be 'time' or 'interval', got '{mode!r}'")


def cron_to_schedule(cron: str) -> dict:
    """Parse a Cloud Scheduler cron string into a UI schedule spec.

    Returns a dict with same shape as schedule_to_cron() input.
    Falls back to time mode with best-effort parse on unrecognised patterns.
    """
    parts = cron.strip().split()
    if len(parts) != 5:
        # Unrecognised â€” return raw cron as opaque time mode
        return {"mode": "time", "time": "00:00", "days": list(range(7))}

    minute_f, hour_f, dom_f, month_f, dow_f = parts

    # --- Interval: */N * * * * (minutes) ---
    if minute_f.startswith("*/") and hour_f == "*":
        try:
            val = int(minute_f[2:])
            if val > 0:
                return {"mode": "interval", "interval_value": val, "interval_unit": "minutes"}
        except ValueError:
            pass

    # --- Interval: 0 */N * * * or 0 * * * * (hours) ---
    if minute_f == "0" and dom_f == "*" and month_f == "*" and dow_f == "*":
        if hour_f == "*":
            return {"mode": "interval", "interval_value": 1, "interval_unit": "hours"}
        if hour_f.startswith("*/"):
            try:
                val = int(hour_f[2:])
                if val > 0:
                    return {"mode": "interval", "interval_value": val, "interval_unit": "hours"}
            except ValueError:
                pass

    # --- Time mode ---
    try:
        minute = int(minute_f)
        hour = int(hour_f)
    except ValueError:
        return {"mode": "time", "time": "00:00", "days": list(range(7))}

    time_str = f"{hour:02d}:{minute:02d}"

    # Parse dow field
    if dow_f == "*":
        ui_days = list(range(7))
    else:
        cron_days: list[int] = []
        for segment in dow_f.split(","):
            if "-" in segment:
                lo_s, hi_s = segment.split("-", 1)
                lo, hi = int(lo_s), int(hi_s)
                cron_days.extend(range(lo, hi + 1))
            else:
                cron_days.append(int(segment))
        # Convert cron dow â†’ UI day indices; skip unknown cron days
        ui_days = sorted(
            _CRON_TO_UI_DOW[d] for d in set(cron_days) if d in _CRON_TO_UI_DOW
        )

    return {"mode": "time", "time": time_str, "days": ui_days}
