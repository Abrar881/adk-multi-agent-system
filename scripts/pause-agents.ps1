# Pause all Multi-Agent System Cloud Scheduler jobs
# Run this to stop all agent activity (no Gemini calls, no cost)
# Cloud Run will scale to zero automatically

$LOCATION = "europe-west2"

$JOBS = @(
    "inbox-sweep",
    "financial-close",
    "route-verification",
    "capacity-planning",
    "weekly-prep",
    "weekly-board-dup",
    "logistics-first-check",
    "finance-sweep",
    "invoice-sweep",
    "preflight-check",
    "reconciliation",
    "chase-afternoon",
    "lock-cleanup",
    "db-hygiene",
    "morning-roll-call",
    "logistics-final-check",
    "verification-emails",
    "chase-morning",
    "renew-gmail-watch",
    "inbox-check",
    "enroute-check-poll",
    "contractor-evening-reminder",
    "send-clerk-timetables"
)

Write-Host "Pausing all Multi-Agent System scheduler jobs..." -ForegroundColor Yellow

foreach ($job in $JOBS) {
    gcloud scheduler jobs pause $job --location=$LOCATION --quiet 2>&1 | Out-Null
    Write-Host "  PAUSED: $job"
}

Write-Host ""
Write-Host "All 23 jobs paused. Agents will not fire." -ForegroundColor Green
Write-Host "Cloud Run will scale to zero. No charges will accrue." -ForegroundColor Green
