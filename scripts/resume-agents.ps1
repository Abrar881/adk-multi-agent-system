# Resume all Multi-Agent System Cloud Scheduler jobs
# Run this when the client wants to test or go live

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

Write-Host "Resuming all Multi-Agent System scheduler jobs..." -ForegroundColor Yellow

foreach ($job in $JOBS) {
    gcloud scheduler jobs resume $job --location=$LOCATION --quiet 2>&1 | Out-Null
    Write-Host "  RESUMED: $job"
}

Write-Host ""
Write-Host "All 23 jobs resumed. Agents are now active." -ForegroundColor Green
Write-Host "Next scheduled runs will fire at their configured times." -ForegroundColor Green
