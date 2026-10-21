# Multi-Agent System - Google Cloud Scheduler Job Setup (PowerShell)
# Run this script ONLY for initial bootstrap or to recreate jobs from scratch.
# WARNING: Re-running this script deletes and recreates every job, which will
# overwrite any schedule changes made via the dashboard. Use the dashboard to
# adjust individual job schedules and enable/disable state.

$PROJECT_ID = "YOUR_GCP_PROJECT_ID"
$REGION = "europe-west2"
$SERVICE_URL = "https://multi-agent-system-828224712340.europe-west2.run.app"
$TIMEZONE = "Europe/London"
$SERVICE_ACCOUNT = "Acme-scheduler@$PROJECT_ID.iam.gserviceaccount.com"

Write-Host "=== Multi-Agent System Scheduler Setup ===" -ForegroundColor Cyan
Write-Host "Project: $PROJECT_ID"
Write-Host "Region: $REGION"
Write-Host "Service URL: $SERVICE_URL"
Write-Host "Timezone: $TIMEZONE"
Write-Host ""

# Create service account if it doesn't exist
Write-Host "=== Creating Service Account (if needed) ===" -ForegroundColor Cyan
gcloud iam service-accounts create Acme-scheduler `
    --display-name="Acme Scheduler Service Account" `
    --project="$PROJECT_ID" 2>$null
if ($LASTEXITCODE -ne 0) { Write-Host "Service account already exists, continuing..." }

# Grant scheduler service account permission to invoke Cloud Run
Write-Host ""
Write-Host "=== Granting Cloud Run Invoker Permission ===" -ForegroundColor Cyan
gcloud run services add-iam-policy-binding multi-agent-system `
    --member="serviceAccount:$SERVICE_ACCOUNT" `
    --role="roles/run.invoker" `
    --region="$REGION" `
    --project="$PROJECT_ID"

Write-Host ""
Write-Host "=== Granting Cloud Scheduler Admin to Dashboard SA ===" -ForegroundColor Cyan
# Acme-runner is the dashboard Cloud Run SA; it needs cloudscheduler.admin to
# call update_job / resume_job / pause_job from the Automation tab.
gcloud projects add-iam-policy-binding $PROJECT_ID `
    --member="serviceAccount:Acme-runner@$PROJECT_ID.iam.gserviceaccount.com" `
    --role="roles/cloudscheduler.admin"
Write-Host "  OK cloudscheduler.admin granted to Acme-runner" -ForegroundColor Green
# Acme-runner also needs actAs on Acme-scheduler because Cloud Scheduler jobs
# use OIDC auth (--oidc-service-account-email=Acme-scheduler); GCP validates
# actAs even for schedule-only updates.
gcloud iam service-accounts add-iam-policy-binding "Acme-scheduler@$PROJECT_ID.iam.gserviceaccount.com" `
    --member="serviceAccount:Acme-runner@$PROJECT_ID.iam.gserviceaccount.com" `
    --role="roles/iam.serviceAccountUser"
Write-Host "  OK iam.serviceAccountUser granted to Acme-runner on Acme-scheduler" -ForegroundColor Green

Write-Host ""
Write-Host "=== Creating Scheduler Jobs ===" -ForegroundColor Cyan

function Create-Job {
    param(
        [string]$Name,
        [string]$Schedule,
        [string]$Task
    )

    Write-Host "Creating job: $Name ($Schedule) -> $Task"

    # Delete existing job silently if it exists
    gcloud scheduler jobs delete $Name `
        --project="$PROJECT_ID" `
        --location="$REGION" `
        --quiet 2>$null

    # Write body to temp file to avoid shell quoting issues
    $tmpFile = [System.IO.Path]::GetTempFileName()
    $body = '{"task":"' + $Task + '"}'
    [System.IO.File]::WriteAllText($tmpFile, $body, [System.Text.Encoding]::UTF8)

    gcloud scheduler jobs create http $Name `
        --project="$PROJECT_ID" `
        --location="$REGION" `
        --schedule="$Schedule" `
        --time-zone="$TIMEZONE" `
        --uri="$SERVICE_URL/schedule" `
        --http-method=POST `
        --headers="Content-Type=application/json" `
        --message-body-from-file="$tmpFile" `
        --oidc-service-account-email="$SERVICE_ACCOUNT" `
        --max-retry-attempts=1 `
        --max-retry-duration=900s `
        --attempt-deadline=540s

    Remove-Item $tmpFile -Force

    if ($LASTEXITCODE -eq 0) {
        Write-Host "  OK Created $Name" -ForegroundColor Green
    } else {
        Write-Host "  FAILED $Name" -ForegroundColor Red
    }
}

# Periodic inbox check (12h safety net; Gmail push webhook handles real-time delivery)
Create-Job "inbox-check"           "0 */12 * * *" "inbox_check"

# Daily Gmail push watch renewal (watch expires every 7 days; daily renewal is idempotent)
Create-Job "renew-gmail-watch"     "0 6 * * *"   "renew_gmail_watch"

# Morning tasks
Create-Job "morning-roll-call"     "0 10 * * *"  "morning_roll_call"
Create-Job "preflight-check"       "0 9 * * *"   "preflight_check"
Create-Job "chase-morning"         "30 9 * * *"  "chase_morning"
Create-Job "reconciliation"        "30 10 * * *" "reconciliation"
Create-Job "verification-emails"   "0 16 * * *"  "verification_emails"

# Afternoon tasks
Create-Job "chase-afternoon"       "0 15 * * *"  "chase_afternoon"
Create-Job "logistics-first-check" "30 15 * * *" "logistics_first_check"
Create-Job "finance-sweep"         "0 17 * * *"  "finance_sweep"
Create-Job "invoice-sweep"         "0 */4 * * *" "invoice_sweep"
Create-Job "logistics-final-check" "30 17 * * *" "logistics_final_check"
Create-Job "inbox-sweep"           "35 17 * * *" "inbox_sweep"

# Evening tasks
Create-Job "contractor-evening-reminder" "15 18 * * *" "contractor_evening_reminder"
Create-Job "capacity-planning"     "30 18 * * *" "capacity_planning"
Create-Job "financial-close"       "0 19 * * *"  "financial_close"
Create-Job "route-verification"    "45 19 * * *" "route_verification"
Create-Job "weekly-board-dup"      "30 20 * * 0" "weekly_board_dup"
Create-Job "weekly-prep"           "30 20 * * 1-6" "weekly_prep"
Create-Job "send-clerk-timetables" "0 20 * * *"    "send_clerk_timetables"
Create-Job "enroute-check-poll"    "*/30 7-17 * * *" "enroute_check_poll"

# System maintenance
Create-Job "lock-cleanup"          "0 3 * * *"   "lock_cleanup"
Create-Job "db-hygiene"            "30 3 * * *"  "db_hygiene"

Write-Host ""
Write-Host "=== All jobs created! ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "To test a job manually:"
Write-Host "  gcloud scheduler jobs run morning-roll-call --location=$REGION"
Write-Host ""
Write-Host "To view all job statuses:"
Write-Host "  gcloud scheduler jobs list --location=$REGION"
