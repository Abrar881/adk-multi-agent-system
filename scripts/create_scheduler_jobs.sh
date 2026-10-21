#!/bin/bash
# Multi-Agent System - Google Cloud Scheduler Job Setup
# Run this script ONLY for initial bootstrap or to recreate jobs from scratch.
# WARNING: Re-running this script deletes and recreates every job, which will
# overwrite any schedule changes made via the dashboard. Use the dashboard to
# adjust individual job schedules and enable/disable state.
set -e

PROJECT_ID="YOUR_GCP_PROJECT_ID"
REGION="europe-west2"
SERVICE_URL="${CLOUD_RUN_URL:-https://multi-agent-system-828224712340.europe-west2.run.app}"
TIMEZONE="Europe/London"
SERVICE_ACCOUNT="Acme-scheduler@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=== Multi-Agent System Scheduler Setup ==="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Service URL: $SERVICE_URL"
echo "Timezone: $TIMEZONE"
echo ""

# Function to create or update a scheduler job
create_job() {
    local name=$1
    local schedule=$2
    local task=$3
    
    echo "Creating job: $name ($schedule) -> $task"
    
    # Delete existing job if it exists (to allow updates)
    gcloud scheduler jobs delete "$name" \
        --project="$PROJECT_ID" \
        --location="$REGION" \
        --quiet 2>/dev/null || true
    
    # Create the job
    gcloud scheduler jobs create http "$name" \
        --project="$PROJECT_ID" \
        --location="$REGION" \
        --schedule="$schedule" \
        --time-zone="$TIMEZONE" \
        --uri="${SERVICE_URL}/schedule" \
        --http-method=POST \
        --headers="Content-Type=application/json" \
        --message-body="{\"task\":\"${task}\"}" \
        --oidc-service-account-email="$SERVICE_ACCOUNT" \
        --attempt-deadline=540s \
        --max-retry-attempts=1 \
        --max-retry-duration=900s
    
    echo "  âœ“ Created $name"
}

echo ""
echo "=== Creating Service Account (if needed) ==="
gcloud iam service-accounts create Acme-scheduler \
    --display-name="Acme Scheduler Service Account" \
    --project="$PROJECT_ID" 2>/dev/null || echo "Service account already exists"

echo ""
echo "=== Granting Cloud Scheduler Admin to Dashboard SA ==="
# Acme-runner is the dashboard Cloud Run SA; it needs cloudscheduler.admin to
# call update_job / resume_job / pause_job from the Automation tab.
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:Acme-runner@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/cloudscheduler.admin"
echo "  âœ“ cloudscheduler.admin granted to Acme-runner"
# Acme-runner also needs actAs on Acme-scheduler because Cloud Scheduler jobs
# use OIDC auth (--oidc-service-account-email=Acme-scheduler); GCP validates
# actAs even for schedule-only updates.
gcloud iam service-accounts add-iam-policy-binding "Acme-scheduler@${PROJECT_ID}.iam.gserviceaccount.com" \
    --member="serviceAccount:Acme-runner@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/iam.serviceAccountUser"
echo "  âœ“ iam.serviceAccountUser granted to Acme-runner on Acme-scheduler"

echo ""
echo "=== Creating Scheduler Jobs ==="
echo ""

# Periodic inbox check (12h safety net; Gmail push webhook handles real-time delivery)
create_job "inbox-check" "0 */12 * * *" "inbox_check"

# Daily Gmail push watch renewal (watch expires every 7 days; daily renewal is idempotent)
create_job "renew-gmail-watch" "0 6 * * *" "renew_gmail_watch"

# Morning tasks
create_job "morning-roll-call" "0 10 * * *" "morning_roll_call"
create_job "preflight-check" "0 9 * * *" "preflight_check"
create_job "chase-morning" "30 9 * * *" "chase_morning"
create_job "reconciliation" "30 10 * * *" "reconciliation"
create_job "verification-emails" "0 16 * * *" "verification_emails"

# Afternoon tasks
create_job "chase-afternoon" "0 15 * * *" "chase_afternoon"
create_job "logistics-first-check" "30 15 * * *" "logistics_first_check"
create_job "finance-sweep" "0 17 * * *" "finance_sweep"
create_job "invoice-sweep" "0 */4 * * *" "invoice_sweep"
create_job "logistics-final-check" "30 17 * * *" "logistics_final_check"
create_job "inbox-sweep" "35 17 * * *" "inbox_sweep"

# Evening tasks
create_job "contractor-evening-reminder" "15 18 * * *" "contractor_evening_reminder"
create_job "capacity-planning" "30 18 * * *" "capacity_planning"
create_job "financial-close" "0 19 * * *" "financial_close"
create_job "route-verification" "45 19 * * *" "route_verification"
create_job "weekly-board-dup" "30 20 * * 0" "weekly_board_dup"
create_job "weekly-prep" "30 20 * * 1-6" "weekly_prep"
create_job "send-clerk-timetables" "0 20 * * *" "send_clerk_timetables"

# En-route check polling (every 30 min, 07:00-18:00) â€” system task, no LLM
create_job "enroute-check-poll" "*/30 7-17 * * *" "enroute_check_poll"

# System maintenance
create_job "lock-cleanup" "0 3 * * *" "lock_cleanup"
create_job "db-hygiene"   "30 3 * * *" "db_hygiene"

echo ""
echo "=== All jobs created! ==="
echo ""
echo "To test a job manually:"
echo "  gcloud scheduler jobs run morning-roll-call --location=$REGION"
echo ""
echo "To view job status:"
echo "  gcloud scheduler jobs list --location=$REGION"
