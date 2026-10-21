# Multi-Agent System - Cloud Scheduler Smoke Test (PowerShell)
# Layer 3: Triggers every Cloud Scheduler job against the live Cloud Run service
# and checks Cloud Logging for success/error within a timeout window.
#
# Prerequisites:
#   - gcloud CLI authenticated with project access
#   - Cloud Scheduler jobs already created (run create_scheduler_jobs.ps1 first)
#   - Cloud Run service deployed and healthy
#
# Usage:
#   .\scripts\smoke_test_scheduler.ps1
#   .\scripts\smoke_test_scheduler.ps1 -Tasks "inbox_check","reconciliation"
#   .\scripts\smoke_test_scheduler.ps1 -LogWaitSeconds 60 -FailFast

param(
    [string[]]$Tasks = @(),           # Empty = run all tasks
    [int]$LogWaitSeconds = 45,        # Seconds to wait for Cloud Logging to capture result
    [switch]$FailFast,                # Stop on first failure
    [switch]$SkipLogCheck             # Run jobs but skip Cloud Logging verification
)

$PROJECT_ID = "YOUR_GCP_PROJECT_ID"
$REGION     = "europe-west2"
$SERVICE_URL = "https://multi-agent-system-828224712340.europe-west2.run.app"

# All 23 scheduler job names (must match create_scheduler_jobs.ps1)
$ALL_JOBS = @(
    "inbox-check",
    "renew-gmail-watch",
    "morning-roll-call",
    "preflight-check",
    "chase-morning",
    "reconciliation",
    "verification-emails",
    "chase-afternoon",
    "logistics-first-check",
    "finance-sweep",
    "invoice-sweep",
    "logistics-final-check",
    "inbox-sweep",
    "contractor-evening-reminder",
    "capacity-planning",
    "financial-close",
    "route-verification",
    "weekly-board-dup",
    "weekly-prep",
    "send-clerk-timetables",
    "enroute-check-poll",
    "lock-cleanup",
    "db-hygiene"
)

# Map job name -> task payload (matches create_scheduler_jobs.ps1)
$JOB_TO_TASK = @{
    "inbox-check"                  = "inbox_check"
    "renew-gmail-watch"            = "renew_gmail_watch"
    "morning-roll-call"            = "morning_roll_call"
    "preflight-check"              = "preflight_check"
    "chase-morning"                = "chase_morning"
    "reconciliation"               = "reconciliation"
    "verification-emails"          = "verification_emails"
    "chase-afternoon"              = "chase_afternoon"
    "logistics-first-check"        = "logistics_first_check"
    "finance-sweep"                = "finance_sweep"
    "invoice-sweep"                = "invoice_sweep"
    "logistics-final-check"        = "logistics_final_check"
    "inbox-sweep"                  = "inbox_sweep"
    "contractor-evening-reminder"  = "contractor_evening_reminder"
    "capacity-planning"            = "capacity_planning"
    "financial-close"              = "financial_close"
    "route-verification"           = "route_verification"
    "weekly-board-dup"             = "weekly_board_dup"
    "weekly-prep"                  = "weekly_prep"
    "send-clerk-timetables"        = "send_clerk_timetables"
    "enroute-check-poll"           = "enroute_check_poll"
    "lock-cleanup"                 = "lock_cleanup"
    "db-hygiene"                   = "db_hygiene"
}

# Resolve jobs to test
if ($Tasks.Count -gt 0) {
    # Convert task_name -> job-name format for user convenience
    $JobsToTest = $Tasks | ForEach-Object { $_ -replace "_", "-" }
} else {
    $JobsToTest = $ALL_JOBS
}

# Counters
$Passed  = 0
$Failed  = 0
$Skipped = 0
$Results = @()

Write-Host ""
Write-Host "=== Multi-Agent System Cloud Scheduler Smoke Test ===" -ForegroundColor Cyan
Write-Host "Project : $PROJECT_ID"
Write-Host "Region  : $REGION"
Write-Host "Jobs    : $($JobsToTest.Count) / $($ALL_JOBS.Count)"
Write-Host "Log wait: ${LogWaitSeconds}s per job"
Write-Host ""


function Test-HealthEndpoint {
    Write-Host "--- Pre-flight: checking /health ---" -ForegroundColor Cyan
    try {
        $token = (gcloud auth print-identity-token 2>$null)
        $resp = Invoke-WebRequest `
            -Uri "$SERVICE_URL/health" `
            -Headers @{ Authorization = "Bearer $token" } `
            -UseBasicParsing `
            -TimeoutSec 10
        if ($resp.StatusCode -eq 200) {
            Write-Host "  /health OK (200)" -ForegroundColor Green
            return $true
        }
    } catch {
        Write-Host "  /health FAILED: $_" -ForegroundColor Red
    }
    return $false
}


function Invoke-SchedulerJob {
    param([string]$JobName)

    Write-Host "  Triggering job: $JobName ..." -NoNewline
    $triggerTime = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

    gcloud scheduler jobs run $JobName `
        --project="$PROJECT_ID" `
        --location="$REGION" `
        --quiet 2>$null

    if ($LASTEXITCODE -eq 0) {
        Write-Host " triggered" -ForegroundColor Yellow
        return $triggerTime
    } else {
        Write-Host " TRIGGER FAILED" -ForegroundColor Red
        return $null
    }
}


function Wait-ForLogEntry {
    param(
        [string]$TaskName,
        [string]$AfterTime,
        [int]$WaitSeconds
    )

    Write-Host "  Waiting ${WaitSeconds}s for Cloud Logging ..." -NoNewline

    Start-Sleep -Seconds $WaitSeconds

    # Build log filter
    # We look for a structured log entry containing the task name from our orchestrator
    $filter = @"
resource.type="cloud_run_revision"
resource.labels.project_id="$PROJECT_ID"
jsonPayload.task="$TaskName"
timestamp>="$AfterTime"
"@

    $logOutput = gcloud logging read $filter `
        --project="$PROJECT_ID" `
        --limit=5 `
        --format="json" 2>$null

    if (-not $logOutput -or $logOutput -eq "[]") {
        # Fallback: check for any scheduled_task event after triggerTime
        $filter2 = @"
resource.type="cloud_run_revision"
resource.labels.project_id="$PROJECT_ID"
jsonPayload.event="scheduled_task"
timestamp>="$AfterTime"
"@
        $logOutput = gcloud logging read $filter2 `
            --project="$PROJECT_ID" `
            --limit=5 `
            --format="json" 2>$null
    }

    if ($logOutput -and $logOutput -ne "[]") {
        # Check for error status in any entry
        $entries = $logOutput | ConvertFrom-Json 2>$null
        if ($entries) {
            $errorEntry = $entries | Where-Object {
                $_.jsonPayload.status -eq "error" -or
                $_.severity -eq "ERROR"
            }
            if ($errorEntry) {
                Write-Host " LOGGED (ERROR)" -ForegroundColor Red
                return "error"
            }
            Write-Host " LOGGED (OK)" -ForegroundColor Green
            return "success"
        }
    }

    Write-Host " NO LOG FOUND (timeout)" -ForegroundColor DarkYellow
    return "timeout"
}


function Test-ScheduleEndpointDirect {
    <#
    .SYNOPSIS
    Direct HTTP POST to /schedule â€” faster than waiting for Scheduler to fire.
    Used as a secondary verification when log check times out.
    #>
    param([string]$TaskName)

    try {
        $token = (gcloud auth print-identity-token 2>$null)
        $body  = '{"task":"' + $TaskName + '"}'
        $resp  = Invoke-WebRequest `
            -Uri "$SERVICE_URL/schedule" `
            -Method POST `
            -Body $body `
            -ContentType "application/json" `
            -Headers @{ Authorization = "Bearer $token" } `
            -UseBasicParsing `
            -TimeoutSec 120

        if ($resp.StatusCode -eq 200) {
            $data = $resp.Content | ConvertFrom-Json
            return $data.status
        }
    } catch {
        return "http_error"
    }
    return "unknown"
}


# ============================================================
# MAIN TEST LOOP
# ============================================================

$healthy = Test-HealthEndpoint
if (-not $healthy) {
    Write-Host ""
    Write-Host "ABORT: Service is not healthy. Deploy or check Cloud Run logs first." -ForegroundColor Red
    exit 1
}

Write-Host ""

foreach ($job in $JobsToTest) {
    $taskName = $JOB_TO_TASK[$job]
    if (-not $taskName) {
        Write-Host "SKIP (unknown job): $job" -ForegroundColor DarkYellow
        $Skipped++
        continue
    }

    Write-Host "[$job]" -ForegroundColor Cyan

    # Step 1: Trigger via Cloud Scheduler
    $triggerTime = Invoke-SchedulerJob -JobName $job

    if (-not $triggerTime) {
        $status = "trigger_failed"
        Write-Host "  RESULT: FAIL (trigger failed)" -ForegroundColor Red
        $Failed++
    } elseif ($SkipLogCheck) {
        # When skipping log check, hit /schedule directly for immediate result
        Write-Host "  Direct HTTP check (SkipLogCheck) ..." -NoNewline
        $httpStatus = Test-ScheduleEndpointDirect -TaskName $taskName
        if ($httpStatus -eq "success") {
            Write-Host " OK" -ForegroundColor Green
            $status = "success"
            $Passed++
        } else {
            Write-Host " FAIL ($httpStatus)" -ForegroundColor Red
            $status = $httpStatus
            $Failed++
        }
    } else {
        # Step 2: Poll Cloud Logging for the result
        $logStatus = Wait-ForLogEntry -TaskName $taskName -AfterTime $triggerTime -WaitSeconds $LogWaitSeconds

        if ($logStatus -eq "success") {
            $status = "success"
            $Passed++
            Write-Host "  RESULT: PASS" -ForegroundColor Green
        } elseif ($logStatus -eq "timeout") {
            # Fallback: direct HTTP call
            Write-Host "  Fallback: direct /schedule POST ..." -NoNewline
            $httpStatus = Test-ScheduleEndpointDirect -TaskName $taskName
            if ($httpStatus -eq "success") {
                Write-Host " OK (via direct call)" -ForegroundColor Green
                $status = "success"
                $Passed++
            } else {
                Write-Host " FAIL ($httpStatus)" -ForegroundColor Red
                $status = "timeout_no_log"
                $Failed++
            }
        } else {
            $status = $logStatus
            $Failed++
            Write-Host "  RESULT: FAIL ($logStatus)" -ForegroundColor Red
        }
    }

    $Results += [PSCustomObject]@{
        Job    = $job
        Task   = $taskName
        Status = $status
    }

    Write-Host ""

    if ($FailFast -and $Failed -gt 0) {
        Write-Host "FailFast: stopping after first failure." -ForegroundColor Red
        break
    }
}


# ============================================================
# SUMMARY REPORT
# ============================================================

Write-Host "=== Smoke Test Summary ===" -ForegroundColor Cyan
Write-Host ""
Write-Host ("{0,-35} {1,-25} {2}" -f "Job", "Task", "Result")
Write-Host ("-" * 75)

foreach ($r in $Results) {
    $color = if ($r.Status -eq "success") { "Green" } `
             elseif ($r.Status -match "timeout|skip") { "DarkYellow" } `
             else { "Red" }
    Write-Host ("{0,-35} {1,-25} {2}" -f $r.Job, $r.Task, $r.Status.ToUpper()) -ForegroundColor $color
}

Write-Host ""
Write-Host "Passed : $Passed" -ForegroundColor Green
Write-Host "Failed : $Failed" -ForegroundColor $(if ($Failed -gt 0) { "Red" } else { "Green" })
Write-Host "Skipped: $Skipped" -ForegroundColor DarkYellow
Write-Host ""

if ($Failed -gt 0) {
    Write-Host "SMOKE TEST FAILED â€” check Cloud Logging for details:" -ForegroundColor Red
    Write-Host "  gcloud logging read 'resource.type=cloud_run_revision severity>=ERROR' --project=$PROJECT_ID --limit=20"
    exit 1
} else {
    Write-Host "All smoke tests passed." -ForegroundColor Green
    exit 0
}
