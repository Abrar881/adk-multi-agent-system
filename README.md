# ADK Multi-Agent System

Production multi-agent system built with Google ADK (Agent Development Kit) to automate operations for a property inventory company. Handles bookings, invoicing, scheduling, client communication, and knowledge retrieval — reducing manual admin by over 60%.

---

## Problem

Property inventory businesses run on high-volume, time-sensitive coordination: booking clerks, sending confirmations, chasing invoices, answering client queries, scheduling routes. Handling this manually across WhatsApp, email, and Monday.com creates bottlenecks. This system automates the full workflow with specialist AI agents connected via MCP servers to every business tool.

---

## Architecture

```
Inbound events (WhatsApp, email, Monday.com webhooks)
                    │
            ┌───────▼───────┐
            │  Dispatcher   │  ← Triage, routing, RAG knowledge queries
            │   (ADK Agent) │
            └──┬────┬────┬──┘
               │    │    │
    ┌──────────▼┐ ┌─▼──────────┐ ┌──────────────┐
    │  Finance  │ │    Ops     │ │  Reflection  │
    │   Agent   │ │   Agent    │ │    Agent     │
    └──────┬────┘ └─────┬──────┘ └──────────────┘
           │            │
  ┌────────▼────────────▼────────────────────────────┐
  │               MCP Servers                         │
  │  monday_server  │  xero_server  │  gmail_server   │
  │  whatsapp_server│  rag_server   │  ops_server     │
  │  reporting_server│ locking_server│ alerting       │
  └────────────────────────────────────────────────────┘
           │            │            │
    Monday.com        Xero        Twilio/Gmail
   (bookings, tasks,  (invoices,  (WhatsApp, email)
    contractors)      payments)
```

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Agent framework | Google ADK (Agent Development Kit) |
| LLM | Gemini 2.0 Flash (Vertex AI) |
| Context caching | Vertex AI prompt caching (ADK ContextCacheConfig) |
| MCP servers | Custom Python FastMCP servers |
| Project management | Monday.com API |
| Invoicing | Xero API (OAuth2) |
| WhatsApp | Twilio WhatsApp Business API |
| Email | Gmail API (OAuth2) + Pub/Sub push |
| Knowledge base | Vertex AI Search + RAG |
| Session storage | Firestore |
| Infrastructure | Google Cloud Run + Cloud Build |
| Dashboard | Flask + Chart.js |
| Language | Python 3.12 |

---

## Agents

### Dispatcher (`agents/dispatcher.py`)
The entry point for all inbound events. Classifies messages, routes to the right specialist agent, handles new bookings and amendments, answers client queries using RAG, and looks up data from Monday.com boards.

### Finance Agent (`agents/finance.py`, `agents/finance_workflows.py`)
Handles invoicing via Xero: creates invoices for completed jobs, reconciles payments, chases overdue invoices with automated follow-up sequences, and generates financial reports.

### Ops Agent (`agents/ops.py`)
Manages scheduling: checks contractor availability, assigns jobs, sends clerk timetables via WhatsApp, handles en-route status checks, and manages job rescheduling.

### Reflection Agent (`agents/reflection.py`)
Self-evaluation layer that audits agent outputs for accuracy, compliance, and quality before responses are sent to clients.

---

## MCP Servers

| Server | Capabilities |
|--------|-------------|
| `monday_server.py` | Read/write bookings, daily tasks, contractor records |
| `xero_server.py` | Create invoices, record payments, query accounts |
| `gmail_server.py` | Send and read emails, Pub/Sub push processing |
| `whatsapp_server.py` | Send WhatsApp templates, handle inbound messages |
| `rag_server.py` | Query Vertex AI knowledge base (pricing, T&Cs, FAQs) |
| `ops_server.py` | Availability checks, scheduling, route management |
| `reporting_server.py` | Generate operational reports and dashboards |
| `locking_server.py` | Distributed locks preventing concurrent job conflicts |
| `alerting.py` | Admin alerts and circuit-breaker notifications |

---

## Features

- **Multi-channel inbound**: handles WhatsApp messages, emails, and Monday.com webhooks from a single entry point
- **Intelligent routing**: dispatcher classifies intent and routes to the right specialist without hardcoded rules
- **RAG knowledge base**: agents answer pricing, T&C, and FAQ queries by retrieving from a structured Vertex AI index
- **Distributed locking**: prevents double-booking and concurrent modification conflicts across agents
- **Prompt caching**: static agent instructions are cached with Vertex AI ContextCacheConfig — reduces per-call token costs by ~80%
- **Circuit breakers**: MCP servers degrade gracefully under API failures, with automatic recovery
- **Role-based guardrails**: agents are restricted to their domain; cross-agent calls go through the dispatcher
- **PII redaction**: clerk names and personal data are redacted from outbound communications
- **Firestore sessions**: conversation history persists across ADK sessions for context continuity
- **Flask dashboard**: real-time operations dashboard showing bookings, revenue, contractor utilisation

---

## Deployment

Deployed to Google Cloud Run via Cloud Build CI/CD:

```bash
# Build and deploy
gcloud builds submit --config cloudbuild.yaml

# Required: set Cloud Run env vars
# See .env.example for all required variables
```

Cloud Build config handles:
1. Docker image build
2. Pushing to Artifact Registry
3. Deploying to Cloud Run with env vars

---

## Getting Started (local)

```bash
# Install dependencies
pip install -r requirements.txt

# Copy env template and fill in your values
cp .env.example .env

# Ingest knowledge base to Vertex AI
python ingest_knowledge.py

# Run the agent server
python server.py

# Or run the ADK dev UI
adk web
```

---

## Project Structure

```
agents/
  dispatcher.py        Triage agent — routing, bookings, RAG queries
  finance.py           Finance agent — invoicing, payment chasing
  finance_workflows.py Xero workflow orchestration
  ops.py               Ops agent — scheduling, clerk management
  reflection.py        Output quality evaluation agent
  _knowledge.py        RAG retrieval utilities

config/
  monday_config.py     Board IDs and column mappings (Firestore-backed)
  coverage.py          Service coverage areas
  dashboard_settings.py Admin phone/email config
  job_routing.py       Job type routing rules

servers/
  monday_server.py     Monday.com MCP server
  xero_server.py       Xero accounting MCP server
  gmail_server.py      Gmail MCP server
  whatsapp_server.py   Twilio WhatsApp MCP server
  rag_server.py        Vertex AI RAG MCP server
  ops_server.py        Scheduling and availability MCP server
  reporting_server.py  Analytics MCP server
  locking_server.py    Distributed lock MCP server
  alerting.py          Admin alert utilities
  circuit_breaker.py   API failure resilience

sessions/
  firestore_session.py ADK session persistence in Firestore

security/
  sanitize.py          PII redaction utilities
  tool_permissions.py  Agent-level tool access control
  tool_predicate.py    Runtime tool execution guardrails

dashboard/
  app.py               Flask operations dashboard
  templates/           Jinja2 HTML templates
  static/              CSS, JS, manifest

tests/
  eval/datasets/       ADK eval test cases for each agent
  test_*.py            Unit and integration tests

knowledge/
  company_overview.txt    Company description
  service_catalog.txt     Services list
  pricing_inventory.txt   Pricing tables
  terms_and_conditions.txt T&Cs
  coverage_areas.txt      Geographic coverage
  faq.txt                 Frequently asked questions

main.py               ADK orchestrator — initialises all agents + MCP servers
server.py             HTTP server — webhook endpoints for all inbound events
ingest_knowledge.py   Uploads knowledge/ files to Vertex AI Search
mcp_runtime.py        MCP server lifecycle management
system_state.py       Global runtime state and billing controls
telemetry.py          Cloud Logging structured telemetry
cloudbuild.yaml       GCP CI/CD pipeline
Dockerfile            Container definition
```
