"""OpenTelemetry setup for Multi-Agent System (Wave 5).

Call setup_telemetry() once at server startup to enable:
- Cloud Trace span export (GCP trace viewer)
- Structured span attributes for each ADK invocation

Deps (add to requirements.txt):
  opentelemetry-sdk
  opentelemetry-exporter-gcp-trace
"""
import logging
import os

logger = logging.getLogger(__name__)


def setup_telemetry(project_id: str | None = None) -> bool:
    """Configure OpenTelemetry with GCP Cloud Trace exporter.

    Returns True if telemetry was configured successfully, False otherwise.
    Safe to call even if the optional deps are not installed â€” logs a warning
    and returns False rather than raising.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter

        pid = project_id or os.environ.get("GOOGLE_CLOUD_PROJECT")
        if not pid:
            logger.warning("telemetry: GOOGLE_CLOUD_PROJECT not set, skipping Cloud Trace setup")
            return False

        provider = TracerProvider()
        exporter = CloudTraceSpanExporter(project_id=pid)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

        logger.info(f"telemetry: Cloud Trace configured for project {pid}")
        return True

    except ImportError:
        logger.warning(
            "telemetry: opentelemetry-exporter-gcp-trace not installed. "
            "Add it to requirements.txt for Cloud Trace support."
        )
        return False
    except Exception as exc:
        logger.warning(f"telemetry: setup failed: {exc}")
        return False
