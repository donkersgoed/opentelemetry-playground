import os
from typing import Any, Optional
from contextlib import contextmanager

# Related third party imports
import boto3
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    BatchSpanProcessor,
)

# Local application/library specific imports
import lambda_cache

HONEYCOMB_ENDPOINT = os.environ.get("HONEYCOMB_ENDPOINT")
HONEYCOMB_KEY_SECRET = os.environ.get("HONEYCOMB_KEY_SECRET")

if HONEYCOMB_KEY_SECRET:
    secretsmanager_client = boto3.client("secretsmanager")
    HONEYCOMB_KEY = secretsmanager_client.get_secret_value(
        SecretId=HONEYCOMB_KEY_SECRET
    )["SecretString"]


class OtelHelper:
    def __init__(self, service_name, root_span_name) -> None:
        self._root_span_name = root_span_name
        trace.set_tracer_provider(
            tracer_provider=TracerProvider(
                resource=Resource.create({"service.name": service_name})
            )
        )
        trace.get_tracer_provider().add_span_processor(
            BatchSpanProcessor(
                ConsoleSpanExporter(
                    formatter=lambda span: span.to_json(indent=None) + os.linesep
                )
            )
        )
        if HONEYCOMB_ENDPOINT and HONEYCOMB_KEY_SECRET:
            trace.get_tracer_provider().add_span_processor(
                BatchSpanProcessor(
                    OTLPSpanExporter(
                        endpoint=HONEYCOMB_ENDPOINT,
                        headers={"x-honeycomb-team": HONEYCOMB_KEY},
                    )
                )
            )
        self._tracer = trace.get_tracer(__name__)

    @contextmanager
    def start_root_span(self, context: Any, user_attributes: Optional[dict] = None):
        lambda_request_id = context.aws_request_id.lower()
        if not user_attributes:
            user_attributes = {}
        default_attributes = {
            # If there is one invocation ID in the list (our own), this is a cold start
            "faas.coldstart": len(
                lambda_cache.get_execution_environment_cache_entry("invocation_ids")
            )
            == 1,
            "faas.execution": lambda_request_id,
            "faas.memory_limit_in_mb": context.memory_limit_in_mb,
            "faas.name": context.function_name,
            "faas.version": context.function_version,
        }
        merged_attributes = default_attributes | user_attributes
        lambda_cache.set_invocation_cache_entry("otel_attributes", merged_attributes)
        with self._tracer.start_as_current_span(
            name=self._root_span_name,
            attributes=merged_attributes,
        ):
            yield
            # This is necessary to send traces to the ADOT collector
            trace.get_tracer_provider().force_flush()
