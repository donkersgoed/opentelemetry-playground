# Standard library imports
import os

# Local application/library specific imports
import lambda_cache
from otel_helper import OtelHelper
from thumbnail_generator import ThumbnailGenerator


lambda_helper = OtelHelper(
    service_name="opentelemetry_playgound",
    root_span_name="create_thumbnails_from_s3_events",
)


def event_handler(event, context):
    lambda_cache.initialize_invocation(context)
    with lambda_helper.start_root_span(
        context,
        user_attributes={
            # Set app.direct_export_to_honeycomb to True if we're using the local
            # exporter, and to False if we're just logging traces to CloudWatch.
            "app.direct_export_to_honeycomb": os.environ.get("HONEYCOMB_KEY_SECRET")
            is not None
        },
    ):
        ThumbnailGenerator.generate_thumbnails(event)
