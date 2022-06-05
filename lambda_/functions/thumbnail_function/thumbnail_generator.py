import os

from opentelemetry import trace
from PIL import Image
import boto3

import lambda_cache


MAX_SIZE = (100, 100)
s3_client = boto3.client("s3")
tracer = trace.get_tracer(__name__)


class ThumbnailGenerator:
    @classmethod
    def generate_thumbnails(cls, lambda_event):
        s3_object_key = lambda_event["detail"]["object"]["key"]
        s3_object_size = lambda_event["detail"]["object"]["size"]
        s3_object_etag = lambda_event["detail"]["object"]["etag"]
        s3_bucket_name = lambda_event["detail"]["bucket"]["name"]

        shared_otel_attributes = lambda_cache.get_invocation_cache_entry(
            "otel_attributes"
        )
        record_attributes = {
            "app.source_image.s3_bucket": s3_bucket_name,
            "app.source_image.s3_object_key": s3_object_key,
            "app.source_image.object_size": s3_object_size,
            "app.source_image.etag": s3_object_etag,
        }
        span_attributes = shared_otel_attributes | record_attributes

        filename, extension = os.path.splitext(s3_object_key)
        local_filename = f"/tmp/{s3_object_etag}{extension}"
        local_thumbnail_filename = f"/tmp/{s3_object_etag}.thumbnail{extension}"
        s3_upload_key = filename.replace("uploads/", "thumbnails/") + ".jpg"

        # Fetch from S3
        with tracer.start_as_current_span(
            name="download_file", attributes=span_attributes
        ) as span:
            span.add_event(
                "Starting file download",
                attributes={"local_filename": local_filename},
            )

            s3_client.download_file(
                s3_bucket_name,
                s3_object_key,
                local_filename,
            )
            span.add_event("Completed file download")

        # Create Thumbnail
        with tracer.start_as_current_span(
            name="process_image", attributes=span_attributes
        ) as span:
            span.add_event("Starting thumbnail generation")
            with Image.open(local_filename) as im:
                im.thumbnail(MAX_SIZE)
                im.save(local_thumbnail_filename, "JPEG")

            span.add_event("Completed thumbnail generation")

        # Upload to S3
        with tracer.start_as_current_span(
            name="upload_to_s3", attributes=span_attributes
        ) as span:
            span.add_event(
                "Starting file upload",
                attributes={"s3_upload_key": s3_upload_key},
            )
            s3_client.upload_file(
                local_thumbnail_filename,
                s3_bucket_name,
                s3_upload_key,
            )
            span.add_event("Completed file upload")
