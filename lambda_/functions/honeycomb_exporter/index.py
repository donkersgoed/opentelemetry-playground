# Standard library imports
import base64
import os
from copy import deepcopy
import dateutil.parser
import json
import zlib

# Related third party imports
import boto3
import requests
from google.protobuf.json_format import ParseDict
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)

HONEYCOMB_ENDPOINT = os.environ["HONEYCOMB_ENDPOINT"]
HONEYCOMB_KEY_SECRET = os.environ["HONEYCOMB_KEY_SECRET"]

secretsmanager_client = boto3.client("secretsmanager")
HONEYCOMB_KEY = secretsmanager_client.get_secret_value(SecretId=HONEYCOMB_KEY_SECRET)[
    "SecretString"
]

SUPPORTED_SPAN_KEYS = [
    "trace_id",
    "span_id",
    "traceState",
    "parentSpanId",
    "name",
    "kind",
    "startTimeUnixNano",
    "endTimeUnixNano",
    "attributes",
    "droppedAttributesCount",
    "events",
    "droppedEventsCount",
    "links",
    "droppedLinksCount",
    "status",
]

SUPPORTED_SPAN_EVENT_KEYS = [
    "name",
    "timeUnixNano",
    "attributes",
    "droppedAttributesCount",
]

SUPPORTED_SPAN_LINK_KEYS = [
    "trace_id",
    "span_id",
    "traceState",
    "attributes",
    "droppedAttributesCount",
]


def event_handler(event, _context):
    # Unpack Kinesis records
    zipped_records = [rec["kinesis"]["data"] for rec in event["Records"]]

    # Unzip Kinesis records
    unzipped_records = [
        zlib.decompress(base64.b64decode(rec), 16 + zlib.MAX_WBITS).decode("utf-8")
        for rec in zipped_records
    ]

    undecodable_messages = []
    decoded_messages = []

    # Attempt to JSON decode the CloudWatch logs messages. This should always work, since
    # the payload looks like this:
    # {
    #     "messageType": "DATA_MESSAGE",
    #     "owner": "739178438747",
    #     "logGroup": "/aws/lambda/OpentelemetryPlaygroundSt-ThumbnailFunction2CEC8CF-2jZDagaP2bW9",
    #     "logStream": "2022/06/05/[$LATEST]6a7be0ba512e4126be8327ea0ff4e6bc",
    #     "subscriptionFilters": [
    #         "OpentelemetryPlaygroundStack-SubscriptionFilter27E39E625-QZC018RH7J2M"
    #     ],
    #     "logEvents": [
    #         {
    #             "id": "36895528348509238467599255984368681116550744030445502464",
    #             "timestamp": 1654452710887,
    #             "message": "START RequestId: c9a31149-3076-4b71-a289-36ccea8c81e2 Version: $LATEST\\n"
    #         }
    #     ]
    # }
    for record in unzipped_records:
        try:
            decoded_messages.append(json.loads(record))
        except Exception:
            undecodable_messages.append(record)

    if undecodable_messages:
        print(f"Could not decode {len(undecodable_messages)} from Kinesis")

    otel_tracing_messages = []
    not_tracing_messages = []

    # Attempt to parse every CloudWatch log line and convert it to OTel protobuf format
    for decoded_message in decoded_messages:
        cloudwatch_log_events = decoded_message["logEvents"]
        for log_event in cloudwatch_log_events:
            try:
                message = json.loads(log_event["message"])
            except Exception:
                not_tracing_messages.append(log_event)
                continue

            try:
                otel_tracing_messages.append(_convert_cw_log_to_otel(message))
            except Exception as exc:
                not_tracing_messages.append(log_event)
                print(
                    "Got valid JSON but could not convert it to OTel tracing format. "
                    f"Exception: {type(exc)} - {exc}. Original message on next line."
                )
                print(log_event["message"])

    print(
        f"Found {len(otel_tracing_messages)} valid spans and "
        f"{len(not_tracing_messages)} invalid spans."
    )

    # Convert all found tracing messages to the protobuf container format
    resource_spans = {}
    for resource, span in otel_tracing_messages:
        # Spans are grouped per resource, so build a resource dictionary
        service_name = resource.get("service.name", "unknown")
        if service_name not in resource_spans:
            resource_spans[service_name] = {
                "resource": resource,
                "scopeSpans": [{"spans": []}],
            }
        # Add span to the scopeSpans key for the matching resource
        resource_spans[service_name]["scopeSpans"][0]["spans"].append(span)

    # Convert the protobuf-formatted JSON to actual protobuf and send it off
    # to Honeycomb.
    traces = {"resourceSpans": list(resource_spans.values())}
    message = ParseDict(traces, ExportTraceServiceRequest())
    response = requests.post(
        HONEYCOMB_ENDPOINT,
        data=message.SerializeToString(),
        headers={
            "x-honeycomb-team": HONEYCOMB_KEY,
            "content-type": "application/protobuf",
        },
    )
    print(f"Honeycomb response code: {response.status_code}")
    response.raise_for_status()


def _convert_cw_log_to_otel(log_event: dict):
    span = deepcopy(log_event)

    # Move the "resource" key out of the span
    resource = {"attributes": _parse_attributes(span["resource"])}

    # Move the context to the root level
    span["trace_id"] = span["context"]["trace_id"]
    span["span_id"] = span["context"]["span_id"]

    # Reformat the span kind from "SpanKind.INTERNAL" to "SPAN_KIND_INTERNAL"
    span["kind"] = f'SPAN_KIND_{span["kind"][9:]}'

    # Rename the "parent_id" key to "parentSpanId"
    span["parentSpanId"] = span["parent_id"]

    # Convert start and end time to nanoseconds
    span["startTimeUnixNano"] = _datetime_to_nano(span["start_time"])
    span["endTimeUnixNano"] = _datetime_to_nano(span["end_time"])

    # Reformat the status code from "UNSET" to "STATUS_CODE_UNSET"
    span["status"] = {"code": f'STATUS_CODE_{span["status"]["status_code"]}'}

    # Reformat the attributes
    span["attributes"] = _parse_attributes(span["attributes"])

    # Transform each event in span events
    span_events = []
    for span_event in span["events"]:
        new_span_event = deepcopy(span_event)
        new_span_event["attributes"] = _parse_attributes(new_span_event["attributes"])
        new_span_event["timeUnixNano"] = _datetime_to_nano(span_event["timestamp"])

        # Drop any unknown fields from the event JSON
        span_event_copy_known_fields = {}
        for key in SUPPORTED_SPAN_EVENT_KEYS:
            if key in new_span_event:
                span_event_copy_known_fields[key] = new_span_event[key]

        span_events.append(span_event_copy_known_fields)
    span["events"] = span_events

    # Transform each link in span links
    span_links = []
    for span_link in span["links"]:
        new_span_link = deepcopy(span_link)
        new_span_link["attributes"] = _parse_attributes(new_span_link["attributes"])
        new_span_link["trace_id"] = span_link["context"]["trace_id"]
        new_span_link["span_id"] = span_link["context"]["span_id"]

        # Drop any unknown fields from the link JSON
        span_link_copy_known_fields = {}
        for key in SUPPORTED_SPAN_LINK_KEYS:
            if key in new_span_link:
                span_link_copy_known_fields[key] = new_span_link[key]

        span_links.append(span_link_copy_known_fields)
    span["links"] = span_links

    # Drop any unknown fields from the JSON
    span_copy_known_fields = {}
    for key in SUPPORTED_SPAN_KEYS:
        if key in span:
            span_copy_known_fields[key] = span[key]

    return resource, span_copy_known_fields


def _parse_attributes(input_kv_pair):
    """
    Convert key-value attributes to OTel protobuf format.

    Example input: { "key_1": "my_string", "key_2": 15 }
    Output: [
        {
            "key": "key_1",
            "value": {
                "stringValue": "my_string"
            }
        },
        {
            "key": "key_2",
            "value": {
                "intValue": 15
            }
        },
    ]
    """
    response = []
    for k, v in input_kv_pair.items():
        type_identifier = "stringValue"
        if isinstance(v, bool):
            type_identifier = "boolValue"
        elif isinstance(v, int):
            type_identifier = "intValue"
        elif isinstance(v, float):
            type_identifier = "doubleValue"

        response.append({"key": k, "value": {type_identifier: v}})
    return response


def _datetime_to_nano(datetime_isostr) -> int:
    """Convert an ISO 8601 date to a timestamp with nanosecond accuracy."""
    date = dateutil.parser.isoparse(datetime_isostr)
    return int(date.timestamp() * 1000 * 1000 * 1000)
