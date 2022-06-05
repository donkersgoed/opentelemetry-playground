# OpenTelemetry Playground

This project compares sending OpenTelemetry (OTel) traces in two variants:

- Directly from the OTel library to Honeycomb.io (OTLPSpanExporter)
- Through CloudWatch Logs -> Kinesis Data Streams -> Lambda -> Honeycomb.io
