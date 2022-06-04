#!/usr/bin/env python3

# Third party imports
import aws_cdk as cdk

# Local application/library specific imports
from opentelemetry_playground.opentelemetry_playground_stack import (
    OpentelemetryPlaygroundStack,
)


app = cdk.App()
OpentelemetryPlaygroundStack(
    scope=app,
    construct_id="OpentelemetryPlaygroundStack",
)

app.synth()
