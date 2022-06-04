"""Module for the main OpentelemetryPlayground Stack."""

# Third party imports
from aws_cdk import (
    Stack,
)
from constructs import Construct


class OpentelemetryPlaygroundStack(Stack):
    """The OpentelemetryPlayground Stack."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        """Construct a new OpentelemetryPlaygroundStack."""
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here
