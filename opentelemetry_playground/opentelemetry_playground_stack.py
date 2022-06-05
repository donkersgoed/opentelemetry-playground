"""Module for the main OpentelemetryPlayground Stack."""

# Third party imports
from aws_cdk import (
    Duration,
    Fn,
    RemovalPolicy,
    Stack,
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    aws_kinesis as kds,
    aws_logs as logs,
    aws_logs_destinations as logs_destinations,
    aws_lambda_event_sources as event_sources,
)
from constructs import Construct


class OpentelemetryPlaygroundStack(Stack):
    """The OpentelemetryPlayground Stack."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        """Construct a new OpentelemetryPlaygroundStack."""
        super().__init__(scope, construct_id, **kwargs)

        stream = kds.Stream(scope=self, id="LoggingStream")

        honeycomb_exporter_layer = lambda_.LayerVersion(
            scope=self,
            id="HoneycombFunctionLayer",
            code=lambda_.Code.from_asset(
                "lambda_/layers/honeycomb_exporter/python.zip"
            ),
        )

        honeycomb_exporter_function = lambda_.Function(
            scope=self,
            id="HoneycombFunction",
            code=lambda_.Code.from_asset("lambda_/functions/honeycomb_exporter"),
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="index.event_handler",
            layers=[honeycomb_exporter_layer],
            memory_size=2048,
            timeout=Duration.minutes(3),
            environment={
                "HONEYCOMB_ENDPOINT": "https://api.honeycomb.io/v1/traces",
                "HONEYCOMB_KEY_SECRET": "honeycomb_key",
            },
        )
        honeycomb_exporter_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["secretsmanager:GetSecretValue"],
                resources=[
                    Fn.join(
                        "",
                        [
                            "arn:aws:secretsmanager:",
                            Fn.ref("AWS::Region"),
                            ":",
                            Fn.ref("AWS::AccountId"),
                            f":secret:honeycomb_key-*",
                        ],
                    )
                ],
            )
        )

        honeycomb_exporter_function.add_event_source(
            event_sources.KinesisEventSource(
                stream,
                batch_size=1900,
                starting_position=lambda_.StartingPosition.TRIM_HORIZON,
                max_batching_window=Duration.seconds(60),
            )
        )

        image_bucket = s3.Bucket(
            scope=self,
            id="ImageBucket",
            removal_policy=RemovalPolicy.DESTROY,
            event_bridge_enabled=True,
        )

        lambda_layer = lambda_.LayerVersion(
            scope=self,
            id="ThumbnailFunctionLayer",
            code=lambda_.Code.from_asset(
                "lambda_/layers/thumbnail_function/python.zip"
            ),
        )

        # Direct-to-hc exporter
        direct_to_hc_function = lambda_.Function(
            scope=self,
            id="ThumbnailFunction",
            code=lambda_.Code.from_asset("lambda_/functions/thumbnail_function"),
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="index.event_handler",
            memory_size=512,
            layers=[lambda_layer],
            environment={
                "IMAGE_BUCKET": image_bucket.bucket_name,
                "HONEYCOMB_ENDPOINT": "https://api.honeycomb.io/v1/traces",
                "HONEYCOMB_KEY_SECRET": "honeycomb_key",
            },
        )
        image_bucket.grant_read_write(direct_to_hc_function)
        direct_to_hc_function.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["secretsmanager:GetSecretValue"],
                resources=[
                    Fn.join(
                        "",
                        [
                            "arn:aws:secretsmanager:",
                            Fn.ref("AWS::Region"),
                            ":",
                            Fn.ref("AWS::AccountId"),
                            f":secret:honeycomb_key-*",
                        ],
                    )
                ],
            )
        )

        direct_to_hc_function_lg = logs.LogGroup(
            scope=self,
            id="ThumbnailFunctionLogGroup",
            log_group_name=Fn.sub(
                "/aws/lambda/${Function}",
                {"Function": direct_to_hc_function.function_name},
            ),
            retention=logs.RetentionDays.ONE_MONTH,
        )

        # AWS Logs Subscription Role
        subscription_role = iam.Role(
            scope=self,
            id="SubscriptionRole",
            assumed_by=iam.ServicePrincipal(
                Fn.join(".", ["logs", Fn.ref("AWS::Region"), "amazonaws.com"]),
                conditions={
                    "StringLike": {
                        "aws:SourceArn": Fn.join(
                            ":",
                            [
                                "arn:aws:logs",
                                Fn.ref("AWS::Region"),
                                Fn.ref("AWS::AccountId"),
                                "log-group",
                                "*",
                            ],
                        ),
                    }
                },
            ),
        )

        # Via CloudWatch
        via_cloudwatch_function = lambda_.Function(
            scope=self,
            id="ThumbnailFunction2",
            code=lambda_.Code.from_asset("lambda_/functions/thumbnail_function"),
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="index.event_handler",
            memory_size=512,
            layers=[lambda_layer],
            environment={"IMAGE_BUCKET": image_bucket.bucket_name},
        )
        image_bucket.grant_read_write(via_cloudwatch_function)

        via_cloudwatch_function_lg = logs.LogGroup(
            scope=self,
            id="ThumbnailFunction2LogGroup",
            log_group_name=Fn.sub(
                "/aws/lambda/${Function}",
                {"Function": via_cloudwatch_function.function_name},
            ),
            retention=logs.RetentionDays.ONE_MONTH,
        )

        logs.SubscriptionFilter(
            scope=self,
            id="SubscriptionFilter",
            log_group=direct_to_hc_function_lg,
            destination=logs_destinations.KinesisDestination(
                stream=stream,
                role=subscription_role,
            ),
            filter_pattern=logs.FilterPattern.all_events(),
        )

        logs.SubscriptionFilter(
            scope=self,
            id="SubscriptionFilter2",
            log_group=via_cloudwatch_function_lg,
            destination=logs_destinations.KinesisDestination(
                stream=stream,
                role=subscription_role,
            ),
            filter_pattern=logs.FilterPattern.all_events(),
        )

        # Trigger both functions on S3 upload.
        rule = events.Rule(
            self,
            "PutObjectRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={"object": {"key": [{"prefix": "uploads/"}]}},
            ),
        )
        rule.add_target(
            targets.LambdaFunction(
                direct_to_hc_function,
            )
        )
        rule.add_target(
            targets.LambdaFunction(
                via_cloudwatch_function,
            )
        )
