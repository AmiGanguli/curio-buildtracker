from aws_cdk import (
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    Duration,
    RemovalPolicy,
)
from constructs import Construct

class RustFunction(Construct):
    """
    A Construct that creates a Rust Lambda function and a DynamoDB Table.
    """
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id)

        # Create DynamoDB Table
        self.table = dynamodb.Table(
            self,
            "Table",
            partition_key=dynamodb.Attribute(
                name="id",
                type=dynamodb.AttributeType.STRING
            ),
            removal_policy=RemovalPolicy.DESTROY, # For dev/test
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        self.function = lambda_.Function(
            self,
            "Function",
            runtime=lambda_.Runtime.PROVIDED_AL2023,
            handler="bootstrap",
            code=lambda_.Code.from_asset("../target/lambda/curio-buildtracker"),
            architecture=lambda_.Architecture.X86_64,
            memory_size=128,
            timeout=Duration.seconds(30),
            environment={
                "RUST_BACKTRACE": "1",
                "TABLE_NAME": self.table.table_name,
            },
            **kwargs,
        )

        # Grant function permissions to the table
        self.table.grant_read_write_data(self.function)
