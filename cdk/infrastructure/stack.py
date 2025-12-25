from aws_cdk import (
    Stack,
    aws_sqs as sqs,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_events,
    CfnOutput,
)
from constructs import Construct
from infrastructure.construct import RustFunction

class RustLambdaStack(Stack):
    """
    Standalone stack for testing the RustFunction construct with SQS and DynamoDB.
    """
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. Create SQS Queue
        queue = sqs.Queue(self, "CurioQueue")

        # 2. Rust Lambda (Consumer)
        consumer_fn = RustFunction(self, "CurioLambda")
        
        # Add SQS Event Source to Rust Lambda
        consumer_fn.function.add_event_source(
            lambda_events.SqsEventSource(queue)
        )

        # 3. Python Producer Lambda (for testing)
        producer_fn = lambda_.Function(
            self,
            "ProducerLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_inline(f"""
import boto3
import os
import json

sqs = boto3.client('sqs')
queue_url = os.environ['QUEUE_URL']

def handler(event, context):
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({{'test': 'Hello from Producer', 'data': event}})
    )
    print(f"Sent message: {{response['MessageId']}}")
    return {{'statusCode': 200, 'body': 'Message Sent'}}
            """),
            environment={
                "QUEUE_URL": queue.queue_url,
            },
        )

        # Grant producer permission to send messages
        queue.grant_send_messages(producer_fn)

        # Outputs
        CfnOutput(self, "ProducerFunctionName", value=producer_fn.function_name)
        CfnOutput(self, "QueueUrl", value=queue.queue_url)
        CfnOutput(self, "TableName", value=consumer_fn.table.table_name)
