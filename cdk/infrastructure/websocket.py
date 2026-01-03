from aws_cdk import (
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as integrations,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_events,
    aws_iam as iam,
    RemovalPolicy,
    CfnOutput,
    Duration,
)
from constructs import Construct

class WebSocketConstruct(Construct):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. DynamoDB Tables
        self.connections_table = dynamodb.Table(
            self, "ConnectionsTable",
            partition_key=dynamodb.Attribute(name="connectionId", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl",
        )

        self.status_table = dynamodb.Table(
            self, "StatusTable",
            partition_key=dynamodb.Attribute(name="taskId", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="timestamp", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            stream=dynamodb.StreamViewType.NEW_IMAGE, # Enable stream for broadcaster
        )
        
        # Add GSI for querying by state (e.g. find running tasks)
        self.status_table.add_global_secondary_index(
            index_name="StateIndex",
            partition_key=dynamodb.Attribute(name="state", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="updatedAt", type=dynamodb.AttributeType.STRING),
            projection_type=dynamodb.ProjectionType.ALL
        )

        # Add GSI for querying by jobType (e.g. find all catalog jobs)
        self.status_table.add_global_secondary_index(
            index_name="JobTypeIndex",
            partition_key=dynamodb.Attribute(name="jobType", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="updatedAt", type=dynamodb.AttributeType.STRING),
            projection_type=dynamodb.ProjectionType.ALL
        )

        # 2. WebSocket API
        self.web_socket_api = apigwv2.WebSocketApi(self, "StatusApi")
        
        apigwv2.WebSocketStage(self, "ProdStage",
            web_socket_api=self.web_socket_api,
            stage_name="prod",
            auto_deploy=True,
        )

        # 3. Lambdas
        # Common environment variables
        lambda_env = {
            "CONNECTIONS_TABLE": self.connections_table.table_name,
            "STATUS_TABLE": self.status_table.table_name,
        }

        # Connect Handler
        connect_handler = lambda_.Function(
            self, "ConnectHandler",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_inline("""
import boto3
import os
import time

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['CONNECTIONS_TABLE'])

def handler(event, context):
    connection_id = event['requestContext']['connectionId']
    # TTL of 2 hours
    ttl = int(time.time()) + (2 * 60 * 60)
    
    try:
        table.put_item(Item={'connectionId': connection_id, 'ttl': ttl})
        return {'statusCode': 200, 'body': 'Connected'}
    except Exception as e:
        print(e)
        return {'statusCode': 500, 'body': 'Failed to connect'}
            """),
            environment=lambda_env,
        )
        self.connections_table.grant_write_data(connect_handler)

        # Disconnect Handler
        disconnect_handler = lambda_.Function(
            self, "DisconnectHandler",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_inline("""
import boto3
import os

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['CONNECTIONS_TABLE'])

def handler(event, context):
    connection_id = event['requestContext']['connectionId']
    try:
        table.delete_item(Key={'connectionId': connection_id})
        return {'statusCode': 200, 'body': 'Disconnected'}
    except Exception as e:
        print(e)
        return {'statusCode': 500, 'body': 'Failed to disconnect'}
            """),
            environment=lambda_env,
        )
        self.connections_table.grant_write_data(disconnect_handler)

        # Integrate Lambdas with WebSocket Routes
        self.web_socket_api.add_route(
            "$connect",
            integration=integrations.WebSocketLambdaIntegration("ConnectIntegration", connect_handler)
        )
        self.web_socket_api.add_route(
            "$disconnect",
            integration=integrations.WebSocketLambdaIntegration("DisconnectIntegration", disconnect_handler)
        )

        # Broadcaster Lambda (triggered by StatusTable stream)
        # Needs execution role to POST to Gateway Management API
        broadcaster_handler = lambda_.Function(
            self, "BroadcasterHandler",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_inline("""
import boto3
import os
import json

print("Loading Broadcaster Module")

dynamodb = None
connections_table = None
apigw = None

def init_resources():
    global dynamodb, connections_table, apigw
    if connections_table is None:
        print("Initializing DynamoDB")
        dynamodb = boto3.resource('dynamodb')
        connections_table = dynamodb.Table(os.environ['CONNECTIONS_TABLE'])
    
    if apigw is None:
        print("Initializing API Gateway")
        api_endpoint = os.environ['API_ENDPOINT']
        if api_endpoint.startswith("wss://"):
            api_endpoint = "https://" + api_endpoint[6:]
        if api_endpoint.startswith("ws://"):
            api_endpoint = "http://" + api_endpoint[5:]
        apigw = boto3.client('apigatewaymanagementapi', endpoint_url=api_endpoint)

def handler(event, context):
    print("Handler Invoked")
    try:
        init_resources()
    except Exception as e:
        print(f"Failed to initialize resources: {e}")
        return

    records = event.get('Records', [])
    if not records:
        return

    # Scan for all connections
    scan_resp = connections_table.scan(ProjectionExpression='connectionId')
    connection_ids = [item['connectionId'] for item in scan_resp.get('Items', [])]

    if not connection_ids:
        print("No active connections.")
        return

    for record in records:
        if record['eventName'] in ['INSERT', 'MODIFY']:
            new_image = record['dynamodb']['NewImage']
            # Unmarshal DynamoDB JSON to Python dict
            msg = {
                'taskId': new_image.get('taskId', {}).get('S'),
                'timestamp': new_image.get('updatedAt', {}).get('S'), # Use updatedAt as the event timestamp
                'state': new_image.get('state', {}).get('S'),
                'jobType': new_image.get('jobType', {}).get('S'),
                'startedAt': new_image.get('startedAt', {}).get('S'),
                'message': new_image.get('message', {}).get('S'),
                'level': new_image.get('level', {}).get('S'),
                'processed': new_image.get('processed', {}).get('N'),
                'total': new_image.get('total', {}).get('N'),
                'details': new_image.get('details', {}).get('S') 
            }
            
            payload = json.dumps(msg).encode('utf-8')
            
            for conn_id in connection_ids:
                try:
                    apigw.post_to_connection(ConnectionId=conn_id, Data=payload)
                except apigw.exceptions.GoneException:
                    print(f"Connection {conn_id} is gone, deleting.")
                    connections_table.delete_item(Key={'connectionId': conn_id})
                except Exception as e:
                    print(f"Failed to post to {conn_id}: {e}")

    return {'statusCode': 200}
            """),
            environment={
                "CONNECTIONS_TABLE": self.connections_table.table_name,
                "API_ENDPOINT": self.web_socket_api.api_endpoint + "/prod", 
            },
        )
        
        # Grant permissions
        self.connections_table.grant_read_write_data(broadcaster_handler)
        self.status_table.grant_stream_read(broadcaster_handler)
        
        # Grant permission to manage connections
        broadcaster_handler.add_to_role_policy(iam.PolicyStatement(
            actions=["execute-api:ManageConnections"],
            resources=[f"arn:aws:execute-api:{broadcaster_handler.env.region}:{broadcaster_handler.env.account}:{self.web_socket_api.api_id}/*"]
        ))

        # Add Stream Event Source
        broadcaster_handler.add_event_source(
            lambda_events.DynamoEventSource(
                self.status_table,
                starting_position=lambda_.StartingPosition.LATEST
            )
        )
        
        # ECS Status Handler
        self.ecs_handler = lambda_.Function(
            self, "ECSStatusHandler",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            code=lambda_.Code.from_inline("""
import boto3
import os
import json
import datetime
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['STATUS_TABLE'])

def handler(event, context):
    print("ECS Status Handler")
    print(json.dumps(event))
    
    detail = event.get('detail', {})
    task_arn = detail.get('taskArn', '')
    if not task_arn:
        return
        
    task_id_suffix = task_arn.split('/')[-1][-12:]
    task_id = f"container-{task_id_suffix}"
    
    last_status = detail.get('lastStatus')
    group = detail.get('group', '')
    stopped_reason = detail.get('stoppedReason')
    
    # Only care about our service tasks (filtering usually done by Rule, but good to be safe)
    if not group: 
        return

    # Store container status history (for logs)
    timestamp = datetime.datetime.utcnow().isoformat() + 'Z'
    msg = f"Container {last_status}"
    if stopped_reason:
        msg += f": {stopped_reason}"

    # Deduplicate: Check 'current' status item
    try:
        resp = table.get_item(Key={'taskId': task_id, 'timestamp': 'STATUS'})
        if 'Item' in resp:
            last_item = resp['Item']
            # If state hasn't changed, ignore.
            # We check message too in case of detail changes (like stoppedReason appearing)
            if last_item.get('state') == last_status and last_item.get('message') == msg:
                print(f"Skipping duplicate status for {task_id}: {last_status}")
                return
    except Exception as e:
        print(f"Failed to check duplicates: {e}")

    # Update/Overwrite CURRENT Item 
    # We use a static timestamp 'STATUS' to ensure we overwrite the same item for this task
    try:
        latest_item = {
            'taskId': task_id,
            'timestamp': 'STATUS',
            'updatedAt': timestamp, # Keep track of when it actually changed
            'state': last_status,
            'jobType': 'CONTAINER', 
            'level': 'INFO',
            'message': msg
        }
        table.put_item(Item=latest_item)
    except Exception as e:
        print(f"Failed to put latest item: {e}")
    
    # Aggregation: Count Running Containers
    # Query StateIndex where state = 'RUNNING' AND jobType = 'CONTAINER'
    try:
        # We need to filter by jobType because StateIndex contains logs too
        resp = table.query(
            IndexName='StateIndex',
            KeyConditionExpression=Key('state').eq('RUNNING'),
            FilterExpression=Attr('jobType').eq('CONTAINER'),
            Select='COUNT'
        )
        count = resp['Count']
        
        # Check if count changed to avoid spamming logs
        should_update = True
        try:
            last_items = table.query(
                KeyConditionExpression=Key('taskId').eq('system-active-containers'),
                Limit=1,
                ScanIndexForward=False
            ).get('Items', [])
            
            if last_items:
                last_count = int(last_items[0].get('processed', -1))
                if last_count == count:
                    should_update = False
                    print(f"Metric unchanged ({count}), skipping.")
        except Exception as e:
            print(f"Failed to check last metric: {e}")

        if should_update:
            # Write Metric
            table.put_item(Item={
                'taskId': 'system-active-containers',
                'timestamp': 'STATUS',
                'updatedAt': timestamp,
                'state': 'RUNNING',
                'jobType': 'METRIC',
                'level': 'INFO',
                'message': f"Active Containers: {count}",
                'processed': count,
                'total': count 
            })
    except Exception as e:
        print(f"Failed to aggregate: {e}")
            """),
            environment=lambda_env,
        )
        self.status_table.grant_read_write_data(self.ecs_handler)
        
        CfnOutput(self, "WebSocketApiUrl", value=self.web_socket_api.api_endpoint)
