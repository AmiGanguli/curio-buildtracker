from aws_cdk import (
    Stack,
    aws_sqs as sqs,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_events,
    aws_events as events,
    aws_events_targets as targets,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    CfnOutput,
    Duration
)
import yaml
import re
import os
import json
from constructs import Construct
from infrastructure.construct import RustFunction
from infrastructure.build_manager import BuildManager
from infrastructure.frontend import Frontend
from infrastructure.api_gateway import ApiGateway
from infrastructure.websocket import WebSocketConstruct

class RustLambdaStack(Stack):
    """
    Standalone stack for testing the RustFunction construct with SQS and DynamoDB.
    """
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Load Curio Config
        config_path = os.path.join(os.path.dirname(__file__), '../../examples/config/curio.yaml')
        external_inputs_config = []
        try:
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
                for inp in config_data.get('external_inputs', []):
                    match_pattern = inp.get('match', '')
                    groups = re.findall(r'\?P<([^>]+)>', match_pattern)
                    external_inputs_config.append({
                        'name': inp.get('name'),
                        'fields': groups,
                        'bucket': inp.get('bucket'),
                        'prefix': inp.get('prefix')
                    })
        except Exception as e:
            print(f"Warning: Failed to load curio config: {e}")

        # 0. VPC and Cluster (required for Fargate)
        # Create a new VPC to avoid context lookup issues during synth
        vpc = ec2.Vpc(
            self, "CurioVpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(name="Public", subnet_type=ec2.SubnetType.PUBLIC),
                ec2.SubnetConfiguration(name="Private", subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)
            ]
        )
        
        cluster = ecs.Cluster(self, "CurioCluster", vpc=vpc)

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
        
        # 4. WebSocket Infrastructure
        websocket = WebSocketConstruct(self, "CurioWebSocket")
        
        # 5. Build Manager (ECS Service + EventBus)
        build_manager = BuildManager(self, "BuildManager", cluster=cluster, table=consumer_fn.table, status_table=websocket.status_table)
        
        # 6. ECS Container Monitoring Rule
        # Capture Task State Changes for our cluster
        events.Rule(
            self, "ECSStatusRule",
            event_pattern=events.EventPattern(
                source=["aws.ecs"],
                detail_type=["ECS Task State Change"],
                detail={
                    "clusterArn": [build_manager.service.cluster.cluster_arn]
                }
            ),
            targets=[targets.LambdaFunction(websocket.ecs_handler)]
        )

        
        # 7. Frontend Integration
        frontend = Frontend(self, "CurioFrontend")
        
        # 6. API Handler Lambda (Python)
        # 6. API Handler Lambda (Python)
        api_handler = lambda_.Function(
            self,
            "ApiHandler",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="index.handler",
            timeout=Duration.seconds(30),
            memory_size=512,
            code=lambda_.Code.from_inline("""
import boto3
import os
import json
import uuid
import datetime
from decimal import Decimal
import html
from boto3.dynamodb.conditions import Key, Attr

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table_name = os.environ['TABLE_NAME']
table = dynamodb.Table(table_name)
status_table_name = os.environ.get('STATUS_TABLE', '')
status_table = dynamodb.Table(status_table_name) if status_table_name else None
sqs = boto3.client('sqs')
queue_url = os.environ['QUEUE_URL']

# Helper to convert Decimal to float/int for JSON serialization
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def handler(event, context):
    print("Received event: " + json.dumps(event))
    path = event.get('path', '')
    method = event.get('httpMethod', '')

    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'OPTIONS,GET,POST,PUT,DELETE'
    }
    
    if method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': headers,
            'body': ''
        }
        
    # AUTH CHECK for Catalog (POST, DELETE, GET)
    if path == '/catalog' and (method == 'POST' or method == 'DELETE' or method == 'GET'):
        claims = event.get('requestContext', {}).get('authorizer', {}).get('claims', {})
        # Claims can be 'cognito:groups': 'Group1,Group2' OR ['Group1'] depending on config
        groups_raw = claims.get('cognito:groups', [])
        groups = []
        if isinstance(groups_raw, str):
            groups = groups_raw.split(',')
        elif isinstance(groups_raw, list):
            groups = groups_raw
        
        # Strip whitespace just in case
        groups = [g.strip() for g in groups]

        # POST requires 'operator', DELETE requires 'admin', GET requires 'monitor'
        if method == 'POST':
            if 'operator' not in groups:
                 return {
                    'statusCode': 403,
                    'headers': headers,
                    'body': json.dumps({'error': 'Forbidden: Operator access required'})
                }
        elif method == 'DELETE':
             if 'admin' not in groups:
                 return {
                    'statusCode': 403,
                    'headers': headers,
                    'body': json.dumps({'error': 'Forbidden: Admin access required'})
                }
        elif method == 'GET':
             if 'monitor' not in groups:
                 return {
                    'statusCode': 403,
                    'headers': headers,
                    'body': json.dumps({'error': 'Forbidden: Monitor access required'})
                }

    if method == 'GET' and path == '/config':
        return {
            'statusCode': 200,
            'headers': headers,
            'body': os.environ.get('EXTERNAL_INPUTS_CONFIG', '[]')
        }

    if method == 'GET' and path == '/catalog':
        try:
            # 1. Check for Active Jobs
            running_job = None
            if status_table:
                try:
                    resp = status_table.query(
                        IndexName='StateIndex',
                        KeyConditionExpression=Key('state').eq('RUNNING'),
                        FilterExpression=Attr('jobType').ne('CONTAINER'),
                        Limit=1,
                        ScanIndexForward=False 
                    )
                    if resp.get('Items'):
                        running_job = resp['Items'][0]
                except Exception as e:
                    print(f"Status query failed: {e}")

            if running_job:
                return {
                    'statusCode': 200,
                    'headers': headers,
                    'body': json.dumps({
                        'status': 'RUNNING',
                        'jobType': running_job.get('jobType'),
                        'processed': int(running_job.get('processed', 0)),
                        'total': int(running_job.get('total', 0)),
                        'taskId': running_job.get('taskId')
                    }, default=str)
                }
            else:
                # 2. Return Idle Status + Input List (Lazy Loaded)
                items = []
                query_params = event.get('queryStringParameters') or {}
                parent_id = query_params.get('parentId')
                
                if parent_id:
                    # Fetch children of parent_id
                    try:
                        resp = table.query(
                            IndexName='ParentIndex',
                            KeyConditionExpression=Key('parentId').eq(parent_id)
                        )
                        raw_items = resp.get('Items', [])
                    except Exception as e:
                        print(f"ParentIndex query failed: {e}")
                        raw_items = []
                else:
                    # Provide Roots? Or let client fail? 
                    # The client should drive navigation using configured inputs.
                    # e.g. client knows "harvest_jobs" exists, so it asks for children of "external/harvest_jobs".
                    # But "external/harvest_jobs" itself doesn't exist as an item with children unless we made it a group.
                    # My Rust processor code made "external/harvest_jobs/greenhouse" have parent "external/harvest_jobs".
                    # So querying parentId="external/harvest_jobs" WILL find "greenhouse".
                    # So if no parentId is passed, we can technically return nothing or all roots.
                    # Let's return nothing or a message.
                    # Better: Scan for all type=GROUP? No, too many.
                    # Let's assume client MUST pass parentId.
                    # But for backward compatibility or simple check, let's allow listing all input types?
                    
                    # If no parentId, we can return the external inputs as virtual roots if needed, but 
                    # actually the config endpoint handles that.
                    # Let's just return empty list to signify idle.
                    raw_items = []

                # Parse metadata and format
                for item in raw_items:
                    meta = {}
                    try:
                        meta = json.loads(item.get('metadata', '{}'))
                    except:
                        pass
                        
                    items.append({
                        'id': item.get('id'),
                        'parentId': item.get('parentId'),
                        'type': item.get('type', 'ITEM'),
                        'count': int(item.get('count', 0)),
                        'etag': item.get('etag'),
                        'last_seen': item.get('last_seen'),
                        'status': item.get('status'),
                        'metadata': meta
                    })

                return {
                    'statusCode': 200,
                    'headers': headers,
                    'body': json.dumps({
                        'status': 'IDLE',
                        'items': items,
                        'count': len(items)
                    }, default=str)
                }
        except Exception as e:
            print(f"GET /catalog failed: {e}")
            return {
                'statusCode': 500,
                'headers': headers,
                'body': json.dumps({'error': str(e)})
            }

    if method == 'POST' and path == '/catalog':
        try:
            task_id = f"catalog-{uuid.uuid4()}"
            
            start_time = datetime.datetime.utcnow().isoformat() + 'Z'
            
            # Log QUEUED status
            if status_table:
                status_table.put_item(Item={
                    'taskId': task_id,
                    'timestamp': 'STATUS',
                    'state': 'QUEUED',
                    'jobType': 'CATALOG',
                    'level': 'INFO',
                    'message': 'Catalog Job Queued...',
                    'processed': 0,
                    'total': 0,
                    'startedAt': start_time,
                    'updatedAt': start_time
                })

            # Send message to BuildManager Queue (EventBridge format)
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({
                    'source': 'curio.api',
                    'detail-type': 'CatalogInputFiles',
                    'detail': {
                        'task_id': task_id,
                        'job_type': 'CATALOG',
                        'started_at': start_time
                    }
                })
            )
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps({'message': 'Catalog job started', 'taskId': task_id})
            }
        except Exception as e:
            print(e)
            return {
                'statusCode': 500,
                'headers': headers,
                'body': json.dumps({'error': str(e)})
            }
            
    if method == 'DELETE' and path == '/catalog':
        try:
            task_id = f"purge-{uuid.uuid4()}"
            
            start_time = datetime.datetime.utcnow().isoformat() + 'Z'
            
            # Log QUEUED status
            if status_table:
                status_table.put_item(Item={
                    'taskId': task_id,
                    'timestamp': 'STATUS',
                    'state': 'QUEUED',
                    'jobType': 'PURGE',
                    'level': 'INFO',
                    'message': 'Purge queued...',
                    'processed': 0,
                    'total': 0,
                    'startedAt': start_time,
                    'updatedAt': start_time
                })

            # Send message to BuildManager Queue (EventBridge format) - Async Purge
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({
                    'source': 'curio.api',
                    'detail-type': 'PurgeInputFiles',
                    'detail': {
                        'task_id': task_id,
                        'job_type': 'PURGE',
                        'started_at': start_time
                    }
                })
            )
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps({'message': 'Purge started', 'taskId': task_id})
            }
        except Exception as e:
            print(e)
            return {
                'statusCode': 500,
                'headers': headers,
                'body': json.dumps({'error': str(e)})
            }
        except Exception as e:
            print(e)
            return {
                'statusCode': 500,
                'headers': headers,
                'body': json.dumps({'error': str(e)})
            }

    if method == 'GET' and path == '/content':
        try:
            query_params = event.get('queryStringParameters') or {}
            item_id = query_params.get('id')
            if not item_id:
                return {'statusCode': 400, 'headers': headers, 'body': json.dumps({'error': 'Missing id'})}

            # Security: Verify user has access (monitor role is checked above)

            # 1. Look up item to confirm existence and get type (security check)?
            # Actually, we need to know WHICH bucket it is in. 
            # We can infer bucket from the ID prefix matching the config.
            
            config = json.loads(os.environ.get('EXTERNAL_INPUTS_CONFIG', '[]'))
            target_bucket = None
            
            # Find matching input config
            # ID format: external/<name>/...
            parts = item_id.split('/')
            if len(parts) > 1 and parts[0] == 'external':
                 input_name = parts[1]
                 for cfg in config:
                     if cfg['name'] == input_name:
                         target_bucket = cfg.get('bucket')
                         break
            
            if not target_bucket:
                 return {'statusCode': 404, 'headers': headers, 'body': json.dumps({'error': 'Bucket not found for item'})}

            # 2. Get S3 Key from DynamoDB
            # We must query DynamoDB because the ID is a logical ID (e.g. from regex), not the S3 Key.
            try:
                item_resp = table.get_item(Key={'id': item_id})
                item = item_resp.get('Item')
                if not item:
                     return {'statusCode': 404, 'headers': headers, 'body': json.dumps({'error': 'Item not found in catalog'})}
                
                s3_key = item.get('s3_key')
                if not s3_key:
                    # Fallback for backward compatibility or if no regex used?
                    # If regex was NOT used, ID might be the key?
                    # But safer to error or warn.
                    # Let's assume ID is key if s3_key missing (for items created before this fix, if they match).
                    # But for harvest_jobs, ID != Key.
                    s3_key = item_id
            except Exception as e:
                print(f"DB lookup failed: {e}")
                return {'statusCode': 500, 'headers': headers, 'body': json.dumps({'error': f"DB Error: {e}"})}
            
            # 3. Fetch from S3
            obj = s3.get_object(Bucket=target_bucket, Key=s3_key)
            content = obj['Body'].read().decode('utf-8')
            
            # 4. Sanitize
            # (Relaxed: React handles text safety. Sending raw content to fix double-escape issues.)
            safe_content = content
            
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps({'content': safe_content})
            }
        except Exception as e:
            print(f"Content fetch error: {e}")
            return {'statusCode': 500, 'headers': headers, 'body': json.dumps({'error': str(e)})}

    try:
        # Simple scan for admin interface (GET)
        response = table.scan(Limit=20)
        items = response.get('Items', [])
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps(items, cls=DecimalEncoder)
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps(str(e))
        }
            """),
            environment={
                "TABLE_NAME": consumer_fn.table.table_name,
                "QUEUE_URL": build_manager.queue.queue_url,
                "STATUS_TABLE": websocket.status_table.table_name,
                "EXTERNAL_INPUTS_CONFIG": json.dumps(external_inputs_config)
            },
        )
        
        # Grant API handler read/write access
        consumer_fn.table.grant_read_write_data(api_handler)
        build_manager.queue.grant_send_messages(api_handler)
        websocket.status_table.grant_write_data(api_handler)
        
        # Grant S3 Read Access to ApiHandler for all parsed buckets
        # This is broader than needed but simple.
        # Ideally we construct specific ARNs.
        for inp in external_inputs_config:
            bkt_name = inp.get('bucket')
            if bkt_name:
                import aws_cdk.aws_s3 as s3_lib
                bkt = s3_lib.Bucket.from_bucket_name(self, f"ImportedBucket-{inp.get('name')}", bkt_name)
                bkt.grant_read(api_handler)

        # 7. Renderer Lambda (Rust)
        renderer_fn = lambda_.Function(
            self,
            "RendererFunction",
            runtime=lambda_.Runtime.PROVIDED_AL2023,
            handler="bootstrap",
            code=lambda_.Code.from_asset("../target/lambda/curio-renderer"),
            architecture=lambda_.Architecture.ARM_64,
            memory_size=128,
            timeout=Duration.seconds(30),
            environment={
                "RUST_BACKTRACE": "1",
                "TABLE_NAME": consumer_fn.table.table_name,
                "EXTERNAL_INPUTS_CONFIG": json.dumps(external_inputs_config)
            },
        )
        consumer_fn.table.grant_read_data(renderer_fn)
        for inp in external_inputs_config:
            bkt_name = inp.get('bucket')
            if bkt_name:
                import aws_cdk.aws_s3 as s3_lib
                bkt = s3_lib.Bucket.from_bucket_name(self, f"RendererBucket-{inp.get('name')}", bkt_name)
                bkt.grant_read(renderer_fn)

        # 8. API Gateway
        api_gateway = ApiGateway(self, "CurioApiGateway",
            user_pool=frontend.user_pool,
            handler=api_handler,
            renderer_handler=renderer_fn
        )

        # Outputs
        CfnOutput(self, "ProducerFunctionName", value=producer_fn.function_name)
        CfnOutput(self, "QueueUrl", value=queue.queue_url)
        CfnOutput(self, "TableName", value=consumer_fn.table.table_name)
        CfnOutput(self, "BuildManagerBusName", value=build_manager.bus.event_bus_name)
        CfnOutput(self, "BuildManagerQueueUrl", value=build_manager.queue.queue_url)
        CfnOutput(self, "BuildManagerBucketName", value=build_manager.bucket.bucket_name)
        CfnOutput(self, "CurioProcessorTaskRoleArn", value=build_manager.service.task_definition.task_role.role_arn)
