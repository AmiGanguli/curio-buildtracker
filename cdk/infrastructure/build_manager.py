from aws_cdk import (
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
    aws_applicationautoscaling as appscaling,
    Duration,
    RemovalPolicy,
)
from constructs import Construct

class BuildManager(Construct):
    def __init__(self, scope: Construct, construct_id: str, cluster: ecs.Cluster, cpu: int = 256, memory_limit_mib: int = 512, max_capacity: int = 200, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. Event Bus
        self.bus = events.EventBus(self, "BuildManagerBus", event_bus_name="curio.buildmanager")

        # 2. SQS Queue
        self.queue = sqs.Queue(self, "BuildQueue", visibility_timeout=Duration.seconds(60))

        # 3. ECS Service
        task_def = ecs.FargateTaskDefinition(
            self, 
            "TaskDef",
            cpu=cpu,
            memory_limit_mib=memory_limit_mib,
        )

        container = task_def.add_container(
            "ProcessorContainer",
            image=ecs.ContainerImage.from_asset("../crates/curio-processor"),
            logging=ecs.LogDriver.aws_logs(stream_prefix="CurioProcessor"),
            environment={
                "QUEUE_URL": self.queue.queue_url,
                "CONCURRENCY": str( max(1, cpu // 1024 * 4) ), # Rough default, can be tuned
                "RUST_LOG": "info",
            },
            stop_timeout=Duration.seconds(60),
        )

        self.service = ecs.FargateService(
            self,
            "Service",
            cluster=cluster,
            task_definition=task_def,
            desired_count=0, # Start at 0, let autoscaling handle it
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            assign_public_ip=True,
        )

        # Grant permissions
        self.queue.grant_consume_messages(task_def.task_role)

        # 4. Rules
        
        # Rule 1: Selected messages to SQS
        self.bus.archive("Archive",
            event_pattern=events.EventPattern(
                account=[scope.account]
            ),
            retention=Duration.days(30)
        )

        target_queue = targets.SqsQueue(self.queue)
        
        events.Rule(
            self,
            "ProcessEventsRule",
            event_bus=self.bus,
            event_pattern=events.EventPattern(
                source=["curio.buildmanager"],
                detail_type=["NoOp", "ArtifactAdded", "ArtifactRemoved"],
            ),
            targets=[target_queue]
        )

        # Rule 2: All messages to CloudWatch Logs
        log_group = logs.LogGroup(
            self, 
            "EventLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY
        )

        events.Rule(
            self,
            "LogAllEventsRule",
            event_bus=self.bus,
            event_pattern=events.EventPattern(
                source=["curio.buildmanager"],
            ),
            targets=[targets.CloudWatchLogGroup(log_group)]
        )

        # 5. Autoscaling
        scaling = self.service.auto_scale_task_count(
            min_capacity=0,
            max_capacity=max_capacity
        )

        # Scale to 0 if queue is empty
        # We need to use CloudWatch alarms for 0 <-> 1 scaling because TargetTracking doesn't support scaling to 0 directly in a clean way for backlog.
        # Actually, standard pattern for SQS backlog scaling:
        # Scale out: Alarm on ApproximateNumberOfMessagesVisible > 0
        # Scale in: Alarm on ApproximateNumberOfMessagesVisible == 0 (with data missing treated as not breaching)

        # Simulated Target Tracking using Step Scaling
        # Metric: (Visible + InFlight) / (Running + 0.00001)
        # Condition:
        #  - Scale In if Metric < 1 (More tasks than total Work)
        #  - Scale Out if Metric > 10 (Targeting ~10 Work/Task)
        # Custom Autoscaling Logic
        # Metric: Total Backlog = Visible + InFlight
        total_backlog_metric = cloudwatch.MathExpression(
            expression="m1 + m3",
            using_metrics={
                "m1": self.queue.metric_approximate_number_of_messages_visible(statistic="Average", period=Duration.minutes(1)),
                "m3": self.queue.metric_approximate_number_of_messages_not_visible(statistic="Average", period=Duration.minutes(1))
            },
            period=Duration.minutes(1),
            label="TotalBacklog"
        )

        # Alarm 1: Scale Up & Maintain (Backlog >= 1)
        # Use scale_on_metric (returns None sometimes? So we ignore return)
        scaling.scale_on_metric(
            "ScaleUpPolicy",
            metric=total_backlog_metric,
            scaling_steps=[
                appscaling.ScalingInterval(change=1, lower=1, upper=9),    # 1 <= x < 10 -> Target 1
                appscaling.ScalingInterval(change=2, lower=9, upper=19),   # 10 <= x < 20 -> Target 2
                appscaling.ScalingInterval(change=5, lower=19, upper=49),  # 20 <= x < 50 -> Target 5
                appscaling.ScalingInterval(change=10, lower=49, upper=99), # 50 <= x < 100 -> Target 10
                appscaling.ScalingInterval(change=50, lower=99, upper=299),# 100 <= x < 300 -> Target 50
                appscaling.ScalingInterval(change=200, lower=299),         # 300 <= x -> Target 200
            ],
            adjustment_type=appscaling.AdjustmentType.EXACT_CAPACITY,
        )
        
        # Locate the policy child to access the alarm
        scale_up_policy = scaling.node.try_find_child("ScaleUpPolicy")
        if scale_up_policy and hasattr(scale_up_policy, 'upper_alarm') and scale_up_policy.upper_alarm:
             scale_up_policy.upper_alarm.threshold = 1
             scale_up_policy.upper_alarm.datapoints_to_alarm = 1
             scale_up_policy.upper_alarm.evaluation_periods = 1
             scale_up_policy.upper_alarm.treat_missing_data = cloudwatch.TreatMissingData.NOT_BREACHING


        # Alarm 2: Scale In to Zero (Backlog == 0)
        # Use scale_on_metric
        scaling.scale_on_metric(
            "ScaleZeroPolicy",
            metric=total_backlog_metric,
            scaling_steps=[
                 appscaling.ScalingInterval(change=0, upper=-0.001), 
                 appscaling.ScalingInterval(change=0, lower=-0.001, upper=0)
            ],
            adjustment_type=appscaling.AdjustmentType.EXACT_CAPACITY,
        )

        scale_down_policy = scaling.node.try_find_child("ScaleZeroPolicy")
        if scale_down_policy and hasattr(scale_down_policy, 'lower_alarm') and scale_down_policy.lower_alarm:
            scale_down_policy.lower_alarm.threshold = 0
            scale_down_policy.lower_alarm.datapoints_to_alarm = 1
            scale_down_policy.lower_alarm.evaluation_periods = 1
            scale_down_policy.lower_alarm.treat_missing_data = cloudwatch.TreatMissingData.BREACHING
