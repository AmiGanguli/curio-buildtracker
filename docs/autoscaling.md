# Autoscaling Architecture

This document describes the autoscaling mechanism used by the **BuildManager** service (Curio Processor) to handle SQS message processing dynamically.

## Overview

The `BuildManager` ECS service uses **Step Scaling** based on a custom CloudWatch metric representing the "Total Backlog" of the queue. This allows the service to:
1.  Scale out aggressively when work piles up.
2.  Scale in precisely as work diminishes.
3.  **Scale to Zero** when the queue is empty to save costs.

## Metric Calculation: Total Backlog

Standard SQS scaling often uses `ApproximateNumberOfMessagesVisible`. However, for accurate scaling (detecting total load vs. capacity), we track both **Visible** (waiting) and **InFlight** (processing) messages.

**Metric Math Expression:**
```
TotalBacklog = m1 + m3
```
Where:
*   `m1`: `ApproximateNumberOfMessagesVisible` (Waiting in queue)
*   `m3`: `ApproximateNumberOfMessagesNotVisible` (Currently being processed/in-flight)

This gives us the total "Work Item Count" for the system.

## Scaling Logic

We use **Step Scaling** rather than Target Tracking because Target Tracking struggles with the "Scale to Zero" boundary condition and often lags in infinite queue scenarios.

### Scale Out Policy (Adding Capacity)

*   **Trigger**: When `TotalBacklog >= 1`.
*   **Threshold**: `1`
*   **Evaluation**: 1 minute period, 1 datapoint.

**Scaling Steps:**
The goal is to match the number of tasks roughly to the magnitude of the backlog.

| Backlog Size (Metric) | Target Capacity Added | Logic                    |
| :-------------------- | :-------------------- | :----------------------- |
| 1 - 9                 | +1 Task               | Handle intermittent load |
| 10 - 19               | +2 Tasks              | Ramp up for small batch  |
| 20 - 49               | +5 Tasks              |                          |
| 50 - 99               | +10 Tasks             |                          |
| 100 - 299             | +50 Tasks             | Aggressive processing    |
| 300+                  | +200 Tasks            | Max throughput           |

### Scale In Policy (removing Capacity)

*   **Trigger**: When `TotalBacklog == 0`.
*   **Threshold**: `0`
*   **Evaluation**: 5 minute period (to prevent flapping).
*   **Treat Missing Data**: `BREACHING`
    *   *Reasoning*: SQS often stops reporting metrics when the queue is empty and idle. If we don't treat "Missing Data" as 0 (Breaching), the alarm will hang in `INSUFFICIENT_DATA` state and never scale the service down to 0.

## Configuration

The autoscaling is configured in `infrastructure/build_manager.py`.

*   **Min Capacity**: 0
*   **Max Capacity**: Configurable (Default: 200)
*   **Cooldown**: Default ECS Service cooldowns apply.

## Why not default Target Tracking?

Default AWS Target Tracking for ECS usually tracks `CPUUtilization`. For queue workers, CPU is often not the bottleneck (waiting on I/O) or is a lagging indicator. Tracking the Queue Length is a leading indicator.

Furthermore, standard SQS tracking policies in CDK do not support **Scaling to Zero** natively without extra custom resources or alarms. Our custom alarm setup bridging the 0-1 gap is required for the serverless-like behavior of "Scale to 0 on idle".
