#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$DIR/.."

# cleanup function
cleanup() {
    echo "Stopping services..."
    if [ -n "$DYNAMODB_PID" ]; then
        kill $DYNAMODB_PID 2>/dev/null || true
    fi
    if [ -n "$S3_PID" ]; then
        kill $S3_PID 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

echo "Starting Local Services..."

# Start DynamoDB Local
if [ -f "$DIR/run_dynamodb_local.sh" ]; then
    bash "$DIR/run_dynamodb_local.sh" > /dev/null 2>&1 &
    DYNAMODB_PID=$!
    echo "DynamoDB Local started (PID: $DYNAMODB_PID)"
else
    echo "Error: run_dynamodb_local.sh not found"
    exit 1
fi

# Start S3 (Moto)
if [ -f "$DIR/run_s3_local.sh" ]; then
    bash "$DIR/run_s3_local.sh" > /dev/null 2>&1 &
    S3_PID=$!
    echo "S3 (Moto) started (PID: $S3_PID)"
else
    echo "Error: run_s3_local.sh not found"
    exit 1
fi

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 5

# Set Environment Variables
export DYNAMODB_ENDPOINT="http://localhost:8000"
export AWS_ENDPOINT_URL="http://localhost:5000"
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"
export AWS_REGION="us-east-1"
export RUST_BACKTRACE=1

# Run Tests
echo "Running Cargo Tests..."
cd "$PROJECT_ROOT"
cargo test

echo "Tests Completed Successfully!"
