#!/bin/bash
# Runs Moto Server (S3) for local testing
# Usage: ./scripts/run_s3_local.sh

mkdir -p .moto_data
echo "Starting Moto Server on http://localhost:5000..."
./cdk/.venv/bin/moto_server -p 5000
