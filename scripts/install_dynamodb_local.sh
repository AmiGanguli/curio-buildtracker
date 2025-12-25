#!/bin/bash
set -e

# Directory for DynamoDB Local
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
INSTALL_DIR="$DIR/../bin/dynamodb-local"
mkdir -p "$INSTALL_DIR"

echo "Downloading DynamoDB Local..."
curl -L -o "$INSTALL_DIR/dynamodb_local.tar.gz" https://s3.us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz

echo "Extracting..."
tar -xzf "$INSTALL_DIR/dynamodb_local.tar.gz" -C "$INSTALL_DIR"

echo "Clean up..."
rm "$INSTALL_DIR/dynamodb_local.tar.gz"

echo "DynamoDB Local installed at $INSTALL_DIR"
