#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
INSTALL_DIR="$DIR/../bin/dynamodb-local"

if [ ! -f "$INSTALL_DIR/DynamoDBLocal.jar" ]; then
    echo "DynamoDB Local not found. Please run scripts/install_dynamodb_local.sh first."
    exit 1
fi

echo "Starting DynamoDB Local on port 8000..."
java -Djava.library.path="$INSTALL_DIR/DynamoDBLocal_lib" -jar "$INSTALL_DIR/DynamoDBLocal.jar" -sharedDb -port 8000
