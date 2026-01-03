# Curio Lambda

A simple Rust Lambda function deployed with AWS CDK (Python).

## Prerequisites

- **Rust**: [Install Rust](https://www.rust-lang.org/tools/install)
- **Cargo Lambda**:
  ```bash
  cargo install cargo-lambda
  ```
  _Note: `cargo-lambda` uses `zig` for cross-compilation. If `cargo lambda build` prompts you, follow the instructions or run `pip install ziglang`._
- **AWS CDK CLI**: [Install CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html)
- **Python 3.11+**

## Structure

This project is a **Rust Workspace**:
- `crates/curio-buildtracker/`: The Lambda function source (binary).
  - Embeds `config.json`.
  - Consumes SQS events and writes to DynamoDB via `curio-db`.
- `crates/curio-db/`: Shared library for DynamoDB operations.
- `cdk/`: Python CDK infrastructure.
  - Deploys SQS, DynamoDB, and Lambda.
- `frontend/`: React admin interface. [See Setup Guide](docs/frontend.md).
- `scripts/`: Helper scripts for local development.

## Build & Deploy

### 1. Build Rust Function

Build the release binary for ARM64 (Lambda standard):

```bash
cargo lambda build --release --arm64
```

### 2. Deploy with CDK

Navigate to the `cdk` directory, set up the virtual environment, and deploy:

```bash
cd cdk
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Synthesize template (optional verification)
cdk synth

# Deploy to AWS
cdk deploy
```

## Testing

### Local DynamoDB Testing
We support local testing using DynamoDB Local.

1. **Install DynamoDB Local**:
   ```bash
   ./scripts/install_dynamodb_local.sh
   ```
2. **Run DynamoDB Local**:
   ```bash
   ./scripts/run_dynamodb_local.sh
   ```
3. **Run Tests**:
   ```bash
   export DYNAMODB_ENDPOINT=http://localhost:8000
   export AWS_REGION=us-east-1
   export AWS_ACCESS_KEY_ID=test
   export AWS_SECRET_ACCESS_KEY=test
   cargo test
   ```

### AWS Testing
After deployment, you can invoke the `ProducerLambda` to send a message to SQS. The Rust Lambda will process it and verify with CloudWatch logs.
