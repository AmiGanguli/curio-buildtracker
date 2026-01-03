# Operations Guide

## Granting Access to External Inputs

To allow the `CurioProcessor` to catalog files from an external S3 bucket, you must grant read permissions to the ECS Task Role.

### 1. Identify the Task Role
After deploying the stack, look for the CloudFormation Output named `CurioProcessorTaskRoleArn`.
It will look something like: `arn:aws:iam::123456789012:role/CurioStack-BuildManagerTaskDefTaskRole-ABCDEF123456`

### 2. Grant Permissions
You can grant permissions in two ways:

#### Option A: Bucket Policy (Recommended for cross-account or simple setup)
Add the following statement to the **Bucket Policy** of the *external* bucket (e.g., `harvest-bucket`):

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowCurioProcessorRead",
            "Effect": "Allow",
            "Principal": {
                "AWS": "<YOUR_TASK_ROLE_ARN>"
            },
            "Action": [
                "s3:ListBucket",
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::<EXTERNAL_BUCKET_NAME>",
                "arn:aws:s3:::<EXTERNAL_BUCKET_NAME>/*"
            ]
        }
    ]
}
```

#### Option B: IAM Role Policy
Alternatively, you can attach an inline policy directly to the IAM Role (`CurioProcessorTaskRoleArn`) in the AWS IAM Console:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::<EXTERNAL_BUCKET_NAME>",
                "arn:aws:s3:::<EXTERNAL_BUCKET_NAME>/*"
            ]
        }
    ]
}
```

### 3. Verification
Once permissions are granted, the processor should be able to index files from the external bucket as defined in your `curio.yaml`.
