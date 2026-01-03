import boto3
import json
import os

def main():
    cfn = boto3.client('cloudformation')
    # Default stack name from CDK context often matches the app. 
    # But usually it's defined in the app.py. 
    # We'll assume "CurioLambdaStack" or try to find it.
    stack_name = "CurioLambdaStack" 
    
    print(f"Fetching outputs for stack: {stack_name}...")
    try:
        response = cfn.describe_stacks(StackName=stack_name)
    except Exception as e:
        print(f"Error fetching stack {stack_name}: {e}")
        print("Please ensure the stack is deployed and the name is correct.")
        return

    outputs = response['Stacks'][0].get('Outputs', [])
    
    config = {}
    found_api = False
    
    for o in outputs:
        key = o['OutputKey']
        val = o['OutputValue']
        # CDK generates unique IDs in output keys, so we check for substring
        if "CurioFrontendUserPoolId" in key:
            config["VITE_USER_POOL_ID"] = val
        elif "CurioFrontendUserPoolClientId" in key:
            config["VITE_USER_POOL_CLIENT_ID"] = val
        elif "CurioFrontendIdentityPoolId" in key:
            config["VITE_IDENTITY_POOL_ID"] = val
        elif "CurioApiGatewayApiUrl" in key:
             config["VITE_API_URL"] = val
             found_api = True
             
    # Region
    config["VITE_REGION"] = boto3.session.Session().region_name or "us-east-1"

    if not config:
        print("No relevant outputs found. Is the stack deployed?")
        return

    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "frontend/.env")
    
    env_content = "\n".join([f"{k}={v}" for k, v in config.items()])
    
    print(f"Writing config to {env_path}...")
    with open(env_path, "w") as f:
        f.write(env_content)
        
    print("Done!")
    print("Next steps:")
    print("1. cd frontend")
    print("2. npm run build")
    print("3. cd ../cdk && cdk deploy (to update the S3 bucket with new assets)")

if __name__ == "__main__":
    main()
