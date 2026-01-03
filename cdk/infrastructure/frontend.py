from aws_cdk import (
    RemovalPolicy,
    aws_s3 as s3,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_s3_deployment as s3_deploy,
    aws_cognito as cognito,
    aws_iam as iam,
    CfnOutput,
)
from constructs import Construct
import os

class Frontend(Construct):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. Cognito User Pool
        self.user_pool = cognito.UserPool(self, "UserPool",
            self_sign_up_enabled=True,
            sign_in_aliases=cognito.SignInAliases(email=True),
            auto_verify=cognito.AutoVerifiedAttrs(email=True),
            password_policy=cognito.PasswordPolicy(
                min_length=8,
                require_symbols=False,
            ),
            removal_policy=RemovalPolicy.DESTROY, # For dev
        )

        self.user_pool_client = self.user_pool.add_client("UserPoolClient",
            user_pool_client_name="FrontendClient",
            auth_flows=cognito.AuthFlow(
                user_password=True, 
                user_srp=True
            ),
        )


        # 1a. User Groups
        
        # Admin: Full Access (100)
        cognito.CfnUserPoolGroup(self, "GroupAdmin",
            user_pool_id=self.user_pool.user_pool_id,
            group_name="admin",
            description="Full Access",
            precedence=100
        )
        
        # Operator: Can initiate actions (700)
        cognito.CfnUserPoolGroup(self, "GroupOperator",
            user_pool_id=self.user_pool.user_pool_id,
            group_name="operator",
            description="Can initiate actions",
            precedence=700
        )
        
        # Monitor: Read Only (800)
        cognito.CfnUserPoolGroup(self, "GroupMonitor",
            user_pool_id=self.user_pool.user_pool_id,
            group_name="monitor",
            description="Read Only",
            precedence=800
        )
        
        # Public: Default (900)
        cognito.CfnUserPoolGroup(self, "GroupPublic",
            user_pool_id=self.user_pool.user_pool_id,
            group_name="public",
            description="Default group for all users",
            precedence=900
        )

        # 2. Identity Pool
        self.identity_pool = cognito.CfnIdentityPool(self, "IdentityPool",
            allow_unauthenticated_identities=False,
            cognito_identity_providers=[
                cognito.CfnIdentityPool.CognitoIdentityProviderProperty(
                    client_id=self.user_pool_client.user_pool_client_id,
                    provider_name=self.user_pool.user_pool_provider_name,
                )
            ]
        )
        
        # IAM Roles for Identity Pool
        self.authenticated_role = iam.Role(self, "CognitoDefaultAuthenticatedRole",
            assumed_by=iam.FederatedPrincipal(
                "cognito-identity.amazonaws.com",
                {
                    "StringEquals": {
                        "cognito-identity.amazonaws.com:aud": self.identity_pool.ref,
                    },
                    "ForAnyValue:StringLike": {
                        "cognito-identity.amazonaws.com:amr": "authenticated",
                    },
                },
                "sts:AssumeRoleWithWebIdentity",
            )
        )
        
        # Attach role to Identity Pool
        cognito.CfnIdentityPoolRoleAttachment(self, "IdentityPoolRoleAttachment",
            identity_pool_id=self.identity_pool.ref,
            roles={
                "authenticated": self.authenticated_role.role_arn,
            }
        )

        # 3. S3 Bucket for Frontend Assets
        self.bucket = s3.Bucket(self, "WebsiteBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        # 4. CloudFront Distribution
        self.distribution = cloudfront.Distribution(self, "WebsiteDistribution",
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3Origin(self.bucket),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
            ),
            default_root_object="index.html",
            error_responses=[
                cloudfront.ErrorResponse(
                    http_status=403,
                    response_http_status=200,
                    response_page_path="/index.html",
                ),
                cloudfront.ErrorResponse(
                    http_status=404,
                    response_http_status=200,
                    response_page_path="/index.html",
                ),
            ],
        )

        # 5. Deployment
        # We assume build is run before deploy
        frontend_build_path = os.path.join(os.getcwd(), "../frontend/dist")
        
        # NOTE: If this path does not exist, synth will fail.
        # We can handle this by checking if it exists, or just ensure it exists.
        
        self.deployment = s3_deploy.BucketDeployment(self, "DeployWebsite",
            sources=[s3_deploy.Source.asset(frontend_build_path)],
            destination_bucket=self.bucket,
            distribution=self.distribution,
            distribution_paths=["/*"],
        )
        
        # Outputs
        CfnOutput(self, "UserPoolId", value=self.user_pool.user_pool_id)
        CfnOutput(self, "UserPoolClientId", value=self.user_pool_client.user_pool_client_id)
        CfnOutput(self, "IdentityPoolId", value=self.identity_pool.ref)
        CfnOutput(self, "WebsiteUrl", value=self.distribution.distribution_domain_name)
