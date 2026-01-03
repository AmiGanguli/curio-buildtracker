from aws_cdk import (
    aws_apigateway as apigw,
    aws_cognito as cognito,
    aws_lambda as lambda_,
    CfnOutput,
)
from constructs import Construct

class ApiGateway(Construct):
    def __init__(self, scope: Construct, construct_id: str, 
                 user_pool: cognito.IUserPool, 
                 handler: lambda_.IFunction, 
                 renderer_handler: lambda_.IFunction = None,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.authorizer = apigw.CognitoUserPoolsAuthorizer(self, "Authorizer",
            cognito_user_pools=[user_pool]
        )

        self.api = apigw.RestApi(self, "CurioApi",
            default_cors_preflight_options=apigw.CorsOptions(
                allow_origins=apigw.Cors.ALL_ORIGINS,
                allow_methods=apigw.Cors.ALL_METHODS,
            )
        )

        self.api.root.add_method("GET",
            apigw.LambdaIntegration(handler),
            authorizer=self.authorizer,
            authorization_type=apigw.AuthorizationType.COGNITO,
        )

        # Catalog Endpoint
        catalog = self.api.root.add_resource("catalog")
        catalog.add_method("POST",
            apigw.LambdaIntegration(handler),
            authorizer=self.authorizer,
            authorization_type=apigw.AuthorizationType.COGNITO,
        )

        catalog.add_method("DELETE",
            apigw.LambdaIntegration(handler),
            authorizer=self.authorizer,
            authorization_type=apigw.AuthorizationType.COGNITO,
        )

        catalog.add_method("GET",
            apigw.LambdaIntegration(handler),
            authorizer=self.authorizer,
            authorization_type=apigw.AuthorizationType.COGNITO,
        )

        # Config Endpoint
        config = self.api.root.add_resource("config")
        config.add_method("GET",
            apigw.LambdaIntegration(handler),
            authorizer=self.authorizer,
            authorization_type=apigw.AuthorizationType.COGNITO,
        )

        # Content Endpoint
        content = self.api.root.add_resource("content")
        content.add_method("GET",
            apigw.LambdaIntegration(handler),
            authorizer=self.authorizer,
            authorization_type=apigw.AuthorizationType.COGNITO,
        )

        # Render Endpoint (Rust)
        if renderer_handler:
            render = self.api.root.add_resource("render")
            render.add_method("GET",
                apigw.LambdaIntegration(renderer_handler),
                authorizer=self.authorizer,
                authorization_type=apigw.AuthorizationType.COGNITO,
            )


        
        CfnOutput(self, "ApiUrl", value=self.api.url)
