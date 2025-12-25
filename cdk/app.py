#!/usr/bin/env python3
import os
import aws_cdk as cdk
from infrastructure.stack import RustLambdaStack

app = cdk.App()

RustLambdaStack(app, "CurioLambdaStack")

app.synth()
