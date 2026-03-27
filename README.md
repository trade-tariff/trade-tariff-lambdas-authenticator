# trade-tariff-lambdas-authenticator

Authenticates and authorizes requests to Tariff APIs for API Gateway.

This Lambda is a regional API Gateway custom authorizer. It verifies Cognito access tokens,
checks scope access against the requested API path, and returns the policy information API Gateway
needs to apply authorization and usage-plan throttling.

## Deployments

You will need to have the Serverless Framework installed and configured with appropriate AWS credentials loaded into your environment.

The Lambda function is deployed using Serverless Framework.

To deploy the function, run the following command in the project root directory:

```bash
DEPLOYMENT_BUCKET=<bucket> STAGE=development serverless deploy
```

Each AWS account should have its own deployment bucket and deploys to a stage corresponding to the environment (for example development, staging, production).
