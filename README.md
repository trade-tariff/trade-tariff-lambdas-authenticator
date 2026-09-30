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

## MCP usage plan

MCP traffic shares a single 3,000rpm API Gateway usage plan rather than consuming each end user's
per-key plan (HMRC-2699). A request presenting `X-Mcp-Token` matching `MCP_SECRET_TOKEN` is billed to
`MCP_USAGE_KEY`; everything else is billed to the caller's Cognito `client_id` as before.

The token only selects the usage plan. The access token is still verified and scope-checked, and the
real `client_id` is still returned as `principalId` and in the policy context, so per-user
attribution is unaffected.

| Variable | Contents |
|---|---|
| `MCP_SECRET_TOKEN` | Shared secret the MCP server sends in `X-Mcp-Token`. |
| `MCP_USAGE_KEY` | Value of the `mcp-<env>` API Gateway key. |

Terraform (`trade-tariff-platform-aws-terraform`) generates both values and stores them in the
`mcp-shared-credentials` secret in each AWS account. `.github/bin/deploy` reads that secret when it
deploys, so the values always match the API Gateway key and the WAF rule. There are no GitHub secrets
for MCP.

When `mcp_enabled` is `false` for that environment, both values in the secret are empty, and all
traffic uses per-user plans. If the deploy cannot read the secret, or a key is missing from it, the
deploy stops. Apply the terraform repo before you deploy this one.

If either variable is unset the swap is disabled and all traffic uses per-user plans.
