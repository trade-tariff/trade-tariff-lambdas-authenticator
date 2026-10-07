# Trade Tariff API authenticator

This Node.js Lambda authorises requests to protected Trade Tariff APIs through
API Gateway. It verifies Cognito access tokens, checks token scopes against the
requested API path, and returns an authorisation policy and usage-plan key.
API Gateway applies the policy and throttling; this Lambda does not serve tariff
data.

## Develop and check changes

Use Node.js and Yarn. See [package.json](package.json), [yarn.lock](yarn.lock)
and [CI workflows](.github/workflows/) for dependencies and runtime versions.

```sh
yarn install --frozen-lockfile
yarn test
yarn eslint .
```

Source is in [src/](src/) and Jest tests are in [`__tests__/`](__tests__/).
Use test tokens and mocked integrations. Do not put real access tokens or
credentials in fixtures or issue reports.

## Deployment

[serverless.yml](serverless.yml) defines the Lambda and its configuration.
The [deployment workflows](.github/workflows/) provide stage-specific settings
and AWS roles. Non-main branch pushes can deploy to shared development.

Deployment needs authorised AWS access and a deployment bucket for the target
account. It is not required to run unit tests. Review the target stage and
existing workflow before deploying; do not use production credentials for
local development.

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

## Contribute

Read [CONTRIBUTING.md](CONTRIBUTING.md) for the fork workflow, checks and private
security reporting. Changes to token verification, scopes or policy generation
need security-focused review.

## Licence

The repository uses the [MIT licence](LICENSE). Preserve its copyright notice.
Third-party dependencies retain their own licences.
