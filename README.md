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

## Contribute

Read [CONTRIBUTING.md](CONTRIBUTING.md) for the fork workflow, checks and private
security reporting. Changes to token verification, scopes or policy generation
need security-focused review.

## Licence

The repository uses the [MIT licence](LICENSE). Preserve its copyright notice.
Third-party dependencies retain their own licences.
