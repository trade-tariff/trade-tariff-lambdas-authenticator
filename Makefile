test:
	node tests/invoke.js
inspect:
	node inspect tests/invoke.js

.SILENT:
config-development:
	curl --silent https://cognito-idp.eu-west-2.amazonaws.com/eu-west-2_eYCVlIQL0/.well-known/openid-configuration | jq
.SILENT:
config-staging:
	curl --silent https://cognito-idp.eu-west-2.amazonaws.com/eu-west-2_h8JF71jvX/.well-known/openid-configuration | jq
.SILENT:
config-production:
	curl --silent https://cognito-idp.eu-west-2.amazonaws.com/eu-west-2_dtYQGsKs4/.well-known/openid-configuration | jq

# NOTE: Edge functions do not support environment variables, so we use a script to configure each deployment stage.
configure-development:
	.github/bin/configure development

configure-staging:
	.github/bin/configure staging

configure-production:
	.github/bin/configure production

deploy-development: configure-development
	.github/bin/deploy development

deploy-staging: configure-staging
	.github/bin/deploy staging

deploy-production: configure-production
	.github/bin/deploy production

publish:
	.github/bin/publish-version $STAGE
