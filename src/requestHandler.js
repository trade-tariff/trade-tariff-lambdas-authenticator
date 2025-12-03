const { CognitoJwtVerifier } = require("aws-jwt-verify");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { NodeHttpHandler } = require("@smithy/node-http-handler");
const https = require("https");

const config = require("./config.json");

const {
  applyRateLimit: reducedAtomicityHybridLimitV1,
} = require("./rateLimiterHybridMemoryDynamo");
const {
  applyRateLimit: reducedAtomicityHybridLimitV2,
} = require("./rateLimiterHybridMemoryDynamoV2");
const {
  applyRateLimit: fullyAtomicRateLimit,
} = require("./rateLimiterAtomicDynamoDb");
const { error } = require("./logger");

const rateLimitOptions = {
  "reduced-atomicity-hybrid-v1": reducedAtomicityHybridLimitV1,
  "reduced-atomicity-hybrid-v2": reducedAtomicityHybridLimitV2,
  "fully-atomic-dynamo": fullyAtomicRateLimit,
};

const RATE_LIMITER_CONFIGURABLE_VIA_HEADER =
  config.RATE_LIMITER_CONFIGURABLE_VIA_HEADER;
const DYNAMODB_TABLE = config.DYNAMODB_TABLE;
const USER_POOL_ID = config.USER_POOL_ID;
const SCOPES = config.SCOPES;

const ERRORS = {
  unauthorized: JSON.stringify({
    errors: [
      {
        status: "401",
        title: "Unauthorized",
        detail:
          "Authentication credentials were missing, incorrect or expired. Please sign up to the service to obtain valid credentials at https://hub.trade-tariff.service.gov.uk.",
      },
    ],
  }),
  forbidden: JSON.stringify({
    errors: [
      {
        status: "403",
        title: "Forbidden",
        detail:
          "You do not have permission to access this resource. Request access by signing up to the service at https://hub.trade-tariff.service.gov.uk.",
      },
    ],
  }),
  rateLimitExceeded: JSON.stringify({
    errors: [
      {
        status: "429",
        title: "Too Many Requests",
        detail:
          "You have exceeded your rate limit. Please try your request again later.",
      },
    ],
  }),
};

// NOTE: All of our viewer requests originate from CloudFront in the eu-west-2 region so we create the DynamoDB client in that region.
// This reduces latency and avoids potential issues with regional endpoints.
const agent = new https.Agent({
  keepAlive: true,
  maxSockets: 50,
  keepAliveMsecs: 10000,
});
const ddbClient = new DynamoDBClient({
  region: "eu-west-2",
  requestHandler: new NodeHttpHandler({
    httpsAgent: agent,
    connectionTimeout: 500,
    socketTimeout: 500,
  }),
});

// Hoist the verifier to avoid recreating it on every invocation
const verifier = CognitoJwtVerifier.create({
  userPoolId: USER_POOL_ID,
  tokenUse: "access",
  clientId: null,
});

function authorised(scopes, path) {
  const scopeList = scopes ? scopes.split(" ") : [];

  for (const scope of scopeList) {
    const config = SCOPES[scope];
    if (!config) continue;
    let isExcluded = false;
    if (config.excludedPaths) {
      for (const excludedPath of config.excludedPaths) {
        if (path.includes(excludedPath)) {
          isExcluded = true;
          break;
        }
      }
    }
    if (isExcluded) continue;
    if (config.allowedPaths) {
      for (const allowedPath of config.allowedPaths) {
        if (path.startsWith(allowedPath)) {
          return true;
        }
      }
    }
  }

  return false;
}

async function handler(event, context, callback) {
  const request = event.Records[0].cf.request;
  const headers = request.headers;
  const authHeader = headers["authorization"];

  context.callbackWaitsForEmptyEventLoop = false;

  let applyRateLimit;

  if (RATE_LIMITER_CONFIGURABLE_VIA_HEADER) {
    const rateLimiterHeader = headers["x-rate-limiter"];
    let rateLimiter =
      rateLimiterHeader?.[0]?.value || "reduced-atomicity-hybrid-v2";

    applyRateLimit =
      rateLimitOptions[rateLimiter] ??
      rateLimitOptions["reduced-atomicity-hybrid-v2"];
  } else {
    applyRateLimit = rateLimitOptions["reduced-atomicity-hybrid-v2"];
  }

  // If no Authorization header, forward as unauthenticated
  if (!authHeader || authHeader.length === 0) {
    request.headers["x-client-id"] = [{ key: "X-Client-Id", value: "unknown" }];
    return callback(null, request);
  }

  const authValue = authHeader[0].value;
  if (!authValue.startsWith("Bearer ")) {
    return callback(null, {
      status: "401",
      statusDescription: "Unauthorized",
      body: ERRORS.unauthorized,
    });
  }

  const token = authValue.split(" ")[1];

  try {
    const payload = await verifier.verify(token);
    const clientId = payload.client_id;

    if (!clientId) {
      return callback(null, {
        status: "401",
        statusDescription: "Unauthorized",
        body: ERRORS.unauthorized,
      });
    }

    const scopes = payload.scope;
    const path = request.uri;

    if (!authorised(scopes, path)) {
      return callback(null, {
        status: "403",
        statusDescription: "Forbidden",
        body: ERRORS.forbidden,
      });
    }

    const {
      allowed,
      rateLimitRemaining,
      rateLimitLimit,
      rateLimitReset,
      collision,
    } = await applyRateLimit(ddbClient, DYNAMODB_TABLE, clientId);

    const rateLimitHeaders = {
      "x-ratelimit-limit": [
        { key: "X-RateLimit-Limit", value: rateLimitLimit.toString() },
      ],
      "x-ratelimit-remaining": [
        { key: "X-RateLimit-Remaining", value: rateLimitRemaining.toString() },
      ],
      "x-ratelimit-reset": [
        { key: "X-RateLimit-Reset", value: rateLimitReset.toString() },
      ],
    };

    if (collision) {
      rateLimitHeaders["x-ratelimit-collision"] = [
        { key: "X-RateLimit-Collision", value: "true" },
      ];
    }

    if (!allowed) {
      return callback(null, {
        status: "429",
        statusDescription: "Too Many Requests",
        body: ERRORS.rateLimitExceeded,
        headers: rateLimitHeaders,
      });
    }
    Object.assign(request.headers, rateLimitHeaders);

    request.headers["x-client-id"] = [
      {
        key: "X-Client-Id",
        value: clientId,
      },
    ];

    // Forward the modified request
    return callback(null, request);
  } catch (err) {
    error("Token verification failed:", err);
    // If Authorization present but invalid, reject with 401
    return callback(null, {
      status: "401",
      statusDescription: "Unauthorized",
      body: ERRORS.unauthorized,
    });
  }
}

module.exports.handler = handler;
