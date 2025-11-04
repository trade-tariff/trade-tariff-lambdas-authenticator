const { CognitoJwtVerifier } = require("aws-jwt-verify");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { applyRateLimit } = require("./rateLimiter");
const { jwtDecode } = require("jwt-decode");

const DYNAMODB_TABLE = "client-rate-limits";
const USER_POOL_ID = "eu-west-2_eYCVlIQL0";
const SCOPES = {
  "tariff/read": {
    excludedPaths: ["green_lanes", "user"],
    allowedPaths: ["/uk/api", "/xi/api", "/api"],
  },
};

const ddbClient = new DynamoDBClient({ region: "eu-west-2" });

function authorised(scopes, path) {
  const scopeList = scopes ? scopes.split(" ") : [];

  for (const scope of scopeList) {
    const excludedPaths = SCOPES[scope]?.excludedPaths;
    const allowedPaths = SCOPES[scope]?.allowedPaths;

    if (excludedPaths) {
      for (const excludedPath of excludedPaths) {
        if (path.includes(excludedPath)) {
          return false;
        }
      }
    }

    if (allowedPaths) {
      for (const allowedPath of allowedPaths) {
        if (path.startsWith(allowedPath)) {
          return true;
        }
      }
    }
  }

  return false;
}

module.exports.handler = async (event, _context, callback) => {
  const request = event.Records[0].cf.request;
  const headers = request.headers;
  const authHeader = headers["authorization"];

  // If no Authorization header, forward as unauthenticated
  if (!authHeader || authHeader.length === 0) {
    console.log(
      "No Authorization header found - forwarding as unauthenticated",
    );
    request.headers["x-client-id"] = [{ key: "X-Client-Id", value: "unknown" }];
    return callback(null, request);
  }

  const authValue = authHeader[0].value;
  if (!authValue.startsWith("Bearer ")) {
    console.log("Invalid Authorization header format");
    return callback(null, {
      status: "401",
      statusDescription: "Unauthorized",
      body: "Invalid Authorization header format",
    });
  }
  const token = authValue.split(" ")[1];

  try {
    const decoded = jwtDecode(token);
    console.debug("Decoded token:", decoded);
    const clientId = decoded.client_id;

    const verifier = CognitoJwtVerifier.create({
      userPoolId: USER_POOL_ID,
      tokenUse: "access",
      clientId: clientId,
    });

    const payload = await verifier.verify(token);
    console.debug("Token verified successfully");

    const scopes = payload.scope;
    const path = request.uri;

    if (!authorised(scopes, path)) {
      console.log(`Forbidden: Insufficient scopes for path ${path}`);
      return callback(null, {
        status: "403",
        statusDescription: "Forbidden",
        body: "Insufficient scopes for this path",
      });
    }

    const { allowed, rateLimitRemaining } = await applyRateLimit(
      ddbClient,
      DYNAMODB_TABLE,
      clientId,
    );

    if (!allowed) {
      console.debug(`Rate limit exceeded for clientId ${clientId}`);
      return callback(null, {
        status: "429",
        statusDescription: "Too Many Requests",
        body: "Rate limit exceeded",
        headers: {
          "x-rate-limit-remaining": [
            {
              key: "X-Rate-Limit-Remaining",
              value: "0",
            },
          ],
        },
      });
    }

    request.headers["x-rate-limit-remaining"] = [
      {
        key: "X-Rate-Limit-Remaining",
        value: rateLimitRemaining.toString(),
      },
    ];

    request.headers["x-client-id"] = [
      {
        key: "X-Client-Id",
        value: clientId,
      },
    ];

    console.debug(`Request authorized for clientId ${clientId}`);
    console.debug("Request URI:", request);
    // Forward the modified request
    return callback(null, request);
  } catch (err) {
    console.error("Token verification failed:", err);
    // If Authorization present but invalid, reject with 401
    return callback(null, {
      status: "401",
      statusDescription: "Unauthorized",
      body: "Invalid or expired token",
    });
  }
};
