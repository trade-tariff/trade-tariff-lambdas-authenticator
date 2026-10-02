const crypto = require("node:crypto");

const { CognitoJwtVerifier } = require("aws-jwt-verify");

const config = require("./config.json");
const { info, error } = require("./logger");

const SCOPES = config.SCOPES;
const USER_POOL_ID = process.env.USER_POOL_ID;
const MCP_SECRET_TOKEN = process.env.MCP_SECRET_TOKEN;
const MCP_USAGE_KEY = process.env.MCP_USAGE_KEY;

const tokenCache = new Map();
const MAX_CACHE_SIZE = 1000;
const TOKEN_CACHE_TTL_SECONDS = Number(config.TOKEN_CACHE_TTL_SECONDS ?? 150);

const verifier = CognitoJwtVerifier.create({
  userPoolId: USER_POOL_ID,
  tokenUse: "access",
  clientId: null,
});

async function verifyTokenCached(token) {
  const currentTime = Math.floor(Date.now() / 1000);

  if (tokenCache.has(token)) {
    const { payload, expiresAt } = tokenCache.get(token);

    if (
      expiresAt > currentTime &&
      (!payload.exp || payload.exp > currentTime)
    ) {
      return payload;
    }

    tokenCache.delete(token);
  }

  const payload = await verifier.verify(token);

  if (tokenCache.size >= MAX_CACHE_SIZE) {
    const firstKey = tokenCache.keys().next().value;
    tokenCache.delete(firstKey);
  }

  tokenCache.set(token, {
    payload,
    expiresAt: currentTime + TOKEN_CACHE_TTL_SECONDS,
  });

  return payload;
}

function authorised(scopes, path) {
  const scopeList = scopes ? scopes.split(" ") : [];

  for (const scope of scopeList) {
    const scopeConfig = SCOPES[scope];
    if (!scopeConfig) continue;

    let isExcluded = false;

    if (scopeConfig.excludedPaths) {
      for (const excludedPath of scopeConfig.excludedPaths) {
        if (path.includes(excludedPath)) {
          isExcluded = true;
          break;
        }
      }
    }

    if (isExcluded) continue;

    if (scopeConfig.allowedPaths) {
      for (const allowedPath of scopeConfig.allowedPaths) {
        if (path.startsWith(allowedPath)) {
          return true;
        }
      }
    }
  }

  return false;
}

function buildPolicy({ principalId, effect, resource, clientId, mcp }) {
  return {
    principalId,
    policyDocument: {
      Version: "2012-10-17",
      Statement: [
        {
          Action: "execute-api:Invoke",
          Effect: effect,
          Resource: resource,
        },
      ],
    },
    context: {
      client_id: clientId,
    },
    usageIdentifierKey: mcp ? MCP_USAGE_KEY : clientId,
  };
}

function extractAuthorizationHeader(headers = {}) {
  return headers.Authorization || headers.authorization;
}

// MCP traffic shares one 3,000rpm usage plan instead of consuming the end
// user's ~750rpm per-key plan (HMRC-2699). The token only selects the plan:
// the JWT is still verified and scope-checked, and the real client_id is still
// returned as the principal.
function isMcpRequest(headers = {}) {
  if (!MCP_SECRET_TOKEN || !MCP_USAGE_KEY) return false;

  const presented = headers["x-mcp-token"] || headers["X-Mcp-Token"];
  if (!presented) return false;

  const presentedBytes = Buffer.from(presented);
  const expectedBytes = Buffer.from(MCP_SECRET_TOKEN);
  if (presentedBytes.length !== expectedBytes.length) return false;

  return crypto.timingSafeEqual(presentedBytes, expectedBytes);
}

function logDecision({ decision, reason, clientId, path, method, mcp }) {
  info("authorizer decision", {
    decision,
    reason,
    client_id: clientId,
    path,
    method,
    mcp,
  });
}

async function handler(event) {
  const path = event.path || event.requestContext?.path || "/";
  const method = event.httpMethod || event.requestContext?.httpMethod;
  const authHeader = extractAuthorizationHeader(event.headers);

  if (!authHeader) {
    logDecision({
      decision: "unauthorized",
      reason: "missing_authorization_header",
      path,
      method,
    });
    throw new Error("Unauthorized");
  }

  if (!authHeader.startsWith("Bearer ")) {
    logDecision({
      decision: "unauthorized",
      reason: "invalid_authorization_header",
      path,
      method,
    });
    throw new Error("Unauthorized");
  }

  const token = authHeader.split(" ")[1];

  try {
    const payload = await verifyTokenCached(token);
    const clientId = payload.client_id;

    if (!clientId) {
      logDecision({
        decision: "unauthorized",
        reason: "missing_client_id",
        path,
        method,
      });
      throw new Error("Unauthorized");
    }

    const effect = authorised(payload.scope, path) ? "Allow" : "Deny";
    const mcp = isMcpRequest(event.headers);

    logDecision({
      decision: effect.toLowerCase(),
      clientId,
      path,
      method,
      mcp,
    });

    return buildPolicy({
      principalId: clientId,
      effect,
      resource: event.methodArn,
      clientId,
      mcp,
    });
  } catch (err) {
    if (err.message === "Unauthorized") {
      throw err;
    }

    error("Token verification failed:", err);
    logDecision({
      decision: "unauthorized",
      reason: "token_verification_failed",
      path,
      method,
    });
    throw new Error("Unauthorized");
  }
}

module.exports = {
  handler,
};
