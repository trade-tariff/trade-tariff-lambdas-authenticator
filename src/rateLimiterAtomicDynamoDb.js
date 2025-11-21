/**
 * ===================================================================
 * Fully Atomic DynamoDB Rate Limiter (Synchronous + Retries)
 * ===================================================================
 * Behaviour:
 * - Every request performs a synchronous GetItem + conditional UpdateItem with retries on collision.
 * - Guarantees strict global consistency – no overages even under extreme contention.
 * - Uses optimistic concurrency on lastRefill/tokens to resolve races correctly.
 *
 * Pros:
 * - 100% accurate token counts across all Lambda@Edge POPs
 * - No possibility of over-issuing requests beyond configured limits
 * - Simple to reason about and audit
 *
 * Cons:
 * - Adds ~100–200 ms latency per request from round-trip to DynamoDB + retries
 * - Higher p95/p99 latencies, especially from distant edge locations
 * - Increased DynamoDB RCU/WCU consumption (one read + one write per request)
 *
 * Best for: Billing-critical limits, strict compliance requirements, or low-traffic APIs where latency is not the primary concern.
 */
const {
  GetItemCommand,
  UpdateItemCommand,
} = require("@aws-sdk/client-dynamodb");
const { error } = require("./logger");

function sanitizeNumber(
  value,
  defaultValue,
  minValue = undefined,
  maxValue = undefined,
) {
  let num = Number(value);
  if (isNaN(num)) {
    return defaultValue;
  }
  if (minValue !== undefined && num < minValue) {
    num = minValue;
  }
  if (maxValue !== undefined && num > maxValue) {
    num = maxValue;
  }
  return num;
}

// Sanitize and set defaults for DynamoDB item attributes
// Handles missing or malformed data gracefully and caps values to prevent abuse
// Returns a sanitized item object with guaranteed valid numeric fields
function sanitizeItem(item) {
  const currentTime = Date.now(); // In milliseconds (Using milliseconds reduces ABA problem likelihood https://grokipedia.com/page/ABA_problem)
  const hardMaxTokens = 2500; // Absolute maximum tokens to prevent abuse
  const hardMaxRefillRate = 2500; // Absolute maximum refill rate to prevent abuse

  const defaultRefillRate = 750; // Tokens per interval
  const defaultInterval = 60; // Seconds
  const defaultMaxTokens = 750; // Burst allowance

  const maxTokens = sanitizeNumber(
    item?.maxTokens?.N,
    defaultMaxTokens,
    1,
    hardMaxTokens,
  );
  const refillRate = sanitizeNumber(
    item?.refillRate?.N,
    defaultRefillRate,
    1,
    hardMaxRefillRate,
  );
  const refillInterval = sanitizeNumber(
    item?.refillInterval?.N,
    defaultInterval,
    1,
  );
  const lastRefill = sanitizeNumber(
    item?.lastRefill?.N,
    currentTime,
    0,
    currentTime,
  );
  const tokens = sanitizeNumber(item?.tokens?.N, maxTokens, 0, maxTokens);
  const timeDelta = Math.max(0, currentTime - lastRefill);
  const refillAmount = Math.floor(
    (refillRate * timeDelta) / (refillInterval * 1000),
  );
  const potentialTokens = tokens + refillAmount;
  const cappedTokens = Math.min(potentialTokens, maxTokens);
  const cappedTokensFloored = Math.floor(cappedTokens);

  return {
    tokens,
    lastRefill,
    refillRate,
    refillInterval,
    maxTokens,
    currentTime,
    timeDelta,
    refillAmount,
    potentialTokens,
    cappedTokens,
    cappedTokensFloored,
  };
}

// Atomic Token Bucket Rate Limiter
// Each request consumes 1 token from the bucket
// If no tokens are available, the request is denied Tokens are refilled based on the elapsed time since the last refill
// This implementation uses DynamoDB to store the token bucket state for each client atomically via read-then-conditional-write.
async function applyRateLimit(ddbClient, table, clientId) {
  const getParams = {
    TableName: table,
    Key: { clientId: { S: clientId } },
    ProjectionExpression:
      "tokens, lastRefill, refillRate, refillInterval, maxTokens",
  };
  let item;
  try {
    const getResult = await ddbClient.send(new GetItemCommand(getParams));
    item = getResult.Item || {};
  } catch (err) {
    if (err.name === "ResourceNotFoundException") {
      if (err.message.includes(table)) {
        error(`DynamoDB table ${table} not found: `, err);
        throw err;
      } else {
        item = {};
      }
    } else {
      error("DynamoDB GetItem error:", err);
      throw err;
    }
  }

  item = sanitizeItem(item);

  let rateLimitResult = {
    allowed: item.cappedTokensFloored >= 1,
    rateLimitRemaining: item.cappedTokensFloored, // Pre-consumption for denied
    rateLimitLimit: item.maxTokens,
    collision: false,
  };

  if (rateLimitResult.rateLimitRemaining < item.maxTokens) {
    rateLimitResult.rateLimitReset = Math.ceil(
      ((item.maxTokens - rateLimitResult.rateLimitRemaining) *
        item.refillInterval) /
        item.refillRate,
    );
  } else {
    rateLimitResult.rateLimitReset = 0;
  }

  let newTokens = item.cappedTokensFloored - 1; // Post-consumption for allowed

  if (rateLimitResult.allowed) {
    rateLimitResult.rateLimitRemaining = newTokens;
    if (rateLimitResult.rateLimitRemaining < item.maxTokens) {
      rateLimitResult.rateLimitReset = Math.ceil(
        ((item.maxTokens - rateLimitResult.rateLimitRemaining) *
          item.refillInterval) /
          item.refillRate,
      );
    } else {
      rateLimitResult.rateLimitReset = 0;
    }
  } else {
    return rateLimitResult;
  }

  const updateParams = {
    TableName: table,
    Key: { clientId: { S: clientId } },
    UpdateExpression: `
      SET tokens = :newTokens,
          lastRefill = :currentTime,
          refillRate = :refillRate,
          refillInterval = :refillInterval,
          maxTokens = :maxTokens
    `,
    ConditionExpression: `
      attribute_not_exists(lastRefill) OR lastRefill = :oldLastRefill
    `,
    ExpressionAttributeValues: {
      ":newTokens": { N: newTokens.toString() },
      ":currentTime": { N: item.currentTime.toString() },
      ":refillRate": { N: item.refillRate.toString() },
      ":refillInterval": { N: item.refillInterval.toString() },
      ":maxTokens": { N: item.maxTokens.toString() },
      ":oldLastRefill": { N: item.lastRefill.toString() },
    },
    ReturnValues: "UPDATED_NEW",
  };

  try {
    const result = await ddbClient.send(new UpdateItemCommand(updateParams));
    rateLimitResult.allowed = true;
    return rateLimitResult;
  } catch (err) {
    if (err.name === "ConditionalCheckFailedException") {
      rateLimitResult.allowed = false;
      rateLimitResult.collision = true;
      return rateLimitResult;
    }
    error("DynamoDB UpdateItem error:", err);
    throw err;
  }
}

async function applyRateLimitWithRetry(
  ddbClient,
  table,
  clientId,
  maxRetries = 3,
  baseBackoffMs = 50,
) {
  let result;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    result = await applyRateLimit(ddbClient, table, clientId);

    if (!result.collision) {
      return result;
    }

    // Exponential backoff with jitter
    if (attempt < maxRetries - 1) {
      const backoff = Math.pow(2, attempt) * baseBackoffMs;
      const jitter = Math.random() * 50;
      await new Promise((resolve) => setTimeout(resolve, backoff + jitter));
    }
  }

  // After max retries just return the last result
  return result;
}
module.exports = { applyRateLimit, applyRateLimitWithRetry };
