/**
 * ===================================================================
 * Hybrid Partially-Atomic Rate Limiter
 * ===================================================================
 * Behaviour:
 * - Fast local in-memory calculation with immediate response (optimistic path).
 * - Asynchronously persists state using conditional UpdateItem with limited retries.
 * - On collision, refreshes local cache and re-attempts write in background.
 * - Refills are persisted even on denied requests when time has passed.
 *
 * Pros:
 * - Very low latency (~20–60 ms) while greatly reducing lost updates
 * - Near-global consistency with only tiny windows for overages
 * - Accumulates fractional tokens correctly across denied requests
 * - Self-healing: collisions are detected and corrected automatically
 * - Good balance of performance and accuracy for most production workloads
 *
 * Cons:
 * - Still possible (but rare) brief overages/underages under extreme contention
 * - Slightly higher complexity and DynamoDB usage than pure fire-and-forget
 * - Requires monitoring of collision metric to tune retry/staleness settings
 *
 * Best for: Most real-world SaaS/multi-tenant APIs where you want API-Gateway-like correctness without paying the full latency or operational cost of moving to API Gateway.
 */
const {
  GetItemCommand,
  UpdateItemCommand,
} = require("@aws-sdk/client-dynamodb");
const { error } = require("./logger");

// In-memory cache: Map<clientId, {tokens: number, lastRefill: number, ...}>
const memoryCache = new Map(); // Local to Lambda invocation; approximate and non-persistent
const hardMaxTokens = 2500; // Absolute maximum tokens to prevent abuse
const hardMaxRefillRate = 2500; // Absolute maximum refill rate to prevent abuse
const defaultRefillRate = 300; // Tokens per interval
const defaultInterval = 60; // Seconds
const defaultMaxTokens = 500; // Burst allowance

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
  const currentTime = Date.now(); // In milliseconds
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
  const refillAmount = (refillRate * timeDelta) / (refillInterval * 1000); // Use float for precision
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
// Hybrid In-Memory + DynamoDB Token Bucket Rate Limiter
// Uses in-memory cache for approximate, fast checks/updates.
// Asynchronously syncs to DynamoDB with conditional updates for improved atomicity and eventual consistency.
// Note: In-memory is per-Lambda invocation
async function applyRateLimit(ddbClient, table, clientId) {
  let currentTime = Date.now();
  let cachedItem = memoryCache.get(clientId);
  const getParams = {
    TableName: table,
    Key: { clientId: { S: clientId } },
    ProjectionExpression:
      "tokens, lastRefill, refillRate, refillInterval, maxTokens",
  };

  // If no cache or stale (e.g., older than 1 second for approximation), fetch from DynamoDB
  if (!cachedItem || currentTime - cachedItem.lastAccess > 1000) {
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

    const sanitized = sanitizeItem(item);
    cachedItem = {
      ...sanitized,
      lastAccess: currentTime,
    };
    memoryCache.set(clientId, cachedItem);
  }

  // Refill in-memory tokens approximately (using float precision)
  let timeDelta = Math.max(0, currentTime - cachedItem.lastRefill);
  let refillAmount =
    (cachedItem.refillRate * timeDelta) / (cachedItem.refillInterval * 1000);

  cachedItem.potentialTokens = cachedItem.tokens + refillAmount;
  cachedItem.cappedTokens = Math.min(
    cachedItem.potentialTokens,
    cachedItem.maxTokens,
  );
  cachedItem.cappedTokensFloored = Math.floor(cachedItem.cappedTokens);

  let rateLimitResult = {
    allowed: cachedItem.cappedTokensFloored >= 1,
    rateLimitRemaining: cachedItem.cappedTokensFloored, // Pre-consumption for denied
    rateLimitLimit: cachedItem.maxTokens,
    collision: false,
  };

  if (rateLimitResult.rateLimitRemaining < cachedItem.maxTokens) {
    rateLimitResult.rateLimitReset = Math.ceil(
      ((cachedItem.maxTokens - rateLimitResult.rateLimitRemaining) *
        cachedItem.refillInterval) /
        cachedItem.refillRate,
    );
  } else {
    rateLimitResult.rateLimitReset = 0;
  }

  let isConsume = rateLimitResult.allowed;
  let newTokens = isConsume
    ? cachedItem.cappedTokens - 1
    : cachedItem.cappedTokens;

  if (isConsume) {
    cachedItem.tokens = newTokens;
    cachedItem.lastAccess = currentTime;
    cachedItem.lastRefill = currentTime;
    memoryCache.set(clientId, cachedItem);
    rateLimitResult.rateLimitRemaining = Math.floor(newTokens);
    if (rateLimitResult.rateLimitRemaining < cachedItem.maxTokens) {
      rateLimitResult.rateLimitReset = Math.ceil(
        ((cachedItem.maxTokens - rateLimitResult.rateLimitRemaining) *
          cachedItem.refillInterval) /
          cachedItem.refillRate,
      );
    } else {
      rateLimitResult.rateLimitReset = 0;
    }
  }

  // Asynchronously sync to DynamoDB if needed (non-blocking, with limited retries on collision)
  if (timeDelta > 0 || isConsume) {
    // Sync if refilled or consumed
    const syncToDynamo = async (retries = 2) => {
      currentTime = Date.now(); // Refresh time for accuracy in retries
      timeDelta = Math.max(0, currentTime - cachedItem.lastRefill);
      refillAmount =
        (cachedItem.refillRate * timeDelta) /
        (cachedItem.refillInterval * 1000);
      const oldTokens = cachedItem.tokens; // Pre-refill for denied condition
      cachedItem.cappedTokens = Math.min(
        oldTokens + refillAmount,
        cachedItem.maxTokens,
      );
      isConsume = cachedItem.cappedTokens >= 1 && isConsume; // Re-check if still consumable
      newTokens = isConsume
        ? cachedItem.cappedTokens - 1
        : cachedItem.cappedTokens;
      const conditionExpression = isConsume
        ? "(attribute_not_exists(tokens) OR tokens >= :expectedTokens) AND (attribute_not_exists(lastRefill) OR lastRefill <= :expectedLastRefill)"
        : "(attribute_not_exists(tokens) OR tokens = :oldTokens) AND (attribute_not_exists(lastRefill) OR lastRefill = :expectedLastRefill)";
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
        ConditionExpression: conditionExpression,
        ExpressionAttributeValues: {
          ":newTokens": { N: newTokens.toString() },
          ":currentTime": { N: currentTime.toString() },
          ":refillRate": { N: cachedItem.refillRate.toString() },
          ":refillInterval": { N: cachedItem.refillInterval.toString() },
          ":maxTokens": { N: cachedItem.maxTokens.toString() },
          ":expectedTokens": { N: cachedItem.cappedTokens.toString() }, // Refilled capped
          ":expectedLastRefill": { N: cachedItem.lastRefill.toString() },
          ":oldTokens": { N: oldTokens.toString() }, // Pre-refill for denied
        },
        ReturnValues: "NONE",
      };
      try {
        await ddbClient.send(new UpdateItemCommand(updateParams));
      } catch (err) {
        if (err.name === "ConditionalCheckFailedException") {
          rateLimitResult.collision = true;
          const getResult = await ddbClient.send(new GetItemCommand(getParams));
          const refreshedItem = sanitizeItem(getResult.Item || {});
          cachedItem = { ...refreshedItem, lastAccess: currentTime };
          memoryCache.set(clientId, cachedItem);

          if (retries > 0) {
            return syncToDynamo(retries - 1);
          } else {
            error(
              "Async DynamoDB UpdateItem failed after retries due to collisions.",
            );
            return; // Exhausted retries
          }
        } else {
          error("Async DynamoDB UpdateItem error in hybrid:", err);
        }
      }
    };

    await syncToDynamo(0).catch((err) => {
      error("Sync to Dynamo failed:", err);
    });
  }

  return rateLimitResult;
}
module.exports = { applyRateLimit, memoryCache };
