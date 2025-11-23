/**
 * ===================================================================
 * Optimistic Fire-and-Forget Rate Limiter (Clobbering / Eventual Consistency)
 * ===================================================================
 * Behaviour:
 * - Performs fast in-memory token calculation and returns immediately.
 * - Asynchronously writes the new state to DynamoDB with an unconditional UpdateItem.
 * - Concurrent updates from other POPs can overwrite each other (last-write-wins).
 *
 * Pros:
 * - Extremely low latency – typically < 20 ms added
 * - Minimal DynamoDB cost (writes only when state changes)
 * - High throughput, excellent for bursty traffic
 *
 * Cons:
 * - Possible brief overages/underages during contention (lost updates)
 * - Global state can lag behind reality by seconds
 * - Harder to debug exact token counts in high-concurrency scenarios
 *
 * Best for: High-performance public APIs where occasional minor over-issuance is acceptable and latency is critical.
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

async function applyRateLimit(ddbClient, table, clientId) {
  const currentTime = Date.now();
  let cachedItem = memoryCache.get(clientId);

  // If no cache or stale (e.g., older than 1 second for approximation), fetch from DynamoDB
  if (!cachedItem || currentTime - cachedItem.lastAccess > 1000) {
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
    const sanitized = sanitizeItem(item);
    cachedItem = {
      ...sanitized,
      lastAccess: currentTime,
    };
    memoryCache.set(clientId, cachedItem);
  }

  // Refill in-memory tokens approximately
  const timeDelta = Math.max(0, currentTime - cachedItem.lastRefill);
  const refillAmount = Math.floor(
    (cachedItem.refillRate * timeDelta) / (cachedItem.refillInterval * 1000),
  );
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

  if (!rateLimitResult.allowed) {
    return rateLimitResult; // Deny immediately if no tokens in approx cache
  }

  // Consume token in-memory
  const newTokens = cachedItem.cappedTokensFloored - 1;
  cachedItem.tokens = newTokens;
  cachedItem.lastRefill = currentTime;
  cachedItem.lastAccess = currentTime;
  memoryCache.set(clientId, cachedItem);

  rateLimitResult.rateLimitRemaining = newTokens;
  if (rateLimitResult.rateLimitRemaining < cachedItem.maxTokens) {
    rateLimitResult.rateLimitReset = Math.ceil(
      ((cachedItem.maxTokens - rateLimitResult.rateLimitRemaining) *
        cachedItem.refillInterval) /
        cachedItem.refillRate,
    );
  } else {
    rateLimitResult.rateLimitReset = 0;
  }

  // Asynchronously sync to DynamoDB (eventual consistency, non-blocking)
  // Use unconditional update for approximation; may overwrite concurrent changes but that's ok for approx
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
    ExpressionAttributeValues: {
      ":newTokens": { N: newTokens.toString() },
      ":currentTime": { N: currentTime.toString() },
      ":refillRate": { N: cachedItem.refillRate.toString() },
      ":refillInterval": { N: cachedItem.refillInterval.toString() },
      ":maxTokens": { N: cachedItem.maxTokens.toString() },
    },
    ReturnValues: "NONE",
  };

  ddbClient.send(new UpdateItemCommand(updateParams)).catch((err) => {
    error("Async DynamoDB UpdateItem error in hybrid:", err);
  });

  rateLimitResult.allowed = true;
  return rateLimitResult;
}

module.exports = { applyRateLimit, memoryCache };
