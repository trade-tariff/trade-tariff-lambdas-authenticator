/**
 * Hybrid Rate Limiter (V2 - Partially Atomic)
 *
 * - Very fast: Checks tokens in memory and responds immediately.
 * - Updates DynamoDB in the background using a conditional check.
 * - Result: Low latency, better accuracy than V1, and recovers from conflicts. Still allows some over-issuance.
 */
const {
  GetItemCommand,
  UpdateItemCommand,
} = require("@aws-sdk/client-dynamodb");
const { error } = require("./logger");

// In-memory cache: Map<clientId, {tokens: number, lastRefill: number, ...}>
const memoryCache = new Map(); // Local to Lambda invocation; approximate and non-persistent
const cacheTtlMs = 15000; // 15 seconds cache TTL
const hardMaxTokens = 2500; // Absolute maximum tokens to prevent abuse
const hardMaxRefillRate = 2500; // Absolute maximum refill rate to prevent abuse
const defaultRefillRate = 300; // Tokens per interval
const defaultInterval = 60; // Seconds
const defaultMaxTokens = 500; // Burst allowance

function calculateRateLimitReset(
  remaining,
  maxTokens,
  refillInterval,
  refillRate,
) {
  if (remaining < maxTokens) {
    return Math.ceil(((maxTokens - remaining) * refillInterval) / refillRate);
  }
  return 0;
}

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

// Pure function to calculate the state of the token bucket.
function calculateTokenState(item) {
  const currentTime = Date.now();
  // Handle both DynamoDB format { N: "value" } and plain number
  const getMaxTokens = (i) => i?.maxTokens?.N || i?.maxTokens;
  const getRefillRate = (i) => i?.refillRate?.N || i?.refillRate;
  const getRefillInterval = (i) => i?.refillInterval?.N || i?.refillInterval;
  const getLastRefill = (i) => i?.lastRefill?.N || i?.lastRefill;
  const getTokens = (i) => i?.tokens?.N || i?.tokens;

  const maxTokens = sanitizeNumber(
    getMaxTokens(item),
    defaultMaxTokens,
    1,
    hardMaxTokens,
  );
  const refillRate = sanitizeNumber(
    getRefillRate(item),
    defaultRefillRate,
    1,
    hardMaxRefillRate,
  );
  const refillInterval = sanitizeNumber(
    getRefillInterval(item),
    defaultInterval,
    1,
  );
  const lastRefill = sanitizeNumber(getLastRefill(item), currentTime, 0);
  const tokens = sanitizeNumber(getTokens(item), maxTokens, 0, maxTokens);

  const timeDelta = Math.max(0, currentTime - lastRefill);
  const refillAmount = (refillRate * timeDelta) / (refillInterval * 1000);
  const potentialTokens = tokens + refillAmount;
  const cappedTokens = Math.min(potentialTokens, maxTokens);

  return {
    tokens, // Current tokens (can be float)
    lastRefill,
    refillRate,
    refillInterval,
    maxTokens,
    currentTime,
    cappedTokens, // Capped tokens (can be float)
  };
}

async function syncToDynamo(
  ddbClient,
  table,
  clientId,
  initialState,
  isConsumed,
  retries = 1,
) {
  const { refillRate, refillInterval, maxTokens, cappedTokens } =
    calculateTokenState(initialState);

  // If consumed, new token count is one less than the calculated capped tokens.
  // Otherwise, it's just the refilled tokens. Always floor for storage.
  const newTokens = isConsumed
    ? Math.floor(cappedTokens - 1)
    : Math.floor(cappedTokens);

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
    ConditionExpression:
      "attribute_not_exists(lastRefill) OR lastRefill = :expectedLastRefill",
    ExpressionAttributeValues: {
      ":newTokens": { N: newTokens.toString() },
      ":currentTime": { N: initialState.currentTime.toString() },
      ":refillRate": { N: refillRate.toString() },
      ":refillInterval": { N: refillInterval.toString() },
      ":maxTokens": { N: maxTokens.toString() },
      ":expectedLastRefill": { N: initialState.lastRefill.toString() },
    },
    ReturnValues: "NONE",
  };

  try {
    await ddbClient.send(new UpdateItemCommand(updateParams));
  } catch (err) {
    if (err.name === "ConditionalCheckFailedException") {
      if (retries > 0) {
        const getParams = {
          TableName: table,
          Key: { clientId: { S: clientId } },
          ProjectionExpression:
            "tokens, lastRefill, refillRate, refillInterval, maxTokens",
        };
        const getResult = await ddbClient.send(new GetItemCommand(getParams));
        const refreshedItem = getResult.Item || {};
        const freshState = calculateTokenState(refreshedItem);

        const tokensAfterRefill = isConsumed
          ? Math.floor(freshState.cappedTokens - 1)
          : Math.floor(freshState.cappedTokens);

        memoryCache.set(clientId, {
          ...calculateTokenState(refreshedItem),
          tokens: tokensAfterRefill,
          lastAccess: Date.now(),
        });

        // Retry the update with the fresh state and decremented retries.
        return syncToDynamo(
          ddbClient,
          table,
          clientId,
          freshState,
          isConsumed,
          retries - 1,
        );
      } else {
        error(
          "Async DynamoDB UpdateItem failed after retry due to collision.",
          { clientId },
        );
      }
    } else {
      error("Async DynamoDB UpdateItem error in hybrid:", err);
    }
  }
}

async function applyRateLimit(ddbClient, table, clientId) {
  const currentTime = Date.now();
  let cachedItem = memoryCache.get(clientId);

  // Get state from DB if cache is missing or stale.
  if (!cachedItem || currentTime - cachedItem.lastAccess > cacheTtlMs) {
    const getParams = {
      TableName: table,
      Key: { clientId: { S: clientId } },
      ProjectionExpression:
        "tokens, lastRefill, refillRate, refillInterval, maxTokens",
    };
    try {
      const getResult = await ddbClient.send(new GetItemCommand(getParams));
      const stateFromDB = calculateTokenState(getResult.Item || {});

      // Always populate the cache after a successful DB read.
      cachedItem = { ...stateFromDB, lastAccess: currentTime };
      memoryCache.set(clientId, cachedItem);
    } catch (err) {
      error("DynamoDB GetItem error:", err);
      // On read error, deny the request to be safe.
      return {
        allowed: false,
        rateLimitRemaining: 0,
        rateLimitLimit: defaultMaxTokens,
        collision: false,
        rateLimitReset: 0,
      };
    }
  }

  // Calculate the current token count based on the cached state.
  const timeDelta = Math.max(0, currentTime - cachedItem.lastRefill);
  const refillAmount =
    (cachedItem.refillRate * timeDelta) / (cachedItem.refillInterval * 1000);
  const potentialTokens = cachedItem.tokens + refillAmount;
  const cappedTokens = Math.min(potentialTokens, cachedItem.maxTokens);

  // Decide if the request is allowed and formulate the result.
  const tokensFloored = Math.floor(cappedTokens);
  const isAllowed = tokensFloored >= 1;
  const remaining = isAllowed ? tokensFloored - 1 : tokensFloored;

  const rateLimitResult = {
    allowed: isAllowed,
    rateLimitRemaining: remaining,
    rateLimitLimit: cachedItem.maxTokens,
    collision: false,
    rateLimitReset: calculateRateLimitReset(
      remaining,
      cachedItem.maxTokens,
      cachedItem.refillInterval,
      cachedItem.refillRate,
    ),
  };

  // If allowed (or if tokens have refilled), update cache and trigger async DB sync.
  const hasRefilled = cappedTokens > cachedItem.tokens;
  if (isAllowed || hasRefilled) {
    const newTokens = isAllowed ? cappedTokens - 1 : cappedTokens;
    // Update cache with the new consumed value.
    memoryCache.set(clientId, {
      ...cachedItem,
      tokens: newTokens,
      lastRefill: currentTime,
      lastAccess: currentTime,
    });

    // Pass the pre-consumption state to syncToDynamo for the conditional check.
    syncToDynamo(ddbClient, table, clientId, cachedItem, isAllowed).catch(
      (err) => {
        error("Sync to Dynamo failed:", err);
      },
    );
  }

  return rateLimitResult;
}
module.exports = {
  applyRateLimit,
  memoryCache,
  calculateTokenState,
  syncToDynamo,
};
