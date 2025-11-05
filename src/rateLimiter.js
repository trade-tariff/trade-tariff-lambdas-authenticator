const {
  GetItemCommand,
  UpdateItemCommand,
} = require("@aws-sdk/client-dynamodb");
const { debug, info, warn, error } = require("./logger");

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

// Atomic Token Bucket Rate Limiter
// Each request consumes 1 token from the bucket
// If no tokens are available, the request is denied
// Tokens are refilled based on the elapsed time since the last refill
// This implementation uses DynamoDB to store the token bucket state for each client atomically via read-then-conditional-write.
async function applyRateLimit(ddbClient, table, clientId) {
  const currentTime = Math.floor(Date.now() / 1000); // Seconds for simplicity; use ms for finer granularity if needed
  const defaultRefillRate = 750; // Tokens per interval
  const defaultInterval = 60; // Seconds
  const defaultMaxTokens = 750; // Burst allowance
  const hardMaxTokens = 2500; // Absolute maximum tokens to prevent abuse
  const hardMaxRefillRate = 2500; // Absolute maximum refill rate to prevent abuse

  const getParams = {
    TableName: table,
    Key: { clientId: { S: clientId } },
    ProjectionExpression:
      "tokens, lastRefill, refillRate, refillInterval, maxTokens",
  };

  let item;
  try {
    const getResult = await ddbClient.send(new GetItemCommand(getParams));
    item = getResult.Item;
  } catch (err) {
    if (err.name === "ResourceNotFoundException") {
      if (err.message.includes(table)) {
        error(`DynamoDB table ${table} not found:`, err);
        throw err;
      } else {
        debug(`Client ${clientId} not found in table; treating as new client`);
        item = {}; // New client
      }
    } else {
      error("DynamoDB GetItem error:", err);
      throw err;
    }
  }

  // Sanitize values and set defaults, mins and maxes
  const lastRefill = sanitizeNumber(item?.lastRefill?.N, currentTime, 0);
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
  const maxTokens = sanitizeNumber(
    item?.maxTokens?.N,
    defaultMaxTokens,
    1,
    hardMaxTokens,
  );
  const currentTokens = sanitizeNumber(
    item?.tokens?.N,
    maxTokens,
    0,
    maxTokens,
  );

  const timeDelta = Math.max(0, currentTime - lastRefill);
  if (timeDelta < 0) {
    warn(`Negative time delta detected for client ${clientId}; clock skew?`);
  }

  const refillAmount = Math.floor((refillRate * timeDelta) / refillInterval);
  const potentialTokens = currentTokens + refillAmount;
  const cappedTokens = Math.min(potentialTokens, maxTokens);
  const newTokens = cappedTokens - 1;

  debug(`Calculated tokens for ${clientId}`, {
    currentTokens,
    refillAmount,
    cappedTokens,
    newTokens,
  });

  if (newTokens < 0) {
    debug(
      `Rate limit check failed: Insufficient tokens after refill (potential: ${potentialTokens})`,
    );
    return { allowed: false, rateLimitRemaining: Math.floor(cappedTokens) };
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
      ":newTokens": { N: Math.floor(newTokens).toString() },
      ":currentTime": { N: currentTime.toString() },
      ":refillRate": { N: refillRate.toString() },
      ":refillInterval": { N: refillInterval.toString() },
      ":maxTokens": { N: maxTokens.toString() },
      ":oldLastRefill": { N: lastRefill.toString() },
    },
    ReturnValues: "UPDATED_NEW",
  };

  const rateLimitRemaining = Math.floor(newTokens);

  try {
    const result = await ddbClient.send(new UpdateItemCommand(updateParams));
    debug(
      `Rate limit update succeeded for client ${clientId}`,
      result.Attributes,
    );
    return { allowed: true, rateLimitRemaining: rateLimitRemaining };
  } catch (err) {
    if (err.name === "ConditionalCheckFailedException") {
      debug(
        "Conditional update failed (concurrent modification); denying to be safe",
      );
      return { allowed: false, rateLimitRemaining: 0 };
    }
    error("DynamoDB UpdateItem error:", err);
    throw err;
  }
}

module.exports = { applyRateLimit };
