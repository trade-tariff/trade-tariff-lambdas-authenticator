const {
  GetItemCommand,
  UpdateItemCommand,
} = require("@aws-sdk/client-dynamodb");

// Atomic Token Bucket Rate Limiter
// Refill Rate: 500 tokens per minute
// Burst Allowance: 2500 tokens
// Each request consumes 1 token from the bucket
// If no tokens are available, the request is denied
// Tokens are refilled based on the elapsed time since the last refill
// This implementation uses DynamoDB to store the token bucket state for each client atomically via read-then-conditional-write.
async function applyRateLimit(ddbClient, table, clientId) {
  const currentTime = Math.floor(Date.now() / 1000); // Seconds for simplicity; use ms for finer granularity if needed

  const defaultRefillRate = 500; // Tokens per interval
  const defaultInterval = 60; // Seconds
  const defaultMaxTokens = 2500; // Burst allowance

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
        console.error(`DynamoDB table ${table} not found:`, err);
        throw err;
      } else {
        console.debug(
          `Client ${clientId} not found in table; treating as new client`,
        );
        item = {}; // New client
      }
    } else {
      console.error("DynamoDB GetItem error:", err);
      throw err;
    }
  }

  // Extract values, using defaults if new client or missing
  const lastRefill = item?.lastRefill ? Number(item.lastRefill.N) : currentTime;
  const refillRate = item?.refillRate
    ? Number(item.refillRate.N)
    : defaultRefillRate;
  const refillInterval = item?.refillInterval
    ? Number(item.refillInterval.N)
    : defaultInterval;
  const maxTokens = item?.maxTokens
    ? Number(item.maxTokens.N)
    : defaultMaxTokens;
  const currentTokens = item?.tokens ? Number(item.tokens.N) : maxTokens;

  const timeDelta = Math.max(0, currentTime - lastRefill);
  const refillAmount = (refillRate * timeDelta) / refillInterval;

  const potentialTokens = currentTokens + refillAmount;
  const cappedTokens = Math.min(potentialTokens, maxTokens);
  const newTokens = cappedTokens - 1;

  if (newTokens < 0) {
    console.debug(
      `Rate limit check failed: Insufficient tokens after refill (potential: ${potentialTokens})`,
    );
    return { allowed: false, rateLimitRemaining: 0 };
  }

  const updateParams = {
    TableName: table,
    Key: { clientId: { S: clientId } },
    UpdateExpression: `
      SET tokens = :newTokens,
          lastRefill = :currentTime,
          refillRate = if_not_exists(refillRate, :refillRate),
          refillInterval = if_not_exists(refillInterval, :refillInterval),
          maxTokens = if_not_exists(maxTokens, :maxTokens)
    `,
    ConditionExpression: `
      attribute_not_exists(lastRefill) OR lastRefill = :oldLastRefill
    `,
    ExpressionAttributeValues: {
      ":newTokens": { N: newTokens.toString() },
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
    if (!result.Attributes) {
      console.debug(
        "Rate limit update did not return attributes; denying to be safe",
      );
      return { allowed: false, rateLimitRemaining: 0 };
    }
    return { allowed: true, rateLimitRemaining: rateLimitRemaining };
  } catch (err) {
    if (err.name === "ConditionalCheckFailedException") {
      console.debug(
        "Conditional update failed (concurrent modification); denying to be safe",
      );
      return { allowed: false, rateLimitRemaining: 0 };
    }
    console.error("DynamoDB UpdateItem error:", err);
    throw err;
  }
}

module.exports = { applyRateLimit };
