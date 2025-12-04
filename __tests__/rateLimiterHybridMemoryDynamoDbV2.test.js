const {
  applyRateLimit,
  memoryCache,
  calculateTokenState,
} = require("../src/rateLimiterHybridMemoryDynamoV2");
const syncToDynamo = jest.requireActual(
  "../src/rateLimiterHybridMemoryDynamoV2",
).syncToDynamo;
const { error } = require("../src/logger");

jest.mock("../src/logger", () => ({
  error: jest.fn(),
}));

const {
  GetItemCommand,
  UpdateItemCommand,
} = require("@aws-sdk/client-dynamodb");
// Mock DynamoDB client
const mockSend = jest.fn();
const mockDdbClient = { send: mockSend };

describe("calculateTokenState", () => {
  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2025-11-23T12:00:00.000Z"));
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("should return default values for a new client (undefined item)", () => {
    const state = calculateTokenState(undefined);
    expect(state).toMatchObject({
      tokens: 500,
      lastRefill: Date.now(),
      refillRate: 300,
      refillInterval: 60,
      maxTokens: 500,
      cappedTokens: 500,
    });
  });

  it("should return default values for an empty item", () => {
    const state = calculateTokenState({});
    expect(state).toMatchObject({
      tokens: 500,
      lastRefill: Date.now(),
      refillRate: 300,
      refillInterval: 60,
      maxTokens: 500,
      cappedTokens: 500,
    });
  });

  it("should calculate refilled tokens correctly", () => {
    jest.setSystemTime(new Date("2025-11-23T12:01:00.000Z")); // 60 seconds later
    const item = {
      tokens: { N: "400" },
      lastRefill: {
        N: new Date("2025-11-23T12:00:00.000Z").getTime().toString(),
      },
      refillRate: { N: "10" }, // 10 tokens per 60 seconds
      refillInterval: { N: "60" },
      maxTokens: { N: "500" },
    };
    const state = calculateTokenState(item);
    // 400 (initial) + (10 tokens/60s * 60s) = 400 + 10 = 410
    expect(state.cappedTokens).toBe(410);
  });

  it("should cap refilled tokens at maxTokens", () => {
    jest.setSystemTime(new Date("2025-11-23T12:01:00.000Z")); // 60 seconds later
    const item = {
      tokens: { N: "495" },
      lastRefill: {
        N: new Date("2025-11-23T12:00:00.000Z").getTime().toString(),
      },
      refillRate: { N: "10" }, // 10 tokens per 60 seconds
      refillInterval: { N: "60" },
      maxTokens: { N: "500" },
    };
    const state = calculateTokenState(item);
    // 495 (initial) + 10 (refill) = 505, capped at 500
    expect(state.cappedTokens).toBe(500);
  });

  it("should handle malformed numbers gracefully", () => {
    const item = {
      tokens: { N: "abc" },
      lastRefill: { N: "def" },
      refillRate: { N: "ghi" },
      refillInterval: { N: "jkl" },
      maxTokens: { N: "mno" },
    };
    const state = calculateTokenState(item);
    expect(state).toMatchObject({
      tokens: 500, // default
      lastRefill: Date.now(), // default
      refillRate: 300, // default
      refillInterval: 60, // default
      maxTokens: 500, // default
      cappedTokens: 500, // default
    });
  });
});

describe("applyRateLimit", () => {
  const clientId = "test-client";
  const tableName = "client-rate-limits";

  beforeEach(() => {
    jest.resetAllMocks();
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2025-11-23T12:00:00.000Z"));
    memoryCache.clear();
  });

  afterEach(() => {
    jest.restoreAllMocks(); // Restore syncToDynamo mock
    jest.useRealTimers();
  });

  it("should allow a request for a new client and trigger async update", async () => {
    mockSend.mockResolvedValueOnce({ Item: undefined }); // GetItem for new client

    const result = await applyRateLimit(mockDdbClient, tableName, clientId);

    expect(result).toMatchObject({
      allowed: true,
      rateLimitRemaining: 499,
      rateLimitLimit: 500,
      rateLimitReset: 1,
    });
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(mockSend).toHaveBeenCalledWith(expect.any(GetItemCommand));
    expect(mockSend).toHaveBeenCalledWith(expect.any(UpdateItemCommand));

    const cached = memoryCache.get(clientId);
    expect(cached).toMatchObject({
      tokens: 499,
      lastRefill: Date.now(),
      maxTokens: 500,
    });
  });

  it("should deny a request if tokens are zero", async () => {
    const initialTime = new Date("2025-11-23T12:00:00.000Z").getTime();
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "0" },
        lastRefill: { N: initialTime.toString() },
        refillRate: { N: "10" },
        refillInterval: { N: "60" },
        maxTokens: { N: "500" },
      },
    }); // GetItem returns 0 tokens

    const result = await applyRateLimit(mockDdbClient, tableName, clientId);

    expect(result).toMatchObject({
      allowed: false,
      rateLimitRemaining: 0,
      rateLimitLimit: 500,
      rateLimitReset: 3000, // (500 - 0) * 60 / 10 = 3000 seconds
    });
    expect(mockSend).toHaveBeenCalledTimes(1); // Only GetItem
    expect(memoryCache.get(clientId)).toBeDefined(); // Cache should still be populated
  });

  it("should use in-memory cache for subsequent calls within staleness window", async () => {
    const initialTime = new Date("2025-11-23T12:00:00.000Z").getTime();
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "10" },
        lastRefill: { N: initialTime.toString() },
        refillRate: { N: "10" },
        refillInterval: { N: "60" },
        maxTokens: { N: "500" },
      },
    }); // First GetItem

    await applyRateLimit(mockDdbClient, tableName, clientId); // First call, populates cache
    jest.advanceTimersByTime(500); // Advance time, but within 1000ms staleness
    await applyRateLimit(mockDdbClient, tableName, clientId); // Second call, should use cache

    expect(mockSend).toHaveBeenCalledTimes(2); // First GetItem and set on the cache and Update to dynamodb. Second call uses cache only
    const cached = memoryCache.get(clientId);
    expect(Math.floor(cached.tokens)).toBe(8); // 10 - 1 (first call) - 1 (second call)
  });

  it("should re-fetch from DynamoDB if cache is stale", async () => {
    const initialTime = new Date("2025-11-23T12:00:00.000Z").getTime();
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "10" },
        lastRefill: { N: initialTime.toString() },
        refillRate: { N: "10" },
        refillInterval: { N: "60" },
        maxTokens: { N: "500" },
      },
    }); // First GetItem

    await applyRateLimit(mockDdbClient, tableName, clientId); // First call
    jest.advanceTimersByTime(150000); // Advance time beyond 150000ms staleness
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "9" },
        lastRefill: { N: (initialTime + 1500).toString() },
        refillRate: { N: "10" },
        refillInterval: { N: "60" },
        maxTokens: { N: "500" },
      },
    }); // Second GetItem (re-fetch)

    await applyRateLimit(mockDdbClient, tableName, clientId); // Second call, should re-fetch

    expect(mockSend).toHaveBeenCalledTimes(4);
    expect(mockSend.mock.calls[0][0]).toBeInstanceOf(GetItemCommand); // First applyRateLimit GetItem
    expect(mockSend.mock.calls[1][0]).toBeInstanceOf(UpdateItemCommand); // First applyRateLimit UpdateItem (async and always)
    expect(mockSend.mock.calls[2][0]).toBeInstanceOf(GetItemCommand); // Second applyRateLimit GetItem due to 1.5 second staleness
    expect(mockSend.mock.calls[3][0]).toBeInstanceOf(UpdateItemCommand); // Second applyRateLimit UpdateItem (async and always)
  });

  it("should deny request if initial GetItem fails", async () => {
    const result = await applyRateLimit(mockDdbClient, tableName, clientId);

    expect(result).toMatchObject({
      allowed: false,
      rateLimitRemaining: 0,
      rateLimitLimit: 500,
      rateLimitReset: 0,
    });
    expect(error).toHaveBeenCalledWith(
      "DynamoDB GetItem error:",
      expect.any(Error),
    );
    expect(memoryCache.get(clientId)).toBeUndefined(); // Cache should not be populated
  });

  it("should handle parallel requests without race conditions on the cache", async () => {
    mockSend.mockResolvedValueOnce({
      Item: { tokens: { N: "5" }, lastRefill: { N: Date.now().toString() } },
    });
    await applyRateLimit(mockDdbClient, tableName, clientId);
    expect(memoryCache.get(clientId).tokens).toBe(4); // Primed and consumed 1.

    // Act: Fire off 10 more requests in parallel.
    const promises = [];
    for (let i = 0; i < 10; i++) {
      promises.push(applyRateLimit(mockDdbClient, tableName, clientId));
    }
    const results = await Promise.all(promises);

    const allowedCount = results.filter((r) => r.allowed).length;
    const deniedCount = results.filter((r) => !r.allowed).length;

    expect(allowedCount).toBe(4);
    expect(deniedCount).toBe(6);

    const cached = memoryCache.get(clientId);
    expect(Math.floor(cached.tokens)).toBe(0);
  });

  it("should correctly deplete tokens from in-memory cache during a rapid burst", async () => {
    const initialTime = new Date("2025-11-23T12:00:00.000Z").getTime();
    // Start with a small number of tokens to test depletion quickly.
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "10" },
        lastRefill: { N: initialTime.toString() },
        refillRate: { N: "10" },
        refillInterval: { N: "60" },
        maxTokens: { N: "10" },
      },
    });

    // First call to populate the cache.
    let result = await applyRateLimit(mockDdbClient, tableName, clientId);
    expect(result.allowed).toBe(true);
    expect(result.rateLimitRemaining).toBe(9);
    expect(mockSend).toHaveBeenCalledTimes(2); // Initial GetItem + async UpdateItem

    // Subsequent calls in a rapid burst (9 more times).
    for (let i = 0; i < 9; i++) {
      // Advance time by a small amount, simulating requests in the same second.
      jest.advanceTimersByTime(50);
      result = await applyRateLimit(mockDdbClient, tableName, clientId);
      expect(result.allowed).toBe(true);
      // Each call should decrement the remaining tokens.
      expect(result.rateLimitRemaining).toBe(8 - i);
    }

    // All 10 tokens should now be consumed.
    const cached = memoryCache.get(clientId);
    expect(Math.floor(cached.tokens)).toBe(0);

    // The next call should be denied.
    // A small refill will have occurred, but not enough for a full token.
    jest.advanceTimersByTime(50);
    result = await applyRateLimit(mockDdbClient, tableName, clientId);
    expect(result.allowed).toBe(false);
    expect(result.rateLimitRemaining).toBe(0);

    // The total calls should be the initial Get/Update + 9 Updates that are purely in memory
    expect(mockSend).toHaveBeenCalledTimes(2);
  });
});

describe("syncToDynamo", () => {
  const clientId = "test-client";
  const tableName = "client-rate-limits";
  const initialTime = new Date("2025-11-23T12:00:00.000Z").getTime();

  beforeEach(() => {
    jest.resetAllMocks();
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2025-11-23T12:00:00.000Z"));
    memoryCache.clear();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("should send UpdateItemCommand with correct parameters when consumed", async () => {
    const initialState = {
      tokens: 10,
      lastRefill: initialTime,
      refillRate: 10,
      refillInterval: 60,
      maxTokens: 500,
      currentTime: initialTime,
      cappedTokens: 10,
    };

    await syncToDynamo(mockDdbClient, tableName, clientId, initialState, true);

    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(mockSend).toHaveBeenCalledWith(expect.any(UpdateItemCommand));
    const updateParams = mockSend.mock.calls[0][0].input;
    expect(updateParams.Key.clientId.S).toBe(clientId);
    expect(updateParams.UpdateExpression).toContain("SET tokens = :newTokens");
    expect(updateParams.ConditionExpression).toBe(
      "attribute_not_exists(lastRefill) OR lastRefill = :expectedLastRefill",
    );
    expect(updateParams.ExpressionAttributeValues[":newTokens"].N).toBe("9"); // 10 - 1
    expect(
      updateParams.ExpressionAttributeValues[":expectedLastRefill"].N,
    ).toBe(initialState.lastRefill.toString());
  });

  it("should send UpdateItemCommand with correct parameters when refilled but not consumed", async () => {
    jest.setSystemTime(new Date("2025-11-23T12:01:00.000Z")); // 60 seconds later
    const initialState = {
      tokens: 400,
      lastRefill: initialTime,
      refillRate: 10,
      refillInterval: 60,
      maxTokens: 500,
      currentTime: new Date("2025-11-23T12:01:00.000Z").getTime(),
      cappedTokens: 410, // 400 + 10
    };

    await syncToDynamo(mockDdbClient, tableName, clientId, initialState, false);

    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(mockSend).toHaveBeenCalledWith(expect.any(UpdateItemCommand));
    const updateParams = mockSend.mock.calls[0][0].input;
    expect(updateParams.ExpressionAttributeValues[":newTokens"].N).toBe("410"); // 400 + 10
  });

  it("should refresh cache on ConditionalCheckFailedException and not retry sync", async () => {
    const initialState = {
      tokens: 10,
      lastRefill: initialTime,
      refillRate: 10,
      refillInterval: 60,
      maxTokens: 500,
      currentTime: initialTime,
      cappedTokens: 10,
    };
    mockSend.mockRejectedValueOnce({ name: "ConditionalCheckFailedException" }); // First update fails
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "5" },
        lastRefill: { N: (initialTime + 100).toString() },
      },
    }); // GetItem for cache refresh

    await syncToDynamo(
      mockDdbClient,
      tableName,
      clientId,
      initialState,
      true,
      1,
    ); // 1 retry

    expect(mockSend).toHaveBeenCalledTimes(3); // Update + GetItem plus 1 Retry Update
    expect(mockSend).toHaveBeenCalledWith(expect.any(UpdateItemCommand));
    expect(mockSend).toHaveBeenCalledWith(expect.any(GetItemCommand));
    expect(error).not.toHaveBeenCalled(); // No error logged for first collision
    const cached = memoryCache.get(clientId);
    expect(cached).toMatchObject({
      tokens: 4,
      lastRefill: initialTime + 100,
    });
  });

  it("should log error if ConditionalCheckFailedException occurs and no retries left", async () => {
    const initialState = {
      tokens: 10,
      lastRefill: initialTime,
      refillRate: 10,
      refillInterval: 60,
      maxTokens: 500,
      currentTime: initialTime,
      cappedTokens: 10,
    };
    mockSend.mockRejectedValueOnce({ name: "ConditionalCheckFailedException" }); // First update fails

    await syncToDynamo(
      mockDdbClient,
      tableName,
      clientId,
      initialState,
      true,
      0,
    ); // 0 retries

    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(error).toHaveBeenCalledWith(
      "Async DynamoDB UpdateItem failed after retry due to collision.",
      { clientId },
    );
  });

  it("should log other DynamoDB errors", async () => {
    const initialState = {
      tokens: 10,
      lastRefill: initialTime,
      refillRate: 10,
      refillInterval: 60,
      maxTokens: 500,
      currentTime: initialTime,
      cappedTokens: 10,
    };
    mockSend.mockRejectedValueOnce(new Error("Network issue"));

    await syncToDynamo(mockDdbClient, tableName, clientId, initialState, true);

    expect(mockSend).toHaveBeenCalledTimes(1);
    expect(error).toHaveBeenCalledWith(
      "Async DynamoDB UpdateItem error in hybrid:",
      expect.any(Error),
    );
  });
});
