const {
  applyRateLimit,
  memoryCache,
} = require("../src/rateLimiterHybridMemoryDynamo");
const { GetItemCommand } = require("@aws-sdk/client-dynamodb");

// Mock DynamoDB client
const mockSend = jest.fn();
const mockDdbClient = { send: mockSend };

describe("applyRateLimit", () => {
  beforeEach(() => {
    jest.resetAllMocks();
    jest.useFakeTimers();
    memoryCache.clear();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("allows request for new client with full burst", async () => {
    mockSend.mockResolvedValueOnce({ Item: undefined }); // GetItem: New client
    mockSend.mockResolvedValueOnce({ Attributes: { tokens: { N: "749" } } }); // Update succeeds
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result).toStrictEqual({
      allowed: true,
      rateLimitRemaining: 499,
      rateLimitLimit: 500,
      rateLimitReset: 1,
      collision: false,
    });
    expect(mockSend).toHaveBeenCalledTimes(2); // Get + async Update
    expect(mockSend.mock.calls[0][0]).toBeInstanceOf(GetItemCommand);
    const updateParams = mockSend.mock.calls[1][0].input;
    expect(updateParams.ExpressionAttributeValues[":newTokens"].N).toBe("499");
    expect(updateParams.ExpressionAttributeValues[":currentTime"].N).toBe(
      Date.now().toString(),
    );
    expect(updateParams.ExpressionAttributeValues[":maxTokens"].N).toBe("500");
    expect(updateParams.ExpressionAttributeValues[":refillRate"].N).toBe("300");
  });

  it("denies request when tokens are depleted and no refill", async () => {
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "0" },
        lastRefill: { N: Date.now().toString() },
        refillRate: { N: "300" },
        refillInterval: { N: "60" },
        maxTokens: { N: "500" },
      },
    }); // GetItem: Depleted
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result).toStrictEqual({
      allowed: false,
      rateLimitRemaining: 0,
      rateLimitLimit: 500,
      rateLimitReset: 100,
      collision: false,
    });
    expect(mockSend).toHaveBeenCalledTimes(1); // Only Get, no update since denied
  });

  it("allows request after partial refill", async () => {
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "0" },
        lastRefill: { N: Date.now() - 30000 }, // -30s
        refillRate: { N: "750" },
        refillInterval: { N: "60" },
        maxTokens: { N: "750" },
      },
    }); // GetItem: Depleted but time passed
    mockSend.mockResolvedValueOnce({}); // Update succeeds
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result).toStrictEqual({
      allowed: true,
      rateLimitRemaining: 374,
      rateLimitLimit: 750,
      rateLimitReset: 31,
      collision: false,
    });
    expect(mockSend).toHaveBeenCalledTimes(2); // Get + async Update
    const updateParams = mockSend.mock.calls[1][0].input;
    expect(updateParams.ExpressionAttributeValues[":currentTime"].N).toBe(
      Date.now().toString(),
    );
  });

  it("uses cache for subsequent requests without DynamoDB fetch if not stale", async () => {
    // First call: Fetches from DynamoDB
    mockSend.mockResolvedValueOnce({ Item: undefined }); // New client
    mockSend.mockResolvedValueOnce({}); // Update succeeds
    await applyRateLimit(mockDdbClient, "client-rate-limits", "test-client");
    expect(mockSend).toHaveBeenCalledTimes(2); // Get + Update

    // Second call: Uses cache (assuming within 1s)
    jest.advanceTimersByTime(50); // Advance time by 50ms
    mockSend.mockResolvedValueOnce({}); // Update succeeds
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result.allowed).toBe(true);
    expect(result.allowed).toBe(true);
    expect(result.rateLimitRemaining).toBe(498);
    expect(mockSend).toHaveBeenCalledTimes(3); // Only additional async Update, no Get
  });

  it("fetches from DynamoDB if cache is stale", async () => {
    // Populate cache manually (simulate previous call)
    memoryCache.set("test-client", {
      tokens: 500,
      lastRefill: Date.now() - 1001, // 1001ms ago to make stale
      refillRate: 300,
      refillInterval: 60,
      maxTokens: 500,
      currentTime: Date.now(),
      lastAccess: Date.now() - 1001,
    });

    // Advance time to make stale
    mockSend.mockResolvedValueOnce({ Item: undefined }); // GetItem (stale fetch)
    mockSend.mockResolvedValueOnce(); // Update succeeds
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result.allowed).toBe(true);
    expect(mockSend).toHaveBeenCalledTimes(2); // Get + Update
  });

  it("caps maxTokens over hard limit and persists the cap", async () => {
    mockSend.mockResolvedValueOnce({
      Item: {
        maxTokens: { N: "3000" }, // Over hard max
      },
    }); // GetItem: Invalid config
    mockSend.mockResolvedValueOnce({});
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result).toStrictEqual({
      allowed: true,
      rateLimitRemaining: 2499,
      rateLimitLimit: 2500,
      rateLimitReset: 1,
      collision: false,
    });
    expect(mockSend).toHaveBeenCalledTimes(2);
    const updateParams = mockSend.mock.calls[1][0].input;
    expect(updateParams.ExpressionAttributeValues[":newTokens"].N).toBe("2499");
    expect(updateParams.ExpressionAttributeValues[":maxTokens"].N).toBe("2500"); // Capped and persisted
  });

  it("uses default for invalid refillRate and persists it", async () => {
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "0" },
        lastRefill: { N: (Date.now() - 30000).toString() }, // -30s
        refillRate: { N: "abc" }, // Invalid NaN
        refillInterval: { N: "60" },
        maxTokens: { N: "750" },
      },
    }); // GetItem: Invalid
    mockSend.mockResolvedValueOnce({}); // Update
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result).toStrictEqual({
      allowed: true,
      rateLimitRemaining: 149,
      rateLimitLimit: 750,
      rateLimitReset: 121,
      collision: false,
    }); // Uses default 750 for refill
    expect(mockSend).toHaveBeenCalledTimes(2);
    const updateParams = mockSend.mock.calls[1][0].input;
    expect(updateParams.ExpressionAttributeValues[":refillRate"].N).toBe("300"); // Persisted default
  });

  it("clamps negative tokens to 0 and denies if no refill", async () => {
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "-5" }, // Negative
        lastRefill: { N: Date.now() },
        refillRate: { N: "750" },
        refillInterval: { N: "60" },
        maxTokens: { N: "750" },
      },
    });
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result).toStrictEqual({
      allowed: false,
      rateLimitRemaining: 0,
      rateLimitLimit: 750,
      rateLimitReset: 60,
      collision: false,
    }); // Clamped to 0, denied
    expect(mockSend).toHaveBeenCalledTimes(1); // Only Get, no update since forever denied
  });

  it("clamps refillRate below min to 1", async () => {
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "0" },
        lastRefill: { N: (Date.now() - 30000).toString() }, // -30s
        refillRate: { N: "0" }, // Below min
        refillInterval: { N: "60" },
        maxTokens: { N: "750" },
      },
    });
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result).toStrictEqual({
      allowed: false, // Refill rate clamped to 1 - 1 token per 60s is too slow to refill in 30s
      rateLimitRemaining: 0,
      rateLimitLimit: 750,
      rateLimitReset: 45000,
      collision: false,
    });
    expect(mockSend).toHaveBeenCalledTimes(1); // Denied, no update
  });

  it("caps refill on large time delta", async () => {
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "0" },
        lastRefill: { N: Date.now() - 600000 }, // 10min ago
        refillRate: { N: "750" },
        refillInterval: { N: "60" },
        maxTokens: { N: "750" },
      },
    });
    mockSend.mockResolvedValueOnce({}); // Update succeeds
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result).toStrictEqual({
      allowed: true,
      rateLimitRemaining: 749,
      rateLimitLimit: 750,
      rateLimitReset: 1,
      collision: false,
    });
    expect(mockSend).toHaveBeenCalledTimes(2);
  });
});
