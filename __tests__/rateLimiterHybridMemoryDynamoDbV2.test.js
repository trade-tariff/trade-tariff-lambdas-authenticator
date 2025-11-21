const {
  applyRateLimit,
  memoryCache,
} = require("../src/rateLimiterHybridMemoryDynamoV2");

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

  // Existing tests (unchanged, they pass)
  it("allows request for new client with full burst", async () => {
    mockSend.mockResolvedValueOnce({ Item: undefined }); // GetItem: New client
    mockSend.mockResolvedValueOnce({}); // Update succeeds
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
    expect(updateParams.ExpressionAttributeValues[":newTokens"].N).toBe("498");
    expect(updateParams.ExpressionAttributeValues[":currentTime"].N).toBe(
      Date.now().toString(),
    );
    expect(updateParams.ExpressionAttributeValues[":maxTokens"].N).toBe("500");
    expect(updateParams.ExpressionAttributeValues[":refillRate"].N).toBe("300");
  });

  it("detects collision and retries with re-fetch, updating on success", async () => {
    const initialTime = Date.now();
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "10" },
        lastRefill: { N: initialTime.toString() },
        refillRate: { N: "300" },
        refillInterval: { N: "60" },
        maxTokens: { N: "500" },
      },
    }); // Initial Get

    // First Update fails (collision)
    mockSend.mockRejectedValueOnce({ name: "ConditionalCheckFailedException" });

    // Re-fetch Get during retry
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "12" }, // Simulate concurrent refill/change
        lastRefill: { N: (initialTime + 100).toString() }, // Slightly advanced
        refillRate: { N: "300" },
        refillInterval: { N: "60" },
        maxTokens: { N: "500" },
      },
    });

    // Second Update succeeds with refreshed values
    mockSend.mockResolvedValueOnce({});
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );

    await jest.runAllTimersAsync(); // Flush async
    expect(result.allowed).toBe(true);
    expect(result.rateLimitRemaining).toBe(9); // Based on cached token (and refresh happening async)
    expect(result.collision).toBe(true); // Flagged
    expect(mockSend).toHaveBeenCalledTimes(3); // Initial Get + failed Update + re-fetch Get and refresh of the cache
  });

  it("sets collision true after max retries fail", async () => {
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "10" },
        lastRefill: { N: Date.now().toString() },
        refillRate: { N: "300" },
        refillInterval: { N: "60" },
        maxTokens: { N: "500" },
      },
    }); // Initial Get
    // Fail Update twice (max retries=2)
    mockSend.mockRejectedValueOnce({ name: "ConditionalCheckFailedException" });
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "10" },
        lastRefill: { N: (Date.now() + 100).toString() },
      },
    }); // Re-fetch
    mockSend.mockRejectedValueOnce({ name: "ConditionalCheckFailedException" });
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "10" },
        lastRefill: { N: (Date.now() + 200).toString() },
      },
    }); // Second re-fetch (but retry stops)
    mockSend.mockRejectedValueOnce({ name: "ConditionalCheckFailedException" });
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "10" },
        lastRefill: { N: (Date.now() + 300).toString() },
      },
    }); // Second re-fetch (but retry stops)
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );

    await jest.runAllTimersAsync(); // Flush async
    expect(result.allowed).toBe(true); // Optimistic allow
    expect(result.collision).toBe(true);
    expect(mockSend).toHaveBeenCalledTimes(3);
    expect(mockSend.mock.calls[2][0]).toBeInstanceOf(GetItemCommand); // Last call is GetItem
  });

  it("handles DynamoDB error in async sync gracefully", async () => {
    mockSend.mockResolvedValueOnce({ Item: undefined }); // New client
    mockSend.mockRejectedValueOnce(new Error("Network error")); // Async Update fails
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    await jest.runAllTimersAsync(); // Flush async
    expect(result.allowed).toBe(true);
    expect(result.collision).toBe(false);
    expect(mockSend).toHaveBeenCalledTimes(2); // Get + failed Update
  });
});
