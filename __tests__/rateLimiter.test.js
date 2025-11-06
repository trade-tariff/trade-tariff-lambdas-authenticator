const { applyRateLimit } = require("../src/rateLimiter");
const { GetItemCommand } = require("@aws-sdk/client-dynamodb");
// Mock DynamoDB client
const mockSend = jest.fn();
const mockDdbClient = { send: mockSend };
describe("applyRateLimit", () => {
  beforeEach(() => {
    jest.resetAllMocks();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("allows request for new client with full burst", async () => {
    jest.setSystemTime(new Date(2025, 10, 3, 15, 30, 0)); // Unix: 1762183800
    mockSend.mockResolvedValueOnce({ Item: undefined }); // GetItem: New client
    mockSend.mockResolvedValueOnce({ Attributes: { tokens: { N: "749" } } }); // Update succeeds
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
    });
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(mockSend.mock.calls[0][0]).toBeInstanceOf(GetItemCommand);
    const updateParams = mockSend.mock.calls[1][0].input;
    expect(updateParams.ExpressionAttributeValues[":newTokens"].N).toBe("749");
    expect(updateParams.ExpressionAttributeValues[":oldLastRefill"].N).toBe(
      "1762183800000",
    );
    expect(updateParams.ExpressionAttributeValues[":maxTokens"].N).toBe("750"); // Default persisted
    expect(updateParams.ExpressionAttributeValues[":refillRate"].N).toBe("750"); // Default persisted
  });

  it("denies request when tokens are depleted and no refill", async () => {
    jest.setSystemTime(new Date(2025, 10, 3, 15, 30, 0)); // Unix: 1762183800
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "0" },
        lastRefill: { N: "1762183800000" },
        refillRate: { N: "750" },
        refillInterval: { N: "60" },
        maxTokens: { N: "750" },
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
      rateLimitLimit: 750,
      rateLimitReset: 60,
    });
    expect(mockSend).toHaveBeenCalledTimes(1); // No update attempted
  });

  it("allows request after partial refill", async () => {
    jest.setSystemTime(new Date(2025, 10, 3, 15, 30, 30)); // Unix: 1762183830 (+30s)
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "0" },
        lastRefill: { N: "1762183800000" },
        refillRate: { N: "750" },
        refillInterval: { N: "60" },
        maxTokens: { N: "750" },
      },
    }); // GetItem: Depleted but time passed
    mockSend.mockResolvedValueOnce({ Attributes: { tokens: { N: "374" } } }); // Update succeeds
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
    });
    const updateParams = mockSend.mock.calls[1][0].input;
    expect(updateParams.ExpressionAttributeValues[":newTokens"].N).toBe("374"); // floor((0 + 375) -1)
    expect(updateParams.ExpressionAttributeValues[":oldLastRefill"].N).toBe(
      "1762183800000",
    );
  });

  it("denies on concurrent modification (conditional fail)", async () => {
    jest.setSystemTime(new Date(2025, 10, 3, 15, 30, 0)); // Unix: 1762183800
    mockSend.mockResolvedValueOnce({ Item: undefined }); // GetItem: New client
    const mockError = new Error("Condition failed");
    mockError.name = "ConditionalCheckFailedException";
    mockSend.mockRejectedValueOnce(mockError); // Update fails condition
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );

    expect(result).toStrictEqual({
      allowed: false,
      rateLimitRemaining: 749,
      rateLimitLimit: 750,
      rateLimitReset: 1,
    });
    expect(mockSend).toHaveBeenCalledTimes(2);
  });

  it("caps maxTokens over hard limit and persists the cap", async () => {
    jest.setSystemTime(new Date(2025, 10, 3, 15, 30, 0)); // Unix: 1762183800
    mockSend.mockResolvedValueOnce({
      Item: {
        maxTokens: { N: "3000" }, // Over hard max
      },
    }); // GetItem: Invalid config
    mockSend.mockResolvedValueOnce({ Attributes: { tokens: { N: "2499" } } }); // Update succeeds
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
    });
    const updateParams = mockSend.mock.calls[1][0].input;
    expect(updateParams.ExpressionAttributeValues[":newTokens"].N).toBe("2499");
    expect(updateParams.ExpressionAttributeValues[":maxTokens"].N).toBe("2500"); // Capped and persisted
  });

  it("uses default for invalid refillRate and persists it", async () => {
    jest.setSystemTime(new Date(2025, 10, 3, 15, 30, 30)); // +30s
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "0" },
        lastRefill: { N: "1762183800000" },
        refillRate: { N: "abc" }, // Invalid NaN
        refillInterval: { N: "60" },
        maxTokens: { N: "750" },
      },
    }); // GetItem: Invalid
    mockSend.mockResolvedValueOnce({ Attributes: { tokens: { N: "374" } } }); // Update
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
    }); // Uses default 750 for refill
    const updateParams = mockSend.mock.calls[1][0].input;
    expect(updateParams.ExpressionAttributeValues[":refillRate"].N).toBe("750"); // Persisted default
  });
  it("clamps negative tokens to 0 and denies if no refill", async () => {
    jest.setSystemTime(new Date(2025, 10, 3, 15, 30, 0));
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "-5" }, // Negative
        lastRefill: { N: "1762183800000" },
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
    }); // Clamped to 0, denied
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it("clamps refillRate below min to 1", async () => {
    jest.setSystemTime(new Date(2025, 10, 3, 15, 30, 30)); // +30s
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "0" },
        lastRefill: { N: "1762183800000" },
        refillRate: { N: "0" }, // Below min
        refillInterval: { N: "60" },
        maxTokens: { N: "750" },
      },
    });
    mockSend.mockResolvedValueOnce({ Attributes: { tokens: { N: "0" } } });
    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result).toStrictEqual({
      allowed: false,
      rateLimitRemaining: 0,
      rateLimitLimit: 750,
      rateLimitReset: 45000,
    });
    const updateParams = mockSend.mock.calls[1];
    expect(mockSend).toHaveBeenCalledTimes(1);
  });

  it("caps refill on large time delta", async () => {
    jest.setSystemTime(new Date(2025, 10, 3, 15, 40, 0)); // +600s (10min)
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "0" },
        lastRefill: { N: "1762183800" },
        refillRate: { N: "750" },
        refillInterval: { N: "60" },
        maxTokens: { N: "750" },
      },
    });
    mockSend.mockResolvedValueOnce({ Attributes: { tokens: { N: "749" } } });
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
    });
  });
});
