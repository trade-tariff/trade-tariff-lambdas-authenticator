const { applyRateLimit } = require("../src/rateLimiter");
const {
  GetItemCommand,
  UpdateItemCommand,
} = require("@aws-sdk/client-dynamodb");

// Mock DynamoDB client
const mockSend = jest.fn();
const mockDdbClient = { send: mockSend };

describe("applyRateLimit", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("allows request for new client with full burst", async () => {
    jest.setSystemTime(new Date(2025, 10, 3, 15, 30, 0));
    mockSend.mockResolvedValueOnce({ Item: undefined });
    mockSend.mockResolvedValueOnce({ Attributes: { tokens: { N: "2499" } } });

    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result).toStrictEqual({ allowed: true, rateLimitRemaining: 2499 });
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(mockSend.mock.calls[0][0]).toBeInstanceOf(GetItemCommand);
    const updateParams = mockSend.mock.calls[1][0].input;
    expect(updateParams.ExpressionAttributeValues[":newTokens"].N).toBe("2499");
    expect(updateParams.ExpressionAttributeValues[":oldLastRefill"].N).toBe(
      "1762183800",
    );
  });

  it("denies request when tokens are depleted and no refill", async () => {
    jest.setSystemTime(new Date(2025, 10, 3, 15, 30, 0)); // Unix: 1762183800
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "0" },
        lastRefill: { N: "1762183800" },
        refillRate: { N: "500" },
        refillInterval: { N: "60" },
        maxTokens: { N: "2500" },
      },
    }); // GetItem: Depleted

    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result).toStrictEqual({ allowed: false, rateLimitRemaining: 0 });
    expect(mockSend).toHaveBeenCalledTimes(1); // No update attempted
  });

  it("allows request after partial refill", async () => {
    jest.setSystemTime(new Date(2025, 10, 3, 15, 30, 30));
    mockSend.mockResolvedValueOnce({
      Item: {
        tokens: { N: "0" },
        lastRefill: { N: "1762183800" },
        refillRate: { N: "500" },
        refillInterval: { N: "60" },
        maxTokens: { N: "2500" },
      },
    }); // GetItem: Depleted but time passed

    mockSend.mockResolvedValueOnce({ Attributes: { tokens: { N: "249" } } });

    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result).toStrictEqual({ allowed: true, rateLimitRemaining: 249 });
    const updateParams = mockSend.mock.calls[1][0].input;
    expect(updateParams.ExpressionAttributeValues[":newTokens"].N).toBe("249"); // (0 + 250) -1
    expect(updateParams.ExpressionAttributeValues[":oldLastRefill"].N).toBe(
      "1762183800",
    );
  });

  it("denies on concurrent modification (conditional fail)", async () => {
    jest.setSystemTime(new Date(2025, 10, 3, 15, 30, 0)); // Unix: 1762183800
    mockSend.mockResolvedValueOnce({ Item: undefined });
    const mockError = new Error("Condition failed");
    mockError.name = "ConditionalCheckFailedException";
    mockSend.mockRejectedValueOnce(mockError);

    const result = await applyRateLimit(
      mockDdbClient,
      "client-rate-limits",
      "test-client",
    );
    expect(result).toStrictEqual({ allowed: false, rateLimitRemaining: 0 });
    expect(mockSend).toHaveBeenCalledTimes(2);
  });
});
