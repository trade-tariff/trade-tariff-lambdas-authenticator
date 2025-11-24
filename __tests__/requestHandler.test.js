// Mock rate limiters
jest.mock("../src/rateLimiterHybridMemoryDynamo", () => ({
  applyRateLimit: jest.fn(),
}));
jest.mock("../src/rateLimiterHybridMemoryDynamoV2", () => ({
  applyRateLimit: jest.fn(),
}));
jest.mock("../src/rateLimiterAtomicDynamoDb", () => ({
  applyRateLimit: jest.fn(),
}));
// Mock external libs with factories for proper mocking
jest.mock("aws-jwt-verify", () => ({
  CognitoJwtVerifier: {
    create: jest.fn(),
  },
}));
jest.mock("@aws-sdk/client-dynamodb", () => ({
  DynamoDBClient: jest.fn().mockImplementation(() => ({})), // Mock constructor
}));
jest.mock("jwt-decode", () => ({ jwtDecode: jest.fn() }));
jest.mock("../src/logger", () => ({
  error: jest.fn(),
}));

let { handler } = require("../src/requestHandler");

function loadHandlerWithConfig(overrides = {}) {
  let defaultConfiguration = require("../src/config.json");
  jest.doMock("../src/config.json", () => ({
    ...defaultConfiguration,
    ...overrides,
  }));
  jest.resetModules();
  setupMocks();
  const freshModule = require("../src/requestHandler");
  handler = freshModule.handler;
  return handler;
}

let jwtDecode;
let reducedAtomicityHybridLimitV1;
let reducedAtomicityHybridLimitV2;
let fullyAtomicRateLimit;
let CognitoJwtVerifier;
let error;

function setupMocks(overrides = {}) {
  // Re-acquire current mocks (post-any-reset) to ensure we configure the active instances
  jwtDecode = require("jwt-decode").jwtDecode;
  jwtDecode.mockReturnValue({
    client_id: "test-client",
    ...overrides.jwtDecode,
  });

  CognitoJwtVerifier = require("aws-jwt-verify").CognitoJwtVerifier;
  CognitoJwtVerifier.create.mockReturnValue({
    verify: jest
      .fn()
      .mockResolvedValue({ ...mockJwtPayload, ...overrides.jwtPayload }),
  });

  reducedAtomicityHybridLimitV1 =
    require("../src/rateLimiterHybridMemoryDynamo").applyRateLimit;
  reducedAtomicityHybridLimitV1.mockResolvedValue(
    generateRateLimitResult(...(overrides.rateLimitV1 || [])),
  );

  reducedAtomicityHybridLimitV2 =
    require("../src/rateLimiterHybridMemoryDynamoV2").applyRateLimit;
  reducedAtomicityHybridLimitV2.mockResolvedValue(
    generateRateLimitResult(...(overrides.rateLimitV2 || [])),
  );

  fullyAtomicRateLimit =
    require("../src/rateLimiterAtomicDynamoDb").applyRateLimit;
  fullyAtomicRateLimit.mockResolvedValue(
    generateRateLimitResult(...(overrides.fullyAtomic || [])),
  );

  error = require("../src/logger").error;
  error.mockImplementation(() => {});
}

// Test helpers
function createEvent({ uri = "/uk/api/v2/headings/0104", headers = {} } = {}) {
  return {
    Records: [
      {
        cf: {
          request: {
            uri,
            headers: Object.fromEntries(
              Object.entries(headers).map(([key, value]) => [
                key.toLowerCase(),
                [{ key, value }],
              ]),
            ),
          },
        },
      },
    ],
  };
}

function createContext() {
  return {}; // Minimal mock; handler doesn't use it
}

function generateRateLimitResult(
  allowed = true,
  collision = false,
  remaining = allowed ? 499 : 0,
  limit = 500,
  reset = allowed ? 1 : 60,
) {
  return {
    allowed,
    rateLimitRemaining: remaining,
    rateLimitLimit: limit,
    rateLimitReset: reset,
    collision,
  };
}

// Common mock setups
const mockJwtPayload = {
  scope: "tariff/read",
};

describe("requestHandler", () => {
  let mockCallback;

  beforeEach(() => {
    mockCallback = jest.fn();
    setupMocks();
  });

  // Scenario 1: No Authorization header → Forward with x-client-id: "unknown"
  it("forwards request as unauthenticated when no Authorization header", async () => {
    const event = createEvent({ headers: {} });
    await handler(event, createContext(), mockCallback);
    expect(mockCallback).toHaveBeenCalledWith(
      null,
      expect.objectContaining({
        headers: expect.objectContaining({
          "x-client-id": [{ key: "X-Client-Id", value: "unknown" }],
        }),
      }),
    );
    expect(reducedAtomicityHybridLimitV2).not.toHaveBeenCalled(); // No rate limit applied
  });

  // Scenario 2: Invalid auth (not Bearer) → 401
  it("returns 401 when Authorization is present but not Bearer", async () => {
    const event = createEvent({ headers: { Authorization: "Basic foo" } });
    await handler(event, createContext(), mockCallback);
    expect(mockCallback).toHaveBeenCalledWith(
      null,
      expect.objectContaining({
        status: "401",
        statusDescription: "Unauthorized",
      }),
    );
  });

  // Scenario 3: Invalid JWT (decode/verify fails) → 401
  it("returns 401 when JWT decode fails", async () => {
    jwtDecode.mockImplementation(() => {
      throw new Error("Invalid token");
    });
    const event = createEvent({
      headers: { Authorization: "Bearer invalidtoken" },
    });
    await handler(event, createContext(), mockCallback);
    expect(mockCallback).toHaveBeenCalledWith(
      null,
      expect.objectContaining({
        status: "401",
      }),
    );
    expect(error).toHaveBeenCalledWith(
      "Token verification failed:",
      expect.any(Error),
    );
  });

  it("returns 401 when JWT verify fails", async () => {
    CognitoJwtVerifier.create().verify.mockRejectedValue(
      new Error("Verification failed"),
    );
    const event = createEvent({ headers: { Authorization: "Bearer token" } });
    await handler(event, createContext(), mockCallback);
    expect(mockCallback).toHaveBeenCalledWith(
      null,
      expect.objectContaining({
        status: "401",
      }),
    );
    expect(error).toHaveBeenCalledWith(
      "Token verification failed:",
      expect.any(Error),
    );
  });

  // Scenario 4: Valid JWT but unauthorized scopes/path → 403
  it("returns 403 when scopes do not authorize the path", async () => {
    CognitoJwtVerifier.create().verify.mockResolvedValue({
      scope: "invalid/scope",
    });
    const event = createEvent({
      uri: "/uk/api/v2/headings/0104",
      headers: { Authorization: "Bearer token" },
    });
    await handler(event, createContext(), mockCallback);
    expect(mockCallback).toHaveBeenCalledWith(
      null,
      expect.objectContaining({
        status: "403",
        statusDescription: "Forbidden",
      }),
    );
  });

  // Scenario 5: Valid, authorized, rate limit allowed → Forward with headers + x-client-id
  it("forwards request with rate limit headers when allowed", async () => {
    const event = createEvent({ headers: { Authorization: "Bearer token" } });
    await handler(event, createContext(), mockCallback);
    expect(reducedAtomicityHybridLimitV2).toHaveBeenCalledWith(
      expect.anything(),
      "client-rate-limits",
      "test-client",
    );
    expect(mockCallback).toHaveBeenCalledWith(
      null,
      expect.objectContaining({
        headers: expect.objectContaining({
          "x-ratelimit-limit": [{ key: "X-RateLimit-Limit", value: "500" }],
          "x-ratelimit-remaining": [
            { key: "X-RateLimit-Remaining", value: "499" },
          ],
          "x-ratelimit-reset": [{ key: "X-RateLimit-Reset", value: "1" }],
          "x-client-id": [{ key: "X-Client-Id", value: "test-client" }],
        }),
      }),
    );
    expect(CognitoJwtVerifier.create().verify).toHaveBeenCalledWith("token");

    const call = CognitoJwtVerifier.create.mock.calls.length - 2;
    expect(CognitoJwtVerifier.create.mock.calls[call][0]).toEqual({
      userPoolId: "eu-west-2_eYCVlIQL0",
      tokenUse: "access",
      clientId: "test-client",
    });
  });

  // Scenario 6: Valid, authorized, rate limit denied → 429 with headers
  it("returns 429 when rate limit denied", async () => {
    reducedAtomicityHybridLimitV2.mockResolvedValue(
      generateRateLimitResult(false),
    );
    const event = createEvent({ headers: { Authorization: "Bearer token" } });
    await handler(event, createContext(), mockCallback);
    expect(mockCallback).toHaveBeenCalledWith(
      null,
      expect.objectContaining({
        status: "429",
        statusDescription: "Too Many Requests",
        headers: expect.objectContaining({
          "x-ratelimit-limit": expect.any(Array),
          "x-ratelimit-remaining": expect.any(Array),
          "x-ratelimit-reset": expect.any(Array),
        }),
      }),
    );
  });

  // Scenario 7: Collision flagged → Include x-ratelimit-collision header
  it("includes collision header when flagged", async () => {
    reducedAtomicityHybridLimitV2.mockResolvedValue(
      generateRateLimitResult(true, true),
    );
    const event = createEvent({ headers: { Authorization: "Bearer token" } });
    await handler(event, createContext(), mockCallback);
    expect(mockCallback).toHaveBeenCalledWith(
      null,
      expect.objectContaining({
        headers: expect.objectContaining({
          "x-ratelimit-collision": [
            { key: "X-RateLimit-Collision", value: "true" },
          ],
        }),
      }),
    );
  });

  // Scenario 8: Configurable limiter via header (when enabled) → Use specified limiter
  it("uses configurable limiter via header when enabled", async () => {
    loadHandlerWithConfig({ RATE_LIMITER_CONFIGURABLE_VIA_HEADER: true });
    // Temporarily override const for test
    const event = createEvent({
      headers: {
        Authorization: "Bearer token",
        "x-rate-limiter": "fully-atomic-dynamo",
      },
    });
    await handler(event, createContext(), mockCallback);
    expect(fullyAtomicRateLimit).toHaveBeenCalled();
    expect(reducedAtomicityHybridLimitV2).not.toHaveBeenCalled();
  });

  it("defaults to v2 limiter on invalid header when configurable", async () => {
    loadHandlerWithConfig({ RATE_LIMITER_CONFIGURABLE_VIA_HEADER: true });
    const event = createEvent({
      headers: {
        Authorization: "Bearer token",
        "x-rate-limiter": "invalid-type",
      },
    });
    await handler(event, createContext(), mockCallback);
    expect(reducedAtomicityHybridLimitV2).toHaveBeenCalled();
  });

  it("defaults to v2 limiter when rate limiter not configurable", async () => {
    loadHandlerWithConfig({ RATE_LIMITER_CONFIGURABLE_VIA_HEADER: false });
    const event = createEvent({
      headers: { Authorization: "Bearer token" },
    });
    await handler(event, createContext(), mockCallback);
    expect(reducedAtomicityHybridLimitV2).toHaveBeenCalled();
  });

  // Scenario 9: No client_id in JWT → 401
  it("returns 401 when no client_id in JWT", async () => {
    jwtDecode.mockReturnValue({});
    const event = createEvent({ headers: { Authorization: "Bearer token" } });
    await handler(event, createContext(), mockCallback);
    expect(mockCallback).toHaveBeenCalledWith(
      null,
      expect.objectContaining({
        status: "401",
      }),
    );
  });
});
