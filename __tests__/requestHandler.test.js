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

const mockVerify = jest.fn();

// Mock external libs
jest.mock("aws-jwt-verify", () => ({
  CognitoJwtVerifier: {
    create: jest.fn(() => ({
      verify: mockVerify,
    })),
  },
}));

jest.mock("@aws-sdk/client-dynamodb", () => ({
  DynamoDBClient: jest.fn().mockImplementation(() => ({})),
}));

jest.mock("@smithy/node-http-handler", () => ({
  NodeHttpHandler: jest.fn(),
}));
jest.mock("https", () => ({
  Agent: jest.fn(),
}));

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

  // NOTE: When modules reset, the handler re-runs top-level code.
  // We need to ensure mocks are ready before requiring.
  setupMocks();

  const freshModule = require("../src/requestHandler");
  handler = freshModule.handler;
  return handler;
}

let reducedAtomicityHybridLimitV1;
let reducedAtomicityHybridLimitV2;
let fullyAtomicRateLimit;
let error;

const mockJwtPayload = {
  scope: "tariff/read",
  client_id: "test-client",
};

function setupMocks(overrides = {}) {
  // Reset the shared verify spy implementation
  mockVerify.mockReset();
  mockVerify.mockResolvedValue({
    ...mockJwtPayload,
    ...overrides.jwtPayload,
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
  return {};
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

describe("requestHandler", () => {
  let mockCallback;

  beforeEach(() => {
    mockCallback = jest.fn();
    setupMocks();
  });

  // Scenario 1: No Authorization header
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
    expect(reducedAtomicityHybridLimitV2).not.toHaveBeenCalled();
  });

  // Scenario 2: Invalid auth (not Bearer)
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

  // Scenario 3: JWT verify fails
  it("returns 401 when JWT verify fails", async () => {
    mockVerify.mockRejectedValue(new Error("Verification failed"));

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

  // Scenario 4: Unauthorized scopes
  it("returns 403 when scopes do not authorize the path", async () => {
    setupMocks({
      jwtPayload: { scope: "invalid/scope", client_id: "test-client" },
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

  // Scenario 5: Valid, authorized, rate limit allowed
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
          "x-client-id": [{ key: "X-Client-Id", value: "test-client" }],
        }),
      }),
    );
    expect(mockVerify).toHaveBeenCalledWith("token");
  });

  // Scenario 6: Rate limit denied
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
      }),
    );
  });

  // Scenario 7: Collision flagged
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

  // Scenario 8: Configurable limiter
  it("uses configurable limiter via header when enabled", async () => {
    loadHandlerWithConfig({ RATE_LIMITER_CONFIGURABLE_VIA_HEADER: true });

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

  // Scenario 9: No client_id in JWT
  it("returns 401 when no client_id in JWT", async () => {
    mockVerify.mockResolvedValue({ scope: "tariff/read" }); // No client_id here

    const event = createEvent({ headers: { Authorization: "Bearer token" } });
    await handler(event, createContext(), mockCallback);

    expect(mockCallback).toHaveBeenCalledWith(
      null,
      expect.objectContaining({
        status: "401",
        statusDescription: "Unauthorized",
      }),
    );
  });
});
