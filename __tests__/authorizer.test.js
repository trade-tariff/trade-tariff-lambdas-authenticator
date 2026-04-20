const mockVerify = jest.fn();

jest.mock("aws-jwt-verify", () => ({
  CognitoJwtVerifier: {
    create: jest.fn(() => ({
      verify: mockVerify,
    })),
  },
}));

jest.mock("../src/logger", () => ({
  info: jest.fn(),
  error: jest.fn(),
}));

function loadHandler() {
  jest.resetModules();
  const fresh = require("../src/authorizer");
  return {
    handler: fresh.handler,
    info: require("../src/logger").info,
    error: require("../src/logger").error,
  };
}

function createEvent({
  authorization,
  methodArn = "arn:aws:execute-api:eu-west-2:123456789012:apiid/development/GET/uk/api/commodities",
  path = "/uk/api/commodities",
  method = "GET",
} = {}) {
  return {
    type: "REQUEST",
    methodArn,
    path,
    httpMethod: method,
    headers: authorization ? { Authorization: authorization } : {},
  };
}

describe("authorizer authorizer", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockVerify.mockReset();
    mockVerify.mockResolvedValue({
      client_id: "test-client",
      scope: "tariff/read",
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("rejects requests without an Authorization header and logs the reason", async () => {
    const { handler, info } = loadHandler();

    await expect(handler(createEvent())).rejects.toThrow("Unauthorized");
    expect(info).toHaveBeenCalledWith(
      "authorizer decision",
      expect.objectContaining({
        decision: "unauthorized",
        reason: "missing_authorization_header",
      }),
    );
  });

  it("rejects requests with a non-Bearer Authorization header and logs the reason", async () => {
    const { handler, info } = loadHandler();

    await expect(
      handler(createEvent({ authorization: "Basic abc" })),
    ).rejects.toThrow("Unauthorized");
    expect(info).toHaveBeenCalledWith(
      "authorizer decision",
      expect.objectContaining({
        decision: "unauthorized",
        reason: "invalid_authorization_header",
      }),
    );
  });

  it("rejects requests when token verification fails", async () => {
    mockVerify.mockRejectedValue(new Error("Verification failed"));
    const { handler, error, info } = loadHandler();

    await expect(
      handler(createEvent({ authorization: "Bearer token" })),
    ).rejects.toThrow("Unauthorized");
    expect(error).toHaveBeenCalledWith(
      "Token verification failed:",
      expect.any(Error),
    );
    expect(info).toHaveBeenCalledWith(
      "authorizer decision",
      expect.objectContaining({
        decision: "unauthorized",
        reason: "token_verification_failed",
      }),
    );
  });

  it("rejects requests when the token payload has no client id", async () => {
    mockVerify.mockResolvedValue({
      scope: "tariff/read",
    });
    const { handler, info } = loadHandler();

    await expect(
      handler(createEvent({ authorization: "Bearer token" })),
    ).rejects.toThrow("Unauthorized");
    expect(info).toHaveBeenCalledWith(
      "authorizer decision",
      expect.objectContaining({
        decision: "unauthorized",
        reason: "missing_client_id",
      }),
    );
  });

  it("returns a deny policy for a valid token without matching scope access and logs the decision", async () => {
    mockVerify.mockResolvedValue({
      client_id: "test-client",
      scope: "fpo/read",
    });
    const { handler, info } = loadHandler();
    const event = createEvent({
      authorization: "Bearer token",
      path: "/uk/api/commodities",
    });

    await expect(handler(event)).resolves.toEqual(
      expect.objectContaining({
        principalId: "test-client",
        usageIdentifierKey: "test-client",
        policyDocument: expect.objectContaining({
          Statement: [
            expect.objectContaining({
              Effect: "Deny",
              Resource: event.methodArn,
            }),
          ],
        }),
      }),
    );
    expect(info).toHaveBeenCalledWith(
      "authorizer decision",
      expect.objectContaining({
        decision: "deny",
        client_id: "test-client",
        path: "/uk/api/commodities",
        method: "GET",
      }),
    );
  });

  it("returns an allow policy for a valid token with matching scope access and logs the decision", async () => {
    const { handler, info } = loadHandler();
    const event = createEvent({ authorization: "Bearer token" });

    await expect(handler(event)).resolves.toEqual({
      principalId: "test-client",
      policyDocument: {
        Version: "2012-10-17",
        Statement: [
          {
            Action: "execute-api:Invoke",
            Effect: "Allow",
            Resource: event.methodArn,
          },
        ],
      },
      context: {
        client_id: "test-client",
      },
      usageIdentifierKey: "test-client",
    });
    expect(info).toHaveBeenCalledWith(
      "authorizer decision",
      expect.objectContaining({
        decision: "allow",
        client_id: "test-client",
        path: "/uk/api/commodities",
        method: "GET",
      }),
    );
  });

  it("reuses a cached token payload within the configured cache expiry", async () => {
    jest.spyOn(Date, "now").mockReturnValue(1_700_000_000_000);
    const { handler } = loadHandler();
    const event = createEvent({ authorization: "Bearer cached-token" });

    await handler(event);

    Date.now.mockReturnValue(1_700_000_149_000);
    await handler(event);

    expect(mockVerify).toHaveBeenCalledTimes(1);
  });

  it("re-verifies a cached token payload after the configured cache expiry", async () => {
    jest.spyOn(Date, "now").mockReturnValue(1_700_000_000_000);
    const { handler } = loadHandler();
    const event = createEvent({ authorization: "Bearer expired-cache-token" });

    await handler(event);

    Date.now.mockReturnValue(1_700_000_151_000);
    await handler(event);

    expect(mockVerify).toHaveBeenCalledTimes(2);
  });

  it("re-verifies a cached token payload when the JWT expiry is reached first", async () => {
    jest.spyOn(Date, "now").mockReturnValue(1_700_000_000_000);
    mockVerify.mockResolvedValue({
      client_id: "test-client",
      scope: "tariff/read",
      exp: 1_700_000_010,
    });
    const { handler } = loadHandler();
    const event = createEvent({ authorization: "Bearer jwt-expiry-token" });

    await handler(event);

    Date.now.mockReturnValue(1_700_000_011_000);
    await handler(event);

    expect(mockVerify).toHaveBeenCalledTimes(2);
  });
});
