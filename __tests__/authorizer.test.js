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
  headers = {},
  methodArn = "arn:aws:execute-api:eu-west-2:123456789012:apiid/development/GET/uk/api/commodities",
  path = "/uk/api/commodities",
  method = "GET",
} = {}) {
  return {
    type: "REQUEST",
    methodArn,
    path,
    httpMethod: method,
    headers: authorization ? { Authorization: authorization, ...headers } : { ...headers },
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

  it("denies a tariff/read token on the categorisation path", async () => {
    mockVerify.mockResolvedValue({
      client_id: "test-client",
      scope: "tariff/read",
    });
    const { handler } = loadHandler();
    const event = createEvent({
      authorization: "Bearer token",
      path: "/xi/api/categorisation/themes",
      methodArn:
        "arn:aws:execute-api:eu-west-2:123456789012:apiid/development/GET/xi/api/categorisation/themes",
    });

    await expect(handler(event)).resolves.toEqual(
      expect.objectContaining({
        policyDocument: expect.objectContaining({
          Statement: [expect.objectContaining({ Effect: "Deny" })],
        }),
      }),
    );
  });

  it("denies a tariff/write token on the categorisation path", async () => {
    mockVerify.mockResolvedValue({
      client_id: "test-client",
      scope: "tariff/write",
    });
    const { handler } = loadHandler();
    const event = createEvent({
      authorization: "Bearer token",
      path: "/xi/api/categorisation/themes",
      methodArn:
        "arn:aws:execute-api:eu-west-2:123456789012:apiid/development/GET/xi/api/categorisation/themes",
    });

    await expect(handler(event)).resolves.toEqual(
      expect.objectContaining({
        policyDocument: expect.objectContaining({
          Statement: [expect.objectContaining({ Effect: "Deny" })],
        }),
      }),
    );
  });

  it("allows a tariff/categorisation token on the categorisation path", async () => {
    mockVerify.mockResolvedValue({
      client_id: "test-client",
      scope: "tariff/categorisation",
    });
    const { handler } = loadHandler();
    const event = createEvent({
      authorization: "Bearer token",
      path: "/xi/api/categorisation/themes",
      methodArn:
        "arn:aws:execute-api:eu-west-2:123456789012:apiid/development/GET/xi/api/categorisation/themes",
    });

    await expect(handler(event)).resolves.toEqual(
      expect.objectContaining({
        usageIdentifierKey: "test-client",
        policyDocument: expect.objectContaining({
          Statement: [expect.objectContaining({ Effect: "Allow" })],
        }),
      }),
    );
  });

  it("denies a tariff/categorisation token outside the categorisation path", async () => {
    mockVerify.mockResolvedValue({
      client_id: "test-client",
      scope: "tariff/categorisation",
    });
    const { handler } = loadHandler();
    const event = createEvent({
      authorization: "Bearer token",
      path: "/xi/api/commodities",
    });

    await expect(handler(event)).resolves.toEqual(
      expect.objectContaining({
        policyDocument: expect.objectContaining({
          Statement: [expect.objectContaining({ Effect: "Deny" })],
        }),
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

describe("MCP usage plan", () => {
  const ORIGINAL_ENV = process.env;

  beforeEach(() => {
    jest.clearAllMocks();
    mockVerify.mockReset();
    mockVerify.mockResolvedValue({
      client_id: "test-client",
      scope: "tariff/read",
    });
    process.env = { ...ORIGINAL_ENV, MCP_SECRET_TOKEN: "shared-secret", MCP_USAGE_KEY: "mcp-usage-key" };
  });

  afterEach(() => {
    process.env = ORIGINAL_ENV;
  });

  it("bills a valid MCP request to the shared MCP usage key", async () => {
    const { handler } = loadHandler();

    const result = await handler(
      createEvent({ authorization: "Bearer token", headers: { "x-mcp-token": "shared-secret" } }),
    );

    expect(result.usageIdentifierKey).toBe("mcp-usage-key");
  });

  it("keeps the end user identifiable on an MCP request", async () => {
    const { handler } = loadHandler();

    const result = await handler(
      createEvent({ authorization: "Bearer token", headers: { "x-mcp-token": "shared-secret" } }),
    );

    expect(result.principalId).toBe("test-client");
    expect(result.context.client_id).toBe("test-client");
  });

  it("accepts the header in canonical casing", async () => {
    const { handler } = loadHandler();

    const result = await handler(
      createEvent({ authorization: "Bearer token", headers: { "X-Mcp-Token": "shared-secret" } }),
    );

    expect(result.usageIdentifierKey).toBe("mcp-usage-key");
  });

  it("logs that the decision was for MCP traffic", async () => {
    const { handler, info } = loadHandler();

    await handler(createEvent({ authorization: "Bearer token", headers: { "x-mcp-token": "shared-secret" } }));

    expect(info).toHaveBeenCalledWith(
      "authorizer decision",
      expect.objectContaining({ mcp: true, client_id: "test-client" }),
    );
  });

  it("bills a request with no MCP header to the client's own key", async () => {
    const { handler } = loadHandler();

    const result = await handler(createEvent({ authorization: "Bearer token" }));

    expect(result.usageIdentifierKey).toBe("test-client");
  });

  it("bills a request with a mismatched MCP token to the client's own key", async () => {
    const { handler } = loadHandler();

    const result = await handler(
      createEvent({ authorization: "Bearer token", headers: { "x-mcp-token": "wrong-secret" } }),
    );

    expect(result.usageIdentifierKey).toBe("test-client");
  });

  it("bills a request with an empty MCP token to the client's own key", async () => {
    const { handler } = loadHandler();

    const result = await handler(
      createEvent({ authorization: "Bearer token", headers: { "x-mcp-token": "" } }),
    );

    expect(result.usageIdentifierKey).toBe("test-client");
  });

  it("falls back to the client's own key when the environment is not configured", async () => {
    process.env = { ...ORIGINAL_ENV };
    delete process.env.MCP_SECRET_TOKEN;
    delete process.env.MCP_USAGE_KEY;
    const { handler } = loadHandler();

    const result = await handler(
      createEvent({ authorization: "Bearer token", headers: { "x-mcp-token": "shared-secret" } }),
    );

    expect(result.usageIdentifierKey).toBe("test-client");
  });

  it("still denies an MCP request whose scope does not cover the path", async () => {
    mockVerify.mockResolvedValue({ client_id: "test-client", scope: "tariff/categorisation" });
    const { handler } = loadHandler();

    const result = await handler(
      createEvent({ authorization: "Bearer token", headers: { "x-mcp-token": "shared-secret" } }),
    );

    expect(result.policyDocument.Statement[0].Effect).toBe("Deny");
  });

  it("still rejects an MCP request with no token at all", async () => {
    const { handler } = loadHandler();

    await expect(handler(createEvent({ headers: { "x-mcp-token": "shared-secret" } }))).rejects.toThrow(
      "Unauthorized",
    );
  });
});
