jest.mock("aws-jwt-verify", () => ({
  CognitoJwtVerifier: {
    create: jest.fn(() => ({ verify: jest.fn() })),
  },
}));

describe("configuration loading", () => {
  const originalEnv = process.env;

  beforeEach(() => {
    jest.resetModules();
    process.env = { ...originalEnv };
    delete process.env.USER_POOL_ID;
    delete process.env.LOG_LEVEL;
  });

  afterAll(() => {
    process.env = originalEnv;
  });

  it("uses USER_POOL_ID from the environment when building the verifier", () => {
    process.env.USER_POOL_ID = "eu-west-2_env_pool";
    const { CognitoJwtVerifier } = require("aws-jwt-verify");

    require("../src/authorizer");

    expect(CognitoJwtVerifier.create).toHaveBeenCalledWith(
      expect.objectContaining({
        userPoolId: "eu-west-2_env_pool",
      }),
    );
  });

  it("prefers LOG_LEVEL from the environment over config defaults", () => {
    process.env.LOG_LEVEL = "WARN";
    const logSpy = jest.spyOn(console, "log").mockImplementation(() => {});

    jest.isolateModules(() => {
      const logger = require("../src/logger");
      logger.info("ignored");
      logger.warn("shown");
    });

    expect(logSpy).toHaveBeenCalledTimes(1);
    expect(logSpy).toHaveBeenCalledWith(
      expect.stringContaining('"level":"WARN"'),
    );

    logSpy.mockRestore();
  });
});
