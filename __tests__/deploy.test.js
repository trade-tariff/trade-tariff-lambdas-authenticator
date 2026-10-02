const { spawnSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

// Tests for .github/bin/deploy, which reads the MCP values from the
// mcp-shared-credentials secret before it runs `serverless deploy`. The fake
// aws and serverless commands in __tests__/fixtures/bin go first on PATH.
//
// The script uses associative arrays, so it needs bash 4 or later, and it
// needs jq. macOS has bash 3.2, so these tests skip there. In CI they must
// run, so a missing prerequisite is a failure, not a skip.

const DEPLOY_SCRIPT = path.join(__dirname, "..", ".github", "bin", "deploy");
const FAKE_BIN = path.join(__dirname, "fixtures", "bin");

function bashMajorVersion() {
  const result = spawnSync("bash", ["-c", "echo ${BASH_VERSINFO[0]}"], {
    encoding: "utf8",
  });
  if (result.status !== 0) return 0;
  return Number(result.stdout.trim());
}

function hasJq() {
  return spawnSync("jq", ["--version"]).status === 0;
}

const missingPrerequisites = [];
if (bashMajorVersion() < 4) missingPrerequisites.push("bash 4 or later");
if (!hasJq()) missingPrerequisites.push("jq");

if (missingPrerequisites.length > 0 && process.env.CI) {
  throw new Error(
    `deploy tests need ${missingPrerequisites.join(" and ")} in CI`,
  );
}

if (missingPrerequisites.length > 0) {
  console.warn(
    `Skipping deploy tests: they need ${missingPrerequisites.join(" and ")}.`,
  );
}

const describeWhenRunnable =
  missingPrerequisites.length > 0 ? describe.skip : describe;

function runDeploy(fakeAwsMode) {
  const outputDirectory = fs.mkdtempSync(
    path.join(os.tmpdir(), "deploy-test-"),
  );
  const serverlessOutput = path.join(outputDirectory, "serverless.json");

  const result = spawnSync("bash", [DEPLOY_SCRIPT, "staging"], {
    encoding: "utf8",
    env: {
      ...process.env,
      PATH: `${FAKE_BIN}:${process.env.PATH}`,
      FAKE_AWS_MODE: fakeAwsMode,
      FAKE_SERVERLESS_OUTPUT: serverlessOutput,
      // Old GitHub secrets must not reach the deploy any more.
      MCP_SECRET_TOKEN: "stale-github-secret",
      MCP_USAGE_KEY: "stale-github-secret",
    },
  });

  const serverlessRan = fs.existsSync(serverlessOutput);
  const serverlessEnv = serverlessRan
    ? JSON.parse(fs.readFileSync(serverlessOutput, "utf8"))
    : null;
  fs.rmSync(outputDirectory, { recursive: true, force: true });

  return { status: result.status, stderr: result.stderr, serverlessEnv };
}

describeWhenRunnable(".github/bin/deploy", () => {
  it("passes the values from mcp-shared-credentials to serverless", () => {
    const { status, serverlessEnv } = runDeploy("value");

    expect(status).toBe(0);
    expect(serverlessEnv).toEqual({
      MCP_SECRET_TOKEN: "token-from-secret",
      MCP_USAGE_KEY: "key-from-secret",
      STAGE: "staging",
    });
  });

  it("passes empty values when MCP is off in that environment", () => {
    const { status, serverlessEnv } = runDeploy("empty");

    expect(status).toBe(0);
    expect(serverlessEnv).toEqual({
      MCP_SECRET_TOKEN: "",
      MCP_USAGE_KEY: "",
      STAGE: "staging",
    });
  });

  it("stops before serverless when the secret has no value", () => {
    const { status, stderr, serverlessEnv } = runDeploy("no_value");

    expect(status).not.toBe(0);
    expect(stderr).toContain("Cannot read mcp-shared-credentials");
    expect(serverlessEnv).toBeNull();
  });

  it("stops before serverless when the secret cannot be read", () => {
    const { status, stderr, serverlessEnv } = runDeploy("denied");

    expect(status).not.toBe(0);
    expect(stderr).toContain("AccessDeniedException");
    expect(serverlessEnv).toBeNull();
  });

  it("stops before serverless when a key is missing from the secret", () => {
    const { status, stderr, serverlessEnv } = runDeploy("missing_key");

    expect(status).not.toBe(0);
    expect(stderr).toContain("mcp-shared-credentials has no MCP_USAGE_KEY");
    expect(serverlessEnv).toBeNull();
  });
});
