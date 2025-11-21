const LOG_LEVEL = "DEBUG";

const LEVELS = {
  ERROR: 0,
  WARN: 1,
  INFO: 2,
  DEBUG: 3,
};

function getLevelNum(level) {
  return LEVELS[level.toUpperCase()] ?? 2;
}

function log(level, message, data = {}) {
  const currentLevelNum = getLevelNum(LOG_LEVEL);
  const logLevelNum = getLevelNum(level);
  if (logLevelNum > currentLevelNum) return;
  const entry = {
    timestamp: new Date().toISOString(),
    level: level.toUpperCase(),
    message,
    ...data,
  };
  console.log(JSON.stringify(entry));
}

function debug(message, data = {}) {
  log("DEBUG", message, data);
}

function info(message, data = {}) {
  log("INFO", message, data);
}

function warn(message, data = {}) {
  log("WARN", message, data);
}

function error(message, err) {
  const isErrorObject = err instanceof Error;
  if (!isErrorObject) {
    log("ERROR", message, { error: err });
    return;
  } else {
    log("ERROR", message, { error: err.message, stack: err.stack });
  }
}

module.exports = {
  debug,
  info,
  warn,
  error,
};
