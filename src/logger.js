const LOG_LEVEL = "DEBUG";

const LEVELS = {
  ERROR: 0,
  WARN: 1,
  INFO: 2,
  DEBUG: 3,
};
const currentLevel = LEVELS[LOG_LEVEL.toUpperCase()] ?? 2;

function shouldLog(level) {
  return LEVELS[level.toUpperCase()] <= currentLevel;
}

function scheduleLog(level, message, data = {}) {
  if (!shouldLog(level)) return;

  // Use process.nextTick + setImmediate to push to end of event loop
  process.nextTick(() => {
    setImmediate(() => {
      const entry = {
        timestamp: new Date().toISOString(),
        level: level.toUpperCase(),
        message,
        ...data,
      };
      // This is the only console.log in the entire module – runs after response sent
      console.log(JSON.stringify(entry));
    });
  });
}

function debug(message, data = {}) {
  scheduleLog("DEBUG", message, data);
}

function info(message, data = {}) {
  scheduleLog("INFO", message, data);
}

function warn(message, data = {}) {
  scheduleLog("WARN", message, data);
}

function error(message, err) {
  const isErrorObject = err instanceof Error;
  if (!isErrorObject) {
    scheduleLog("ERROR", message, { error: err });
    return;
  } else {
    scheduleLog("ERROR", message, { error: err.message, stack: err.stack });
  }
}

module.exports = {
  debug,
  info,
  warn,
  error,
};
