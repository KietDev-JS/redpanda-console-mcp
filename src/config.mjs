// Configuration, read once from the environment.
//
// There is deliberately no default base URL: pointing a Kafka client at a
// guessed host is worse than refusing to start.

/** The Console aborts the response stream above this many messages. */
export const MAX_RESULTS_LIMIT = 500;

/** Hard caps and defaults. */
export const LIMITS = {
  /** Max topics returned by list_topics. */
  MAX_PAGE_SIZE: 1000,
  /** Default topic page size. */
  DEFAULT_PAGE_SIZE: 100,
  /** Default message count for fetch_latest / fetch_by_offset. */
  DEFAULT_MAX_RESULTS: 10,
  /** Default message count for fetch_by_time / search_messages. */
  DEFAULT_SEARCH_RESULTS: 20,
  /** Upstream request timeout, milliseconds. */
  TIMEOUT_MS: 60_000,
  /** Longest timeout a Node timer can represent, in seconds. */
  MAX_TIMEOUT_SECONDS: 2_147_483,
  /** Default retries for transient upstream failures. */
  DEFAULT_MAX_RETRIES: 2,
  /** Upper bound on CONSOLE_MAX_RETRIES. */
  MAX_RETRIES: 5,
};

const TRUTHY = new Set(['1', 'true', 'yes', 'on']);
const FALSY = new Set(['0', 'false', 'no', 'off']);

export class ConfigError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ConfigError';
  }
}

function envBool(source, name, fallback) {
  const raw = source[name];
  if (raw === undefined || String(raw).trim() === '') return fallback;
  const value = String(raw).trim().toLowerCase();
  if (TRUTHY.has(value)) return true;
  if (FALSY.has(value)) return false;
  const allowed = [...TRUTHY, ...FALSY].sort().join(', ');
  throw new ConfigError(`${name} must be one of ${allowed} (got ${JSON.stringify(raw)}).`);
}

function envPositiveNumber(source, name, fallback) {
  const raw = source[name];
  if (raw === undefined || String(raw).trim() === '') return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n)) {
    throw new ConfigError(`${name} must be a number (got ${JSON.stringify(raw)}).`);
  }
  if (n <= 0) {
    throw new ConfigError(`${name} must be positive (got ${n}).`);
  }
  return n;
}

function envTimeoutSeconds(source, name, fallback) {
  const seconds = envPositiveNumber(source, name, fallback);
  // A larger delay overflows Node's timer and fires almost immediately.
  if (seconds > LIMITS.MAX_TIMEOUT_SECONDS) {
    throw new ConfigError(`${name} must not exceed ${LIMITS.MAX_TIMEOUT_SECONDS} (got ${seconds}).`);
  }
  return seconds;
}

function envRetryCount(source, name, fallback) {
  const raw = source[name];
  if (raw === undefined || String(raw).trim() === '') return fallback;
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 0 || n > LIMITS.MAX_RETRIES) {
    throw new ConfigError(
      `${name} must be an integer from 0 to ${LIMITS.MAX_RETRIES} (got ${JSON.stringify(raw)}).`,
    );
  }
  return n;
}

/**
 * Build config from an environment mapping.
 *
 * Takes the env as an argument rather than reading `process.env` directly so
 * tests can exercise it without mutating global state.
 *
 * @param {Record<string, string | undefined>} env
 */
export function loadConfig(env = process.env) {
  const rawBase = (env.CONSOLE_BASE_URL || '').trim().replace(/\/+$/, '');
  if (!rawBase) {
    throw new ConfigError(
      'CONSOLE_BASE_URL is not set. Point it at your Redpanda Console, ' +
        'for example: CONSOLE_BASE_URL=https://console.example.com',
    );
  }
  if (!/^https?:\/\//i.test(rawBase)) {
    throw new ConfigError(
      `CONSOLE_BASE_URL must start with http:// or https:// (got ${JSON.stringify(scrubSecrets(rawBase))})`,
    );
  }

  let parsed;
  try {
    parsed = new URL(rawBase);
  } catch {
    throw new ConfigError(`CONSOLE_BASE_URL is not a valid URL: ${scrubSecrets(rawBase)}`);
  }
  if (parsed.search || parsed.hash) {
    throw new ConfigError('CONSOLE_BASE_URL must not contain a query string or fragment');
  }

  // Keep any mount path (reverse proxies often serve Console under a subpath)
  // but drop the trailing slash so joining with "/v1/..." is exact.
  const basePath = parsed.pathname.replace(/\/+$/, '');

  return {
    origin: parsed.origin,
    basePath,
    baseUrl: `${parsed.origin}${basePath}`,
    apiKey: (env.CONSOLE_API_KEY || '').trim(),
    timeoutMs: Math.round(envTimeoutSeconds(env, 'CONSOLE_TIMEOUT', LIMITS.TIMEOUT_MS / 1000) * 1000),
    maxRetries: envRetryCount(env, 'CONSOLE_MAX_RETRIES', LIMITS.DEFAULT_MAX_RETRIES),
    verifyTls: envBool(env, 'CONSOLE_VERIFY_TLS', true),
    // URL.origin drops user:password@, so such credentials are never sent.
    urlCredentialsIgnored: Boolean(parsed.username || parsed.password),
  };
}

/** Redact the credential so config can be logged safely. */
export function redact(config) {
  return { ...config, apiKey: config.apiKey ? '<redacted>' : '' };
}

const REDACTED = '<redacted>';

/**
 * Remove credentials from text that is about to leave the process.
 *
 * Upstream error bodies and proxy pages are untrusted and can echo request
 * headers back. This is for diagnostics only: Kafka message content is the
 * data the caller asked for and is never rewritten.
 *
 * @param {string} text
 * @param {string[]} [secrets] literal values to remove, such as the API key
 */
export function scrubSecrets(text, secrets = []) {
  let out = String(text ?? '');
  for (const secret of secrets) {
    if (secret) out = out.split(secret).join(REDACTED);
  }
  return out
    .replace(/\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+/gi, `$1 ${REDACTED}`)
    .replace(/\/\/[^/?#\s]*@/g, `//${REDACTED}@`)
    .replace(
      /\b(api[_-]?key|access[_-]?token|refresh[_-]?token|token|secret|password|passwd)(["']?\s*[=:]\s*["']?)[^\s"'&,;}]+/gi,
      `$1$2${REDACTED}`,
    );
}
