import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { loadConfig, redact, ConfigError, LIMITS, MAX_RESULTS_LIMIT } from '../src/config.mjs';

describe('loadConfig', () => {
  test('requires CONSOLE_BASE_URL', () => {
    assert.throws(() => loadConfig({}), ConfigError);
    assert.throws(() => loadConfig({ CONSOLE_BASE_URL: '   ' }), /CONSOLE_BASE_URL is not set/);
  });

  test('requires an http(s) scheme', () => {
    assert.throws(
      () => loadConfig({ CONSOLE_BASE_URL: 'console.example.com' }),
      /must start with http/,
    );
    assert.throws(
      () => loadConfig({ CONSOLE_BASE_URL: 'ftp://console.example.com' }),
      /must start with http/,
    );
    assert.throws(
      () => loadConfig({ CONSOLE_BASE_URL: 'file:///etc/passwd' }),
      /must start with http/,
    );
  });

  test('accepts http and https', () => {
    assert.equal(loadConfig({ CONSOLE_BASE_URL: 'http://localhost:8080' }).origin, 'http://localhost:8080');
    assert.equal(loadConfig({ CONSOLE_BASE_URL: 'https://c.example.com' }).origin, 'https://c.example.com');
  });

  test('strips trailing slashes', () => {
    const c = loadConfig({ CONSOLE_BASE_URL: 'https://c.example.com///' });
    assert.equal(c.baseUrl, 'https://c.example.com');
    assert.equal(c.basePath, '');
  });

  test('preserves a reverse-proxy subpath', () => {
    const c = loadConfig({ CONSOLE_BASE_URL: 'https://proxy.example.com/redpanda/' });
    assert.equal(c.basePath, '/redpanda');
    assert.equal(c.baseUrl, 'https://proxy.example.com/redpanda');
  });

  test('rejects query strings and fragments', () => {
    assert.throws(() => loadConfig({ CONSOLE_BASE_URL: 'https://c.example.com?a=1' }), /query string/);
    assert.throws(() => loadConfig({ CONSOLE_BASE_URL: 'https://c.example.com#x' }), /query string/);
  });

  test('rejects a malformed URL', () => {
    assert.throws(() => loadConfig({ CONSOLE_BASE_URL: 'https://' }), ConfigError);
  });

  test('api key defaults to empty and is trimmed', () => {
    assert.equal(loadConfig({ CONSOLE_BASE_URL: 'https://c.example.com' }).apiKey, '');
    assert.equal(
      loadConfig({ CONSOLE_BASE_URL: 'https://c.example.com', CONSOLE_API_KEY: '  k  ' }).apiKey,
      'k',
    );
  });

  test('timeout is seconds in, milliseconds out', () => {
    const base = { CONSOLE_BASE_URL: 'https://c.example.com' };
    assert.equal(loadConfig(base).timeoutMs, 60_000);
    assert.equal(loadConfig({ ...base, CONSOLE_TIMEOUT: '5' }).timeoutMs, 5_000);
    assert.equal(loadConfig({ ...base, CONSOLE_TIMEOUT: '0.5' }).timeoutMs, 500);
    assert.equal(loadConfig({ ...base, CONSOLE_TIMEOUT: '' }).timeoutMs, 60_000);
  });

  test('rejects non-positive and non-numeric timeouts', () => {
    const base = { CONSOLE_BASE_URL: 'https://c.example.com' };
    assert.throws(() => loadConfig({ ...base, CONSOLE_TIMEOUT: '0' }), /must be positive/);
    assert.throws(() => loadConfig({ ...base, CONSOLE_TIMEOUT: '-3' }), /must be positive/);
    assert.throws(() => loadConfig({ ...base, CONSOLE_TIMEOUT: 'soon' }), /must be a number/);
  });

  test('verify_tls parses truthy and falsy spellings', () => {
    const base = { CONSOLE_BASE_URL: 'https://c.example.com' };
    assert.equal(loadConfig(base).verifyTls, true);
    for (const v of ['0', 'false', 'FALSE', 'no', 'off', ' Off ']) {
      assert.equal(loadConfig({ ...base, CONSOLE_VERIFY_TLS: v }).verifyTls, false, v);
    }
    for (const v of ['1', 'true', 'YES', 'on']) {
      assert.equal(loadConfig({ ...base, CONSOLE_VERIFY_TLS: v }).verifyTls, true, v);
    }
    assert.equal(loadConfig({ ...base, CONSOLE_VERIFY_TLS: '' }).verifyTls, true);
  });

  test('rejects an unparseable boolean rather than guessing', () => {
    assert.throws(
      () => loadConfig({ CONSOLE_BASE_URL: 'https://c.example.com', CONSOLE_VERIFY_TLS: 'maybe' }),
      /must be one of/,
    );
  });
});

describe('redact', () => {
  test('hides a present api key', () => {
    const c = loadConfig({ CONSOLE_BASE_URL: 'https://c.example.com', CONSOLE_API_KEY: 'secret' });
    const r = redact(c);
    assert.equal(r.apiKey, '<redacted>');
    assert.equal(JSON.stringify(r).includes('secret'), false);
  });

  test('leaves an absent key empty', () => {
    assert.equal(redact(loadConfig({ CONSOLE_BASE_URL: 'https://c.example.com' })).apiKey, '');
  });
});

describe('limits', () => {
  test('the documented Console cap is 500', () => {
    assert.equal(MAX_RESULTS_LIMIT, 500);
  });

  test('defaults are sane', () => {
    assert.equal(LIMITS.DEFAULT_PAGE_SIZE, 100);
    assert.equal(LIMITS.DEFAULT_MAX_RESULTS, 10);
    assert.equal(LIMITS.DEFAULT_SEARCH_RESULTS, 20);
  });
});
