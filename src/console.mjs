// Low-level client for the Redpanda Console HTTP API.
//
// The Console exposes two distinct HTTP surfaces and this module speaks both:
//
//  * Dataplane REST (`/v1/...`) - ordinary JSON request/response.
//  * Connect RPC (`/<package>.<Service>/<Method>`) - Buf's Connect protocol.
//    Unary methods use plain JSON. Server-streaming methods use the Connect
//    *streaming* framing: a 5-byte prefix (1 flag byte + 4-byte big-endian
//    length) in front of each JSON payload.
//
// Everything here is transport-level; semantic shaping lives in `models.mjs`.

import { scrubSecrets } from './config.mjs';

// Connect streaming envelope: 1 flag byte + uint32 big-endian payload length.
const ENVELOPE_LEN = 5;
const FLAG_END_STREAM = 0b0000_0010;
const FLAG_COMPRESSED = 0b0000_0001;

/**
 * Largest Connect frame accepted. The length prefix is untrusted: without a
 * cap, a non-Connect body makes the reader buffer until the timeout.
 */
export const MAX_FRAME_BYTES = 64 * 1024 * 1024;

/** Statuses that usually clear by themselves. Every call here only reads. */
const RETRYABLE_STATUS = new Set([429, 502, 503, 504]);
const RETRY_BASE_MS = 250;
const RETRY_MAX_DELAY_MS = 5_000;

const NARROW_HINT =
  'Narrow the read with partition_id, start_offset or start_timestamp_ms, ' +
  'lower max_results, or raise CONSOLE_TIMEOUT.';

const HTTP_HINTS = {
  401: 'Check CONSOLE_API_KEY. SSO-only Consoles cannot be used with this server.',
  403: 'The credential lacks permission for this resource; use a Console user with read access.',
  404: 'Check the topic name with list_topics, and that CONSOLE_BASE_URL includes any reverse-proxy subpath.',
  429: 'The Console is rate limiting requests; wait before retrying.',
  502: 'The Console or its proxy is unavailable; retry shortly.',
  503: 'The Console or its proxy is unavailable; retry shortly.',
  504: 'The Console or its proxy timed out; retry shortly or narrow the request.',
};

const RPC_HINTS = {
  unauthenticated: HTTP_HINTS[401],
  permission_denied: HTTP_HINTS[403],
  not_found: 'Check the topic name with list_topics.',
};

function describeBody(body) {
  const text = String(body ?? '').trim();
  if (/^<(!doctype|html)/i.test(text)) return '(HTML page, likely a login page or proxy error)';
  return text.slice(0, 500) || '(empty response)';
}

export class ConsoleError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ConsoleError';
  }
}

/** Non-2xx HTTP response from the Console. */
export class ConsoleHTTPError extends ConsoleError {
  constructor(statusCode, url, body) {
    const detail = extractJsonError(body) || describeBody(body);
    const hint = HTTP_HINTS[statusCode];
    super(`HTTP ${statusCode} from ${url}: ${detail}${hint ? ` ${hint}` : ''}`);
    this.name = 'ConsoleHTTPError';
    this.statusCode = statusCode;
    this.url = url;
    this.body = body;
  }
}

/** Application-level error reported inside a Connect response. */
export class ConsoleRPCError extends ConsoleError {
  constructor(code, message) {
    // The code comes from the server, so it must not resolve inherited keys.
    const hint = Object.hasOwn(RPC_HINTS, String(code)) ? RPC_HINTS[code] : '';
    super(`${code ? `${code}: ${message}` : message}${hint ? ` ${hint}` : ''}`);
    this.name = 'ConsoleRPCError';
    this.code = code;
    this.rpcMessage = message;
  }
}

/**
 * The connection failed or timed out after the response had started.
 *
 * Raised by the stream reader so the service layer can distinguish a stream
 * that produced nothing from one that delivered some messages and then
 * stopped. The latter is the normal outcome of tailing a quiet topic until
 * the timeout.
 */
export class ConsoleStreamInterrupted extends ConsoleError {
  constructor(message, { timeout = false } = {}) {
    super(message);
    this.name = 'ConsoleStreamInterrupted';
    this.timeout = timeout;
  }
}

/** The response did not conform to the expected wire format. */
export class ConsoleProtocolError extends ConsoleError {
  constructor(message) {
    super(message);
    this.name = 'ConsoleProtocolError';
  }
}

/** Pull a human-readable message out of a Console JSON error body. */
function extractJsonError(body) {
  let payload;
  try {
    payload = JSON.parse(body);
  } catch {
    return null;
  }
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) return null;
  if (payload.error !== null && typeof payload.error === 'object' && !Array.isArray(payload.error)) {
    payload = payload.error;
  }
  const { code, message } = payload;
  if (message && code) return `${code}: ${message}`;
  return message || null;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8');

/** Wrap a JSON message in a Connect streaming envelope. */
export function encodeEnvelope(message) {
  const payload = encoder.encode(JSON.stringify(message));
  const frame = new Uint8Array(ENVELOPE_LEN + payload.length);
  const view = new DataView(frame.buffer);
  view.setUint8(0, 0);
  view.setUint32(1, payload.length, false); // big-endian
  frame.set(payload, ENVELOPE_LEN);
  return frame;
}

/**
 * Reassemble Connect envelopes from an arbitrarily chunked byte stream.
 *
 * Envelopes routinely straddle TCP chunk boundaries, so a buffer is carried
 * across chunks and only complete frames are emitted.
 *
 * @param {AsyncIterable<Uint8Array>} chunks
 * @returns {AsyncGenerator<{ flags: number, payload: Uint8Array }>}
 */
export async function* iterEnvelopes(chunks) {
  let buffer = new Uint8Array(0);
  for await (const chunk of chunks) {
    const merged = new Uint8Array(buffer.length + chunk.length);
    merged.set(buffer, 0);
    merged.set(chunk, buffer.length);
    buffer = merged;

    while (buffer.length >= ENVELOPE_LEN) {
      const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);
      const flags = view.getUint8(0);
      const length = view.getUint32(1, false);
      if (length > MAX_FRAME_BYTES) {
        throw new ConsoleProtocolError(
          `Connect frame of ${length} bytes exceeds the ${MAX_FRAME_BYTES} byte limit. The response ` +
            'is probably not a Connect stream; check that CONSOLE_BASE_URL points at a Redpanda ' +
            'Console API root and not a login page or reverse proxy.',
        );
      }
      const end = ENVELOPE_LEN + length;
      if (buffer.length < end) break;
      yield { flags, payload: buffer.subarray(ENVELOPE_LEN, end) };
      buffer = buffer.subarray(end);
    }
  }
  if (buffer.length > 0) {
    throw new ConsoleProtocolError(
      `Connect stream ended mid-frame with ${buffer.length} trailing bytes. ` +
        'The server may have terminated the response early.',
    );
  }
}

/** Adapt a web ReadableStream (or async iterable) to an async iterable of bytes. */
async function* streamBytes(body) {
  if (!body) return;
  if (typeof body[Symbol.asyncIterator] === 'function') {
    for await (const chunk of body) yield toBytes(chunk);
    return;
  }
  const reader = body.getReader();
  let finished = false;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        finished = true;
        break;
      }
      if (value) yield toBytes(value);
    }
  } finally {
    // When the caller stops early the body is still open. Releasing the lock
    // alone leaves the socket held until garbage collection; cancelling tells
    // fetch to abort the transfer and free the connection now. That matters
    // for live tails, which otherwise keep streaming until the timeout.
    if (!finished) {
      try {
        await reader.cancel();
      } catch {
        /* stream already errored or closed */
      }
    }
    try {
      reader.releaseLock();
    } catch {
      /* already released */
    }
  }
}

function toBytes(chunk) {
  if (chunk instanceof Uint8Array) return chunk;
  if (typeof chunk === 'string') return encoder.encode(chunk);
  return new Uint8Array(chunk);
}

/**
 * Client for the Redpanda Console API.
 */
export class ConsoleClient {
  /**
   * @param {ReturnType<import('./config.mjs').loadConfig>} config
   * @param {{ fetch?: typeof globalThis.fetch, sleep?: typeof wait }} [deps] injectable for tests
   */
  constructor(config, deps = {}) {
    this.config = config;
    this.fetchImpl = deps.fetch ?? globalThis.fetch;
    this.sleep = deps.sleep ?? wait;
    this.headers = {
      Accept: 'application/json',
      'User-Agent': 'redpanda-console-mcp',
      ...(config.apiKey ? { Authorization: `Bearer ${config.apiKey}` } : {}),
    };
  }

  /** Build an absolute URL, preserving any reverse-proxy mount path. */
  url(path, params) {
    const u = new URL(`${this.config.baseUrl}${path}`);
    for (const [k, v] of Object.entries(params || {})) {
      if (v !== undefined && v !== null && v !== '') u.searchParams.set(k, String(v));
    }
    return u;
  }

  /** Remove credentials from diagnostic text before it leaves the process. */
  scrub(text) {
    return scrubSecrets(text, [this.config.apiKey]);
  }

  /**
   * Send a request, retrying transient failures.
   *
   * One deadline covers every attempt and the body download, so retries never
   * extend the configured timeout. Only the initial response is retried: once
   * a stream has started, its partial results are handled by the caller.
   *
   * @param {URL} url
   * @param {RequestInit} init
   * @param {{ signal?: AbortSignal }} [options] `signal` cancels the request
   */
  async send(url, init, { signal: caller } = {}) {
    const signal = requestSignal(this.config.timeoutMs, caller);
    const deadline = Date.now() + this.config.timeoutMs;
    const maxRetries = this.config.maxRetries ?? 0;

    for (let attempt = 0; ; attempt += 1) {
      let res;
      let failure;
      try {
        res = await this.fetchImpl(url, { ...init, signal });
      } catch (e) {
        if (caller?.aborted || signal.aborted || isTimeout(e)) throw this.sendFailure(e, caller);
        failure = e;
      }
      if (res && !RETRYABLE_STATUS.has(res.status)) return res;

      const delay = retryDelay(attempt, res);
      const canRetry =
        attempt < maxRetries && delay <= RETRY_MAX_DELAY_MS && Date.now() + delay < deadline;
      if (!canRetry) {
        if (res) return res;
        throw this.sendFailure(failure, caller);
      }

      await discardBody(res);
      try {
        await this.sleep(delay, signal);
      } catch (e) {
        throw this.sendFailure(e, caller);
      }
    }
  }

  sendFailure(e, caller) {
    if (caller?.aborted) return new ConsoleError('request to Redpanda Console was cancelled');
    if (isTimeout(e)) {
      return new ConsoleError(
        `request to Redpanda Console timed out after ${this.config.timeoutMs}ms. ${NARROW_HINT}`,
      );
    }
    return new ConsoleError(
      `cannot reach Redpanda Console at ${this.config.baseUrl}: ${e?.message || e}. ` +
        'Check CONSOLE_BASE_URL and that this machine can reach it (VPN, proxy, DNS).',
    );
  }

  /**
   * Read a whole response body as text.
   *
   * The timeout signal is still armed while the body downloads, so this can
   * fail too; map it to ConsoleError instead of letting a DOMException leak
   * out as an "Unexpected error".
   */
  async readText(res) {
    try {
      return await res.text();
    } catch (e) {
      throw this.interrupted(e);
    }
  }

  interrupted(e) {
    if (e instanceof ConsoleError) return e;
    if (isTimeout(e)) {
      return new ConsoleStreamInterrupted(
        `Redpanda Console response timed out after ${this.config.timeoutMs}ms. ${NARROW_HINT}`,
        { timeout: true },
      );
    }
    return new ConsoleStreamInterrupted(
      `connection to Redpanda Console was interrupted while reading the response: ${e?.message || e}`,
    );
  }

  // -- REST -----------------------------------------------------------------

  /** GET a dataplane REST endpoint and decode the JSON body. */
  async getJson(path, params, options) {
    const url = this.url(path, params);
    const res = await this.send(url, { method: 'GET', headers: this.headers }, options);
    const text = await this.readText(res);
    if (res.status >= 400) throw new ConsoleHTTPError(res.status, String(url), text);
    return decodeJson(text, url, res);
  }

  // -- Connect RPC ----------------------------------------------------------

  /**
   * Invoke a unary Connect method.
   *
   * Unary Connect uses plain `application/json` with no envelope framing;
   * sending the streaming content type yields HTTP 415.
   */
  async unaryRpc(path, message, options) {
    const url = this.url(path);
    const res = await this.send(
      url,
      {
        method: 'POST',
        headers: { ...this.headers, 'Content-Type': 'application/json' },
        body: JSON.stringify(message || {}),
      },
      options,
    );
    const text = await this.readText(res);
    if (res.status >= 400) throw new ConsoleHTTPError(res.status, String(url), text);
    const payload = decodeJson(text, url, res);
    if (payload !== null && typeof payload === 'object' && !Array.isArray(payload)) {
      const err = payload.error;
      if (err !== null && typeof err === 'object' && !Array.isArray(err)) {
        throw new ConsoleRPCError(err.code ?? 'unknown', err.message ?? '');
      }
    }
    return payload;
  }

  /**
   * Invoke a server-streaming Connect method, yielding decoded frames.
   *
   * Frames are yielded as they arrive so callers can stop early without
   * buffering the whole response. An error carried in the end-of-stream
   * trailer is raised as ConsoleRPCError. A stream that delivered frames but
   * closed without the trailer was cut short, and raises
   * ConsoleStreamInterrupted so it cannot pass for a complete result.
   *
   * @returns {AsyncGenerator<Record<string, unknown>>}
   */
  async *streamRpc(path, message, options) {
    const url = this.url(path);
    const res = await this.send(
      url,
      {
        method: 'POST',
        headers: { ...this.headers, 'Content-Type': 'application/connect+json' },
        body: encodeEnvelope(message),
        duplex: 'half',
      },
      options,
    );

    if (res.status >= 400) {
      throw new ConsoleHTTPError(res.status, String(url), await this.readText(res));
    }

    const contentType = res.headers?.get?.('content-type');
    if (contentType && /^(text\/|application\/json)/i.test(contentType)) {
      await discardBody(res);
      throw new ConsoleProtocolError(
        `Expected a Connect stream from ${url} but got ${contentType}. Check that ` +
          'CONSOLE_BASE_URL points at a Redpanda Console API root and not a login page or reverse proxy.',
      );
    }

    let sawFrame = false;
    let sawEnd = false;
    for await (const { flags, payload } of iterEnvelopes(this.guardedBytes(res.body))) {
      sawFrame = true;
      if (flags & FLAG_COMPRESSED) {
        throw new ConsoleProtocolError(
          'Console returned a compressed Connect frame, which is not supported. ' +
            'Ensure no proxy negotiates RPC-level compression.',
        );
      }
      if (flags & FLAG_END_STREAM) sawEnd = true;
      if (payload.length === 0) continue;

      let frame;
      const text = decoder.decode(payload);
      try {
        frame = JSON.parse(text);
      } catch (e) {
        throw new ConsoleProtocolError(`Malformed JSON in Connect frame: ${e.message}`);
      }
      if (frame === null || typeof frame !== 'object' || Array.isArray(frame)) continue;

      if (flags & FLAG_END_STREAM) {
        const err = frame.error;
        if (err !== null && typeof err === 'object' && !Array.isArray(err)) {
          throw new ConsoleRPCError(err.code ?? 'unknown', err.message ?? '');
        }
        continue;
      }
      yield frame;
    }

    if (sawFrame && !sawEnd) {
      throw new ConsoleStreamInterrupted(
        'Redpanda Console closed the stream before it finished, so the result may be incomplete.',
      );
    }
  }

  /** streamBytes, with transport failures mapped to ConsoleStreamInterrupted. */
  async *guardedBytes(body) {
    try {
      yield* streamBytes(body);
    } catch (e) {
      throw this.interrupted(e);
    }
  }
}

function isTimeout(e) {
  return e?.name === 'TimeoutError' || e?.name === 'AbortError';
}

/**
 * Combine the request timeout with an optional caller cancellation.
 *
 * Written by hand because AbortSignal.any needs Node 20.3 and this package
 * supports 18.17.
 */
function requestSignal(timeoutMs, caller) {
  const timeout = AbortSignal.timeout(timeoutMs);
  if (!caller) return timeout;
  const controller = new AbortController();
  for (const source of [timeout, caller]) {
    if (source.aborted) controller.abort(source.reason);
    else source.addEventListener('abort', () => controller.abort(source.reason), { once: true });
  }
  return controller.signal;
}

/** Backoff for one attempt, or the server's Retry-After when it sent one. */
function retryDelay(attempt, res) {
  const backoff = RETRY_BASE_MS * 2 ** attempt;
  const header = res?.headers?.get?.('retry-after');
  if (!header) return backoff;
  const seconds = Number(header);
  const ms = Number.isFinite(seconds) ? seconds * 1000 : Date.parse(header) - Date.now();
  return Number.isNaN(ms) ? backoff : Math.max(ms, 0);
}

/** Sleep that ends early, by rejecting, when `signal` aborts. */
function wait(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason);
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

/** Release the connection held by a response that will not be read. */
async function discardBody(res) {
  try {
    await res?.body?.cancel?.();
  } catch {
    /* already closed */
  }
}

function decodeJson(text, url, res) {
  try {
    return JSON.parse(text);
  } catch {
    const contentType = res?.headers?.get?.('content-type') || 'unknown';
    const snippet = String(text ?? '').trim().slice(0, 200);
    throw new ConsoleProtocolError(
      `Expected JSON from ${url} but got ${contentType}: ${JSON.stringify(snippet)}. ` +
        'Check that CONSOLE_BASE_URL points at a Redpanda Console API root ' +
        'and not a login page or reverse proxy.',
    );
  }
}
