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

// Connect streaming envelope: 1 flag byte + uint32 big-endian payload length.
const ENVELOPE_LEN = 5;
const FLAG_END_STREAM = 0b0000_0010;
const FLAG_COMPRESSED = 0b0000_0001;

export class ConsoleError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ConsoleError';
  }
}

/** Non-2xx HTTP response from the Console. */
export class ConsoleHTTPError extends ConsoleError {
  constructor(statusCode, url, body) {
    const detail = extractJsonError(body) || String(body ?? '').trim().slice(0, 500) || '(empty response)';
    super(`HTTP ${statusCode} from ${url}: ${detail}`);
    this.name = 'ConsoleHTTPError';
    this.statusCode = statusCode;
    this.url = url;
    this.body = body;
  }
}

/** Application-level error reported inside a Connect response. */
export class ConsoleRPCError extends ConsoleError {
  constructor(code, message) {
    super(code ? `${code}: ${message}` : message);
    this.name = 'ConsoleRPCError';
    this.code = code;
    this.rpcMessage = message;
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
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) yield toBytes(value);
    }
  } finally {
    // Releasing lets the connection be torn down when a caller breaks early.
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
   * @param {{ fetch?: typeof globalThis.fetch }} [deps] injectable for tests
   */
  constructor(config, deps = {}) {
    this.config = config;
    this.fetchImpl = deps.fetch ?? globalThis.fetch;
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

  async send(url, init) {
    try {
      return await this.fetchImpl(url, {
        ...init,
        signal: AbortSignal.timeout(this.config.timeoutMs),
      });
    } catch (e) {
      if (e?.name === 'TimeoutError' || e?.name === 'AbortError') {
        throw new ConsoleError(
          `request to Redpanda Console timed out after ${this.config.timeoutMs}ms`,
        );
      }
      throw new ConsoleError(
        `cannot reach Redpanda Console at ${this.config.baseUrl}: ${e?.message || e}`,
      );
    }
  }

  // -- REST -----------------------------------------------------------------

  /** GET a dataplane REST endpoint and decode the JSON body. */
  async getJson(path, params) {
    const url = this.url(path, params);
    const res = await this.send(url, { method: 'GET', headers: this.headers });
    const text = await res.text();
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
  async unaryRpc(path, message) {
    const url = this.url(path);
    const res = await this.send(url, {
      method: 'POST',
      headers: { ...this.headers, 'Content-Type': 'application/json' },
      body: JSON.stringify(message || {}),
    });
    const text = await res.text();
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
   * trailer is raised as ConsoleRPCError.
   *
   * @returns {AsyncGenerator<Record<string, unknown>>}
   */
  async *streamRpc(path, message) {
    const url = this.url(path);
    const res = await this.send(url, {
      method: 'POST',
      headers: { ...this.headers, 'Content-Type': 'application/connect+json' },
      body: encodeEnvelope(message),
      duplex: 'half',
    });

    if (res.status >= 400) {
      throw new ConsoleHTTPError(res.status, String(url), await res.text());
    }

    for await (const { flags, payload } of iterEnvelopes(streamBytes(res.body))) {
      if (flags & FLAG_COMPRESSED) {
        throw new ConsoleProtocolError(
          'Console returned a compressed Connect frame, which is not supported. ' +
            'Ensure no proxy negotiates RPC-level compression.',
        );
      }
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
