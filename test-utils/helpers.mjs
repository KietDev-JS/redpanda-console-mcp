// Shared test helpers: a fake fetch so the suite runs fully offline.

import { loadConfig } from '../src/config.mjs';
import { ConsoleClient } from '../src/console.mjs';
import { ConsoleService } from '../src/service.mjs';
import { createHandlers } from '../src/tools.mjs';

export const TEST_ENV = {
  CONSOLE_BASE_URL: 'https://console.example.com',
};

/**
 * Marker for a raw HTTP response, as opposed to a plain JSON body.
 *
 * Without this, a fixture like `{ status: 404, text: 'nope' }` is
 * indistinguishable from a JSON body that happens to have those keys, and the
 * fake would silently return it as a 200.
 *
 * @param {{ status?: number, text?: string, body?: unknown, chunks?: Uint8Array[] }} spec
 */
export function reply(spec) {
  return { __httpResponse: true, ...spec };
}

/** Build a Connect streaming envelope around a JSON payload. */
export function envelope(message, flags = 0) {
  const payload = new TextEncoder().encode(JSON.stringify(message));
  const frame = new Uint8Array(5 + payload.length);
  const view = new DataView(frame.buffer);
  view.setUint8(0, flags);
  view.setUint32(1, payload.length, false);
  frame.set(payload, 5);
  return frame;
}

/** A normal data frame carrying one message. */
export function dataFrame(data) {
  return envelope({ data });
}

/** The end-of-stream trailer, optionally carrying an error. */
export function endFrame(body = {}) {
  return envelope(body, 0b0000_0010);
}

/** Concatenate frames into a single byte array. */
export function concat(...parts) {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

/** Split a byte array into fixed-size chunks, to exercise reassembly. */
export function chunked(bytes, size) {
  const chunks = [];
  for (let i = 0; i < bytes.length; i += size) chunks.push(bytes.subarray(i, i + size));
  return chunks;
}

/** A minimal ReadableStream-like object over an array of chunks. */
function bodyFrom(chunks) {
  let i = 0;
  let released = false;
  return {
    getReader() {
      return {
        async read() {
          if (i >= chunks.length) return { done: true, value: undefined };
          return { done: false, value: chunks[i++] };
        },
        releaseLock() {
          released = true;
        },
        get released() {
          return released;
        },
      };
    },
  };
}

/**
 * Build a client whose fetch returns canned responses and records calls.
 *
 * @param {object|((url: URL, init: object) => object)} responder
 *   A plain JSON body, a `reply(...)` response, an Error to throw, or a
 *   function returning any of those.
 */
export function makeClient(responder, env = TEST_ENV) {
  const calls = [];
  const fetchImpl = async (url, init) => {
    calls.push({
      url: new URL(url),
      method: init.method,
      headers: init.headers,
      body: init.body,
      signal: init.signal,
    });
    const raw = typeof responder === 'function' ? await responder(new URL(url), init) : responder;
    if (raw instanceof Error) throw raw;
    const r = raw && raw.__httpResponse ? raw : { body: raw };
    const status = r.status ?? 200;
    const text = r.text !== undefined ? r.text : JSON.stringify(r.body ?? {});

    return {
      ok: status >= 200 && status < 300,
      status,
      headers: { get: (k) => (r.headers || {})[String(k).toLowerCase()] ?? null },
      text: async () => (r.chunks ? Buffer.concat(r.chunks.map(Buffer.from)).toString() : text),
      body: r.chunks ? bodyFrom(r.chunks) : undefined,
    };
  };

  const client = new ConsoleClient(loadConfig(env), { fetch: fetchImpl });
  const service = new ConsoleService(client);
  return { client, service, calls, handlers: createHandlers(service) };
}

/** A topic list fixture of the requested size. */
export function topicList(n, prefix = 'topic') {
  return {
    topics: Array.from({ length: n }, (_, i) => ({
      name: `${prefix}-${i}`,
      partition_count: (i % 4) + 1,
      replication_factor: 3,
    })),
  };
}

/** Decode the request body a call sent, as JSON. */
export function sentJson(call) {
  return JSON.parse(Buffer.from(call.body).toString());
}

/** Decode a Connect-framed request body, skipping the 5-byte envelope. */
export function sentEnvelope(call) {
  const bytes = Buffer.from(call.body);
  return JSON.parse(bytes.subarray(5).toString());
}
