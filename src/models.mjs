// Normalisation of Redpanda Console payloads into plain JSON-friendly objects.
//
// The Console speaks proto3 JSON, which has two properties that bite naive
// consumers and which this module exists to absorb:
//
//  * Zero values are omitted. `partitionId: 0` and `offset: 0` simply do not
//    appear in the payload. Defaulting a missing field to null silently
//    mislabels every message on partition 0.
//  * 64-bit integers are strings. `offset` and `timestamp` arrive as "105825"
//    so that large values survive JavaScript's number range.

export { MAX_RESULTS_LIMIT } from './config.mjs';

const NULL_ENCODING = 'PAYLOAD_ENCODING_NULL';

// Canonical base64, optionally padded. Used to decide whether a string is
// base64 at all before trying to decode it.
const BASE64_RE = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

/**
 * Coerce a proto3 JSON scalar to a number, tolerating string encoding.
 * Returns `fallback` for anything that is not a clean integer.
 */
export function asInt(value, fallback = null) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'boolean') return fallback;
  if (typeof value === 'number') return Number.isFinite(value) ? value : fallback;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    // Number('') is 0 and Number(' 1 ') is 1, so require explicit integer text.
    if (!/^[+-]?\d+$/.test(trimmed)) return fallback;
    return Number(trimmed);
  }
  return fallback;
}

/**
 * Decode base64 to text. Returns `{ text, binary }`.
 *
 * Binary payloads that are not valid UTF-8 are returned as base64 rather than
 * being mangled by lossy replacement characters.
 */
export function b64ToText(raw) {
  if (!BASE64_RE.test(raw) || raw.length % 4 !== 0) {
    // Not base64 after all - the Console already gave us plain text.
    return { text: raw, binary: false };
  }

  let data;
  try {
    data = Buffer.from(raw, 'base64');
  } catch {
    return { text: raw, binary: false };
  }
  // Buffer.from is lenient; a mismatched round-trip means it was not base64.
  if (data.toString('base64').replace(/=+$/, '') !== raw.replace(/=+$/, '')) {
    return { text: raw, binary: false };
  }

  const text = new TextDecoder('utf-8', { fatal: false }).decode(data);
  // A replacement char that was not in the input means lossy decoding.
  if (text.includes('\uFFFD')) {
    return { text: data.toString('base64'), binary: true };
  }
  return { text, binary: false };
}

/**
 * Normalise a Console payload object into `{ text, encoding, sizeBytes, binary }`.
 *
 * `normalizedPayload` is the Console's deserialised view (e.g. Avro or
 * Protobuf rendered as JSON); `originalPayload` is the raw bytes. The
 * normalised form is preferred because it is what a human sees in the UI.
 */
export function decodePayload(payload) {
  if (!payload || typeof payload !== 'object') {
    return { text: null, encoding: null, sizeBytes: null, binary: false };
  }

  const encoding = payload.encoding ?? null;
  const result = {
    text: null,
    encoding,
    sizeBytes: asInt(payload.payloadSize),
    binary: false,
  };
  if (encoding === NULL_ENCODING) return result;

  const raw = payload.normalizedPayload || payload.originalPayload;
  if (typeof raw !== 'string' || !raw) return result;

  const { text, binary } = b64ToText(raw);
  result.text = text;
  result.binary = binary;
  return result;
}

/** Render a Kafka epoch-millisecond timestamp as an ISO-8601 UTC string. */
export function formatTimestamp(value) {
  const millis = asInt(value);
  if (millis === null) return null;
  const date = new Date(millis);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function decodeHeaders(rawHeaders) {
  if (!Array.isArray(rawHeaders)) return [];
  const headers = [];
  for (const header of rawHeaders) {
    if (!header || typeof header !== 'object' || Array.isArray(header)) continue;
    const rawValue = header.value;
    const decoded =
      typeof rawValue === 'string' && rawValue ? b64ToText(rawValue) : { text: null, binary: false };
    headers.push({ key: header.key ?? null, value: decoded.text, binary: decoded.binary });
  }
  return headers;
}

/**
 * Convert a raw ListMessages data frame into a stable, flat object.
 *
 * `partition` and `offset` default to 0 rather than null because proto3 omits
 * zero values -- a missing `partitionId` means partition 0, not "unknown".
 */
export function normalizeMessage(data) {
  const source = data && typeof data === 'object' ? data : {};
  const key = decodePayload(source.key);
  const value = decodePayload(source.value);
  return {
    partition: asInt(source.partitionId, 0),
    offset: asInt(source.offset, 0),
    timestamp: formatTimestamp(source.timestamp),
    timestamp_ms: asInt(source.timestamp),
    key: key.text,
    value: value.text,
    headers: decodeHeaders(source.headers),
    key_encoding: key.encoding,
    value_encoding: value.encoding,
    value_size_bytes: value.sizeBytes,
    compression: source.compression ?? null,
    value_is_binary: value.binary,
  };
}
