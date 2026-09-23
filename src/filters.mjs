// Construction of server-side JavaScript filters for `ListMessages`.
//
// Redpanda Console evaluates a user-supplied JavaScript snippet against every
// consumed message inside a sandboxed goja VM. The snippet is a *function body*
// (the Console wraps it in `function () { ... }`) and receives these globals:
//
//  key / value
//      The deserialised payloads. JSON messages arrive as live JS objects;
//      text messages arrive as strings.
//  offset / partitionID / timestamp / headers
//      Message metadata.
//
// Because the snippet is code rather than data, every interpolated value is
// serialised with JSON.stringify, which produces a valid JavaScript string
// literal with quotes, backslashes and control characters escaped. This keeps
// search terms inert no matter what characters they contain.

/** Guard against pathological inputs before they reach the remote VM. */
export const MAX_FILTER_LENGTH = 4096;

// Renders any payload to a searchable string. `undefined`/`null` collapse to
// an empty string so a filter never throws inside the sandbox; a throwing
// filter aborts the entire consume request.
const STRINGIFY = `function __rp_str(v) {
  if (v === null || v === undefined) return '';
  if (typeof v === 'string') return v;
  try { return JSON.stringify(v); } catch (e) { return String(v); }
}`;

/**
 * Build a filter body matching messages whose key or value contains `text`.
 *
 * Both the key and the value are searched. The needle is embedded as a JSON
 * string literal, so quotes and backslashes cannot escape into code.
 *
 * @param {string} text
 * @param {{ caseSensitive?: boolean }} [options]
 */
export function buildContainsFilter(text, { caseSensitive = true } = {}) {
  if (!text) {
    throw new RangeError('Search text must not be empty.');
  }
  if (text.length > MAX_FILTER_LENGTH) {
    throw new RangeError(
      `Search text is too long (${text.length} chars, maximum ${MAX_FILTER_LENGTH}).`,
    );
  }

  // JSON.stringify escapes quotes, backslashes and control characters. It does
  // not escape "</script" or U+2028/U+2029, which are harmless inside goja but
  // are escaped anyway so the snippet is safe to embed or log verbatim.
  const needle = JSON.stringify(caseSensitive ? text : text.toLowerCase())
    .replace(/\u2028/g, '\\u2028')
    .replace(/\u2029/g, '\\u2029')
    .replace(/</g, '\\u003c');
  const fold = caseSensitive ? '' : '.toLowerCase()';

  return (
    `${STRINGIFY}\n` +
    `var __rp_needle = ${needle};\n` +
    `var __rp_hay = (__rp_str(key) + '\\n' + __rp_str(value))${fold};\n` +
    'return __rp_hay.indexOf(__rp_needle) !== -1;'
  );
}

/** Base64-encode filter source for the `filterInterpreterCode` field. */
export function encodeFilter(code) {
  return Buffer.from(code, 'utf-8').toString('base64');
}
