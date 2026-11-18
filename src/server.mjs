// MCP stdio server: JSON-RPC 2.0 framing over newline-delimited stdin/stdout.

import { readFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { TOOLS, createHandlers, ToolError } from './tools.mjs';
import { ConsoleError } from './console.mjs';
import { ValidationError } from './service.mjs';

/** Protocol revisions this server understands, newest first. */
export const SUPPORTED_PROTOCOLS = ['2025-11-25', '2025-06-18', '2025-03-26', '2024-11-05'];

// Read the version from package.json so serverInfo and --version cannot drift
// from the published package. npm always ships package.json.
const PKG = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));

export const SERVER_INFO = { name: PKG.name, version: PKG.version };

/** Max serialized bytes of a single tool result. */
export const MAX_OUT = 200_000;

/** Partitions listed in `truncated.resume_from` before the list is cut off. */
const MAX_RESUME_POINTS = 16;

const byteLength = (text) => Buffer.byteLength(text, 'utf8');

/** The lowest dropped offset per partition, ready for fetch_by_offset. */
function resumePoints(dropped) {
  const lowest = new Map();
  for (const m of dropped) {
    if (!Number.isInteger(m?.partition) || !Number.isInteger(m?.offset)) continue;
    const seen = lowest.get(m.partition);
    if (seen === undefined || m.offset < seen) lowest.set(m.partition, m.offset);
  }
  return [...lowest]
    .slice(0, MAX_RESUME_POINTS)
    .map(([partition_id, offset]) => ({ partition_id, offset }));
}

/** Describe what was shed from a list of `of` items. */
function truncationInfo(key, items, returned) {
  const info = { returned, of: items.length, reason: 'result size limit' };
  if (key === 'topics') {
    info.hint = 'Lower page_size and list again; next_page_token does not cover the topics shed here.';
    return info;
  }
  const resume = resumePoints(items.slice(returned));
  info.hint = resume.length
    ? 'Lower max_results, or continue with fetch_by_offset from resume_from.'
    : 'Lower max_results.';
  if (resume.length) info.resume_from = resume;
  return info;
}

/**
 * Cut one oversized message value down to fit, flagging that it was cut.
 *
 * Returns null when the rest of the message alone is already too large.
 */
function clipMessage(message, build, max) {
  if (typeof message?.value !== 'string') return null;
  const total = message.value.length;
  const shell = { ...message, value: '', value_truncated: true, value_total_chars: total };
  const room = max - byteLength(JSON.stringify(build([shell])));
  if (room <= 2) return null;

  // JSON escaping and multi-byte text make the encoded size larger than the
  // character count, so shrink until the encoded value fits.
  let keep = Math.min(total, room);
  while (keep > 0 && byteLength(JSON.stringify(message.value.slice(0, keep))) > room) {
    keep = Math.floor(keep * 0.8);
  }
  return { ...shell, value: message.value.slice(0, keep) };
}

/**
 * Serialize a result, shedding trailing items if it exceeds the byte cap.
 *
 * Hard-slicing the serialized string would emit unparseable JSON, so array
 * results lose whole elements and report the truncation instead. Each item is
 * measured once, because re-serializing the list after every removal takes
 * minutes on a page of large messages.
 */
export function serializeBounded(result, max = MAX_OUT) {
  const isList = Array.isArray(result);
  const key =
    !isList && result && typeof result === 'object'
      ? ['messages', 'topics'].find((k) => Array.isArray(result[k]))
      : undefined;
  const items = isList ? result : key ? result[key] : null;

  if (!items) {
    const text = JSON.stringify(result ?? null);
    if (byteLength(text) <= max) return text;
    return JSON.stringify({
      error: 'Result exceeded the size limit and could not be reduced. Lower max_results.',
    });
  }

  const listKey = key ?? 'messages';
  const build = (kept) => {
    const truncated = truncationInfo(listKey, items, kept.length);
    if (isList) return { truncated, messages: kept };
    const count = listKey === 'topics' && 'count' in result ? { count: kept.length } : {};
    return { ...result, ...count, [listKey]: kept, truncated };
  };

  // Stop measuring at the cap, so a huge page is never serialized whole.
  let fits = 0;
  let used = byteLength(JSON.stringify(isList ? [] : { ...result, [listKey]: [] }));
  for (const item of items) {
    used += byteLength(JSON.stringify(item) ?? 'null') + 1;
    if (used > max) break;
    fits += 1;
  }
  if (fits === items.length && used <= max) return JSON.stringify(result);

  // The truncation notice needs room too, so give up items until it fits.
  for (let n = fits; n >= 1; n -= 1) {
    const text = JSON.stringify(build(items.slice(0, n)));
    if (byteLength(text) <= max) return text;
  }

  const clipped = listKey === 'messages' ? clipMessage(items[0], build, max) : null;
  if (clipped) return JSON.stringify(build([clipped]));

  const empty = JSON.stringify(build([]));
  if (byteLength(empty) <= max) return empty;
  return JSON.stringify({
    error: 'Result exceeded the size limit and could not be reduced. Lower max_results.',
  });
}

const ALLOWED_ARGS = new Map(TOOLS.map((t) => [t.name, new Set(Object.keys(t.inputSchema.properties))]));

/**
 * Enforce the shape every tool schema declares: an object with no unknown keys.
 *
 * Schemas say `additionalProperties: false`, but clients are not required to
 * validate. Without this a misspelled argument (`partition`, `limit`) is
 * silently dropped and the call runs with defaults -- reading every partition
 * instead of one, say -- which looks like a wrong answer rather than a mistake
 * the model can fix.
 */
export function checkArguments(name, args) {
  if (args === undefined || args === null) return {};
  if (typeof args !== 'object' || Array.isArray(args)) {
    throw new ToolError(
      `arguments must be a JSON object (got ${Array.isArray(args) ? 'array' : typeof args}).`,
    );
  }
  const allowed = ALLOWED_ARGS.get(name);
  const unknown = Object.keys(args).filter((k) => !allowed.has(k));
  if (unknown.length) {
    const expected = [...allowed].join(', ') || '(none)';
    throw new ToolError(
      `unknown argument(s) for ${name}: ${unknown.join(', ')}. Expected: ${expected}.`,
    );
  }
  return args;
}

/**
 * Create a message handler.
 *
 * @param {import('./service.mjs').ConsoleService} service
 * @param {(msg: object) => void} send
 */
export function createServer(service, send) {
  const handlers = createHandlers(service);
  /** Running tools/call requests by JSON-RPC id, for notifications/cancelled. */
  const inFlight = new Map();

  async function callTool(params, signal) {
    const name = params?.name;
    const impl = Object.hasOwn(handlers, name) ? handlers[name] : undefined;
    if (!impl) {
      return { isError: true, content: [{ type: 'text', text: `Unknown tool: ${name}` }] };
    }
    try {
      const result = await impl(checkArguments(name, params.arguments), { signal });
      return { content: [{ type: 'text', text: serializeBounded(result) }] };
    } catch (e) {
      // Validation and upstream failures are tool-level results, not protocol
      // errors: the model should see them and can correct its next call.
      const expected =
        e instanceof ToolError ||
        e instanceof ValidationError ||
        e instanceof ConsoleError ||
        e instanceof RangeError;
      const text = expected ? e.message : `Unexpected error: ${e?.message || e}`;
      return { isError: true, content: [{ type: 'text', text: service.scrub?.(text) ?? text }] };
    }
  }

  /** Run a tool call. A cancelled request gets no response, per the MCP spec. */
  async function runTool(id, params) {
    const controller = new AbortController();
    inFlight.set(id, controller);
    try {
      const result = await callTool(params, controller.signal);
      if (!controller.signal.aborted) send({ id, result });
    } finally {
      if (inFlight.get(id) === controller) inFlight.delete(id);
    }
  }

  return async function handle(msg) {
    if (msg === null || typeof msg !== 'object' || Array.isArray(msg)) {
      return send({ id: null, error: { code: -32600, message: 'Invalid Request' } });
    }
    const { id, method, params } = msg;
    const isRequest = id !== undefined && id !== null;

    // Notifications are never answered, per JSON-RPC, and do no work here
    // apart from cancelling a running call.
    if (!isRequest) {
      if (method === 'notifications/cancelled') inFlight.get(params?.requestId)?.abort();
      return undefined;
    }

    try {
      switch (method) {
        case 'initialize': {
          const want = params?.protocolVersion;
          return send({
            id,
            result: {
              protocolVersion: SUPPORTED_PROTOCOLS.includes(want) ? want : SUPPORTED_PROTOCOLS[0],
              capabilities: { tools: {} },
              serverInfo: SERVER_INFO,
            },
          });
        }
        case 'ping':
          return send({ id, result: {} });
        case 'tools/list':
          return send({ id, result: { tools: TOOLS } });
        case 'tools/call':
          return await runTool(id, params);
        default:
          return send({ id, error: { code: -32601, message: `Method not found: ${method}` } });
      }
    } catch (e) {
      return send({ id, error: { code: -32603, message: String(e?.message || e) } });
    }
  };
}

/** Wire a handler to stdin/stdout. */
export function listen(handle, { input = process.stdin, output = process.stdout } = {}) {
  let closed = false;
  // A client that exits mid-call closes our stdout; writing to it then emits
  // EPIPE as an unhandled 'error' event and crashes with a stack trace.
  output.on?.('error', () => {
    closed = true;
  });
  const send = (m) => {
    if (closed || output.destroyed) return;
    output.write(`${JSON.stringify({ jsonrpc: '2.0', ...m })}\n`);
  };
  const rl = createInterface({ input });

  rl.on('line', (line) => {
    if (!line.trim()) return;
    let msg;
    try {
      msg = JSON.parse(line);
    } catch {
      return send({ id: null, error: { code: -32700, message: 'Parse error' } });
    }
    // Errors are handled inside; keep the reader alive regardless.
    Promise.resolve(handle(msg)).catch(() => {});
  });

  return { send, close: () => rl.close() };
}
