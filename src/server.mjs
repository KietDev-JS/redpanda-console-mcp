// MCP stdio server: JSON-RPC 2.0 framing over newline-delimited stdin/stdout.

import { createInterface } from 'node:readline';
import { TOOLS, createHandlers, ToolError } from './tools.mjs';
import { ConsoleError } from './console.mjs';
import { ValidationError } from './service.mjs';

/** Protocol revisions this server understands, newest first. */
export const SUPPORTED_PROTOCOLS = ['2025-11-25', '2025-06-18', '2025-03-26', '2024-11-05'];

export const SERVER_INFO = { name: 'redpanda-console-mcp', version: '1.0.0' };

/** Max serialized bytes of a single tool result. */
export const MAX_OUT = 200_000;

/**
 * Serialize a result, shedding trailing items if it exceeds the byte cap.
 *
 * Hard-slicing the serialized string would emit unparseable JSON, so array
 * results lose whole elements and report the truncation instead.
 */
export function serializeBounded(result, max = MAX_OUT) {
  let text = JSON.stringify(result ?? null);
  if (text.length <= max) return text;

  if (Array.isArray(result)) {
    const kept = [...result];
    while (kept.length > 0) {
      kept.pop();
      text = JSON.stringify({
        truncated: { returned: kept.length, of: result.length, reason: 'result size limit' },
        messages: kept,
      });
      if (text.length <= max) return text;
    }
    return JSON.stringify({
      truncated: { returned: 0, of: result.length, reason: 'result size limit' },
      messages: [],
    });
  }

  if (result && typeof result === 'object' && Array.isArray(result.topics)) {
    const kept = [...result.topics];
    while (kept.length > 0) {
      kept.pop();
      text = JSON.stringify({
        ...result,
        topics: kept,
        truncated: { returned: kept.length, of: result.topics.length },
      });
      if (text.length <= max) return text;
    }
  }

  return JSON.stringify({
    error: 'Result exceeded the size limit and could not be reduced. Lower max_results.',
  });
}

/**
 * Create a message handler.
 *
 * @param {import('./service.mjs').ConsoleService} service
 * @param {(msg: object) => void} send
 */
export function createServer(service, send) {
  const handlers = createHandlers(service);

  async function callTool(params) {
    const name = params?.name;
    const impl = handlers[name];
    if (!impl) {
      return { isError: true, content: [{ type: 'text', text: `Unknown tool: ${name}` }] };
    }
    try {
      const result = await impl(params.arguments || {});
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
      return { isError: true, content: [{ type: 'text', text }] };
    }
  }

  return async function handle(msg) {
    const { id, method, params } = msg ?? {};
    const isRequest = id !== undefined && id !== null;

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
          return send({ id, result: await callTool(params) });
        default:
          // Notifications (no id) are never answered, per JSON-RPC.
          if (isRequest) {
            send({ id, error: { code: -32601, message: `Method not found: ${method}` } });
          }
      }
    } catch (e) {
      if (isRequest) {
        send({ id, error: { code: -32603, message: String(e?.message || e) } });
      }
    }
  };
}

/** Wire a handler to stdin/stdout. */
export function listen(handle, { input = process.stdin, output = process.stdout } = {}) {
  const send = (m) => output.write(`${JSON.stringify({ jsonrpc: '2.0', ...m })}\n`);
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
