// End-to-end tests: the real binary over stdio, talking to a mock Console on
// the loopback interface. No external system is contacted.

import { test, describe, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer as createHttpServer } from 'node:http';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { dataFrame, endFrame, concat } from '../test-utils/helpers.mjs';

const BIN = fileURLToPath(new URL('../bin/redpanda-console-mcp.mjs', import.meta.url));
const API_KEY = 'stdio-test-key-value';

/** Start the binary and exchange JSON-RPC lines with it. */
function startServer(baseUrl) {
  const child = spawn(process.execPath, [BIN], {
    env: { ...process.env, CONSOLE_BASE_URL: baseUrl, CONSOLE_API_KEY: API_KEY, CONSOLE_MAX_RETRIES: '1' },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  const waiting = new Map();
  const unsolicited = [];
  createInterface({ input: child.stdout }).on('line', (line) => {
    const msg = JSON.parse(line);
    const resolve = waiting.get(msg.id);
    if (resolve) {
      waiting.delete(msg.id);
      resolve(msg);
    } else {
      unsolicited.push(msg);
    }
  });
  const write = (msg) => child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', ...msg })}\n`);
  return {
    child,
    unsolicited,
    notify: write,
    request: (msg) =>
      new Promise((resolve) => {
        waiting.set(msg.id, resolve);
        write(msg);
      }),
    stop: () =>
      new Promise((resolve) => {
        child.once('exit', resolve);
        child.kill();
      }),
  };
}

describe('stdio transport against a mock Console', () => {
  let http;
  let server;
  let flaky = 0;
  let tailClosed;
  const seenAuth = [];

  before(async () => {
    http = createHttpServer((req, res) => {
      seenAuth.push(req.headers.authorization);
      if (req.url.startsWith('/v1/topics?')) {
        const url = new URL(req.url, 'http://mock');
        res.setHeader('content-type', 'application/json');
        res.end(
          JSON.stringify(
            url.searchParams.get('page_token') === 'p2'
              ? { topics: [{ name: 'second' }], next_page_token: '' }
              : { topics: [{ name: 'first' }], next_page_token: 'p2' },
          ),
        );
      } else if (req.url.includes('GetKafkaInfo')) {
        flaky += 1;
        res.statusCode = flaky === 1 ? 503 : 200;
        res.setHeader('content-type', 'application/json');
        res.end(flaky === 1 ? '{"message":"warming up"}' : '{"brokers":[]}');
      } else if (req.url.includes('ListMessages')) {
        let body = Buffer.alloc(0);
        req.on('data', (c) => {
          body = Buffer.concat([body, c]);
        });
        req.on('end', () => {
          const request = JSON.parse(body.subarray(5).toString());
          res.setHeader('content-type', 'application/connect+json');
          if (request.topic === 'quiet') {
            // A live tail with nothing to send: hold the response open.
            res.flushHeaders();
            res.on('close', () => tailClosed?.());
            return;
          }
          res.end(
            Buffer.from(
              concat(
                dataFrame({ offset: '41', value: { normalizedPayload: Buffer.from('hello').toString('base64') } }),
                endFrame(),
              ),
            ),
          );
        });
      } else {
        res.statusCode = 401;
        res.end(`denied ${API_KEY}`);
      }
    });
    await new Promise((resolve) => http.listen(0, '127.0.0.1', resolve));
    server = startServer(`http://127.0.0.1:${http.address().port}`);
  });

  after(async () => {
    await server.stop();
    http.closeAllConnections?.();
    await new Promise((resolve) => http.close(resolve));
  });

  const call = async (id, name, args = {}) => {
    const res = await server.request({ id, method: 'tools/call', params: { name, arguments: args } });
    return res.result;
  };

  test('initialize completes the handshake', async () => {
    const res = await server.request({
      id: 1,
      method: 'initialize',
      params: { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 't', version: '0' } },
    });
    assert.equal(res.result.protocolVersion, '2025-06-18');
    assert.equal(res.result.serverInfo.name, 'redpanda-console-mcp');
    server.notify({ method: 'notifications/initialized' });
  });

  test('tools/list returns the seven read-only tools', async () => {
    const res = await server.request({ id: 2, method: 'tools/list' });
    assert.deepEqual(res.result.tools.map((t) => t.name).sort(), [
      'cluster_info',
      'describe_topic',
      'fetch_by_offset',
      'fetch_by_time',
      'fetch_latest',
      'list_topics',
      'search_messages',
    ]);
    assert.ok(res.result.tools.every((t) => t.annotations.readOnlyHint === true));
  });

  test('list_topics pages through real HTTP', async () => {
    const first = JSON.parse((await call(3, 'list_topics')).content[0].text);
    assert.equal(first.next_page_token, 'p2');
    const second = JSON.parse((await call(4, 'list_topics', { page_token: 'p2' })).content[0].text);
    assert.equal(second.topics[0].name, 'second');
    assert.equal(second.next_page_token, null);
  });

  test('a transient 503 is retried', async () => {
    const result = await call(5, 'cluster_info');
    assert.equal(result.isError, undefined);
    assert.equal(flaky, 2);
  });

  test('fetch_latest decodes a real Connect stream', async () => {
    const [message] = JSON.parse((await call(6, 'fetch_latest', { topic: 't' })).content[0].text);
    assert.equal(message.offset, 41);
    assert.equal(message.value, 'hello');
  });

  test('the API key is sent upstream but scrubbed from errors', async () => {
    const result = await call(7, 'describe_topic', { topic: 't' });
    assert.equal(result.isError, true);
    assert.equal(result.content[0].text.includes(API_KEY), false);
    assert.ok(seenAuth.includes(`Bearer ${API_KEY}`));
  });

  test('notifications/cancelled closes the upstream connection without a response', async () => {
    const closed = new Promise((resolve) => {
      tailClosed = resolve;
    });
    const pending = server.request({
      id: 8,
      method: 'tools/call',
      params: { name: 'fetch_latest', arguments: { topic: 'quiet' } },
    });
    let answered = false;
    pending.then(() => {
      answered = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 200));
    server.notify({ method: 'notifications/cancelled', params: { requestId: 8 } });
    await closed;

    // Prove the server is still responsive and stayed silent about request 8.
    const ping = await server.request({ id: 9, method: 'ping' });
    assert.deepEqual(ping.result, {});
    assert.equal(answered, false);
  });
});

describe('stdio transport with piped input', () => {
  test('a call already running finishes after stdin closes', async () => {
    // docs/SETUP.md and CI both pipe one request in and read the answer, so
    // closing stdin must not cancel work in progress.
    const http = createHttpServer((_req, res) => {
      setTimeout(() => {
        res.setHeader('content-type', 'application/json');
        res.end('{"brokers":[]}');
      }, 100);
    });
    await new Promise((resolve) => http.listen(0, '127.0.0.1', resolve));
    const server = startServer(`http://127.0.0.1:${http.address().port}`);
    try {
      const answer = server.request({
        id: 1,
        method: 'tools/call',
        params: { name: 'cluster_info', arguments: {} },
      });
      server.child.stdin.end();
      const res = await answer;
      assert.equal(res.result.isError, undefined);
    } finally {
      server.child.kill();
      http.closeAllConnections?.();
      await new Promise((resolve) => http.close(resolve));
    }
  });

  test('credentials in the base URL produce a warning that does not repeat them', async () => {
    const child = spawn(process.execPath, [BIN], {
      env: { ...process.env, CONSOLE_BASE_URL: 'http://user:hunter2@127.0.0.1:9' },
      stdio: ['pipe', 'ignore', 'pipe'],
    });
    let stderr = '';
    child.stderr.on('data', (c) => {
      stderr += c;
    });
    child.stdin.end();
    await new Promise((resolve) => child.once('exit', resolve));
    assert.match(stderr, /credentials in CONSOLE_BASE_URL are ignored/);
    assert.equal(stderr.includes('hunter2'), false);
  });
});
