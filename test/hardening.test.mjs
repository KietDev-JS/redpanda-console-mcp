// Regression tests for reliability, security and agent-facing behaviour.
// Everything runs against a fake fetch; nothing here touches the network.

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { loadConfig, redact, scrubSecrets, ConfigError } from '../src/config.mjs';
import {
  ConsoleError,
  ConsoleHTTPError,
  ConsoleProtocolError,
  ConsoleRPCError,
  ConsoleStreamInterrupted,
  MAX_FRAME_BYTES,
} from '../src/console.mjs';
import { ValidationError } from '../src/service.mjs';
import { normalizeMessage } from '../src/models.mjs';
import { createServer, serializeBounded } from '../src/server.mjs';
import { TOOLS, intArg, ToolError } from '../src/tools.mjs';
import {
  makeClient,
  reply,
  envelope,
  dataFrame,
  endFrame,
  concat,
  topicList,
  timeoutError,
} from '../test-utils/helpers.mjs';

const BASE = 'https://console.example.com';
const RETRY_ENV = { CONSOLE_BASE_URL: BASE, CONSOLE_MAX_RETRIES: '2' };

/** Answer each request with the next response, repeating the last one. */
function sequence(...responses) {
  let i = 0;
  return () => responses[Math.min(i++, responses.length - 1)];
}

/** A sleep that records its delays and returns immediately. */
function fakeSleep() {
  const delays = [];
  const sleep = async (ms) => {
    delays.push(ms);
  };
  return { delays, sleep };
}

async function collect(gen) {
  const out = [];
  for await (const item of gen) out.push(item);
  return out;
}

describe('retries', () => {
  for (const status of [429, 502, 503, 504]) {
    test(`retries HTTP ${status} and returns the eventual success`, async () => {
      const { delays, sleep } = fakeSleep();
      const { client, calls } = makeClient(
        sequence(reply({ status, text: 'busy' }), { ok: true }),
        RETRY_ENV,
        { sleep },
      );
      assert.deepEqual(await client.getJson('/v1/topics'), { ok: true });
      assert.equal(calls.length, 2);
      assert.deepEqual(delays, [250]);
    });
  }

  test('backs off exponentially, then surfaces the last response', async () => {
    const { delays, sleep } = fakeSleep();
    const { client, calls } = makeClient(reply({ status: 503, text: 'down' }), RETRY_ENV, { sleep });
    await assert.rejects(client.getJson('/v1/topics'), ConsoleHTTPError);
    assert.equal(calls.length, 3);
    assert.deepEqual(delays, [250, 500]);
  });

  test('does not retry a client error', async () => {
    const { sleep } = fakeSleep();
    const { client, calls } = makeClient(reply({ status: 404, text: 'nope' }), RETRY_ENV, { sleep });
    await assert.rejects(client.getJson('/v1/topics'), ConsoleHTTPError);
    assert.equal(calls.length, 1);
  });

  test('retries a network failure', async () => {
    const { sleep } = fakeSleep();
    const { client, calls } = makeClient(sequence(new Error('ECONNRESET'), { ok: 1 }), RETRY_ENV, { sleep });
    assert.deepEqual(await client.unaryRpc('/svc/M'), { ok: 1 });
    assert.equal(calls.length, 2);
  });

  test('reports the network failure once retries run out', async () => {
    const { sleep } = fakeSleep();
    const { client, calls } = makeClient(new Error('ECONNRESET'), RETRY_ENV, { sleep });
    await assert.rejects(client.getJson('/v1/topics'), /cannot reach Redpanda Console.*ECONNRESET/);
    assert.equal(calls.length, 3);
  });

  test('never retries a timeout, which already used the whole budget', async () => {
    const { sleep } = fakeSleep();
    const { client, calls } = makeClient(timeoutError(), RETRY_ENV, { sleep });
    await assert.rejects(client.getJson('/v1/topics'), /timed out after/);
    assert.equal(calls.length, 1);
  });

  test('honours Retry-After in seconds', async () => {
    const { delays, sleep } = fakeSleep();
    const { client } = makeClient(
      sequence(reply({ status: 429, text: '', headers: { 'retry-after': '2' } }), {}),
      RETRY_ENV,
      { sleep },
    );
    await client.getJson('/v1/topics');
    assert.deepEqual(delays, [2000]);
  });

  test('honours Retry-After as an HTTP date', async () => {
    const { delays, sleep } = fakeSleep();
    const when = new Date(Date.now() + 3000).toUTCString();
    const { client } = makeClient(
      sequence(reply({ status: 503, text: '', headers: { 'retry-after': when } }), {}),
      RETRY_ENV,
      { sleep },
    );
    await client.getJson('/v1/topics');
    assert.ok(delays[0] > 0 && delays[0] <= 3000, `unexpected delay ${delays[0]}`);
  });

  test('gives up rather than waiting out a long Retry-After', async () => {
    const { delays, sleep } = fakeSleep();
    const { client, calls } = makeClient(
      // Inside the 60 s deadline, so only the delay cap can refuse this wait.
      reply({ status: 429, text: 'slow down', headers: { 'retry-after': '10' } }),
      RETRY_ENV,
      { sleep },
    );
    await assert.rejects(client.getJson('/v1/topics'), /HTTP 429/);
    assert.equal(calls.length, 1);
    assert.deepEqual(delays, []);
  });

  test('does not retry past the request deadline', async () => {
    const { sleep } = fakeSleep();
    const { client, calls } = makeClient(
      reply({ status: 503, text: 'down' }),
      { ...RETRY_ENV, CONSOLE_TIMEOUT: '0.1' },
      { sleep },
    );
    await assert.rejects(client.getJson('/v1/topics'), /HTTP 503/);
    assert.equal(calls.length, 1);
  });

  test('CONSOLE_MAX_RETRIES=0 disables retries', async () => {
    const { client, calls } = makeClient(reply({ status: 503, text: 'down' }));
    await assert.rejects(client.getJson('/v1/topics'), /HTTP 503/);
    assert.equal(calls.length, 1);
  });

  test('retries a stream that fails before it starts', async () => {
    const { sleep } = fakeSleep();
    const { service, calls } = makeClient(
      sequence(
        reply({ status: 503, text: 'down' }),
        reply({ chunks: [concat(dataFrame({ offset: '4' }), endFrame())] }),
      ),
      RETRY_ENV,
      { sleep },
    );
    const out = await service.fetchLatest({ topic: 't' });
    assert.equal(out[0].offset, 4);
    assert.equal(calls.length, 2);
  });

  test('does not restart a stream that fails midway', async () => {
    const { sleep } = fakeSleep();
    const { service, calls } = makeClient(
      reply({ chunks: [dataFrame({ offset: '4' })], failWith: new TypeError('terminated') }),
      RETRY_ENV,
      { sleep },
    );
    const out = await service.fetchLatest({ topic: 't' });
    assert.equal(out.messages.length, 1);
    assert.equal(calls.length, 1);
  });
});

describe('cancellation', () => {
  /** A fetch that hangs until its signal aborts, as a quiet live tail does. */
  const hanging = (_url, init) =>
    new Promise((_resolve, reject) => {
      init.signal.addEventListener('abort', () => reject(init.signal.reason), { once: true });
    });

  const call = (id, name = 'fetch_latest', args = { topic: 't' }) => ({
    id,
    method: 'tools/call',
    params: { name, arguments: args },
  });

  test('notifications/cancelled aborts the upstream request and sends no response', async () => {
    const { service, calls } = makeClient(hanging);
    const sent = [];
    const handle = createServer(service, (m) => sent.push(m));
    const running = handle(call(1));
    await new Promise((r) => setImmediate(r));
    assert.equal(calls.length, 1);

    await handle({ method: 'notifications/cancelled', params: { requestId: 1, reason: 'user' } });
    await running;
    assert.equal(calls[0].signal.aborted, true);
    assert.equal(sent.length, 0, 'a cancelled request must not be answered');
  });

  test('cancelling one call leaves another running', async () => {
    const { service, calls } = makeClient(hanging);
    const sent = [];
    const handle = createServer(service, (m) => sent.push(m));
    const first = handle(call(1));
    const second = handle(call(2));
    await new Promise((r) => setImmediate(r));

    await handle({ method: 'notifications/cancelled', params: { requestId: 1 } });
    await first;
    assert.equal(calls[0].signal.aborted, true);
    assert.equal(calls[1].signal.aborted, false);

    await handle({ method: 'notifications/cancelled', params: { requestId: 2 } });
    await second;
  });

  test('cancelling an unknown or finished request is harmless', async () => {
    const { service } = makeClient(topicList(1));
    const sent = [];
    const handle = createServer(service, (m) => sent.push(m));
    await handle(call(1, 'list_topics', {}));
    await handle({ method: 'notifications/cancelled', params: { requestId: 1 } });
    await handle({ method: 'notifications/cancelled', params: { requestId: 99 } });
    await handle({ method: 'notifications/cancelled' });
    assert.equal(sent.length, 1);
  });

  test('every tool forwards cancellation to its upstream request', async () => {
    const args = {
      list_topics: {},
      cluster_info: {},
      describe_topic: { topic: 't' },
      fetch_latest: { topic: 't' },
      fetch_by_offset: { topic: 't', offset: 0 },
      fetch_by_time: { topic: 't', timestamp_ms: 0 },
      search_messages: { topic: 't', text: 'x' },
    };
    for (const tool of TOOLS) {
      const { service, calls } = makeClient(hanging);
      const handle = createServer(service, () => {});
      const running = handle(call(7, tool.name, args[tool.name]));
      await new Promise((r) => setImmediate(r));
      await handle({ method: 'notifications/cancelled', params: { requestId: 7 } });
      await running;
      assert.equal(calls[0].signal.aborted, true, `${tool.name} ignored cancellation`);
    }
  });

  test('a cancelled client call says it was cancelled, not timed out', async () => {
    const { client } = makeClient(hanging);
    const controller = new AbortController();
    const running = client.getJson('/v1/topics', undefined, { signal: controller.signal });
    await new Promise((r) => setImmediate(r));
    controller.abort();
    await assert.rejects(running, /was cancelled/);
  });

  test('cancellation interrupts a retry wait', async () => {
    const controller = new AbortController();
    const { client, calls } = makeClient(reply({ status: 503, text: 'down' }), RETRY_ENV);
    const running = client.getJson('/v1/topics', undefined, { signal: controller.signal });
    await new Promise((r) => setImmediate(r));
    controller.abort();
    await assert.rejects(running, /was cancelled/);
    assert.equal(calls.length, 1);
  });
});

describe('JSON-RPC conformance', () => {
  const run = async (msg) => {
    const { service, calls } = makeClient(topicList(1));
    const sent = [];
    await createServer(service, (m) => sent.push(m))(msg);
    return { sent, calls };
  };

  test('a tools/call notification does no work and gets no response', async () => {
    const { sent, calls } = await run({
      method: 'tools/call',
      params: { name: 'list_topics', arguments: {} },
    });
    assert.equal(sent.length, 0);
    assert.equal(calls.length, 0);
  });

  test('ping and tools/list notifications are not answered', async () => {
    assert.equal((await run({ method: 'ping' })).sent.length, 0);
    assert.equal((await run({ method: 'tools/list' })).sent.length, 0);
  });

  test('a non-object message is an Invalid Request', async () => {
    for (const bad of [42, 'text', null, [], [{ id: 1, method: 'ping' }]]) {
      const { sent } = await run(bad);
      assert.equal(sent[0].error.code, -32600, JSON.stringify(bad));
      assert.equal(sent[0].id, null);
    }
  });

  test('request id 0 is a request, not a notification', async () => {
    const { sent } = await run({ id: 0, method: 'ping' });
    assert.equal(sent[0].id, 0);
  });
});

describe('secret handling', () => {
  const KEY = 'sk-test-value-123456';
  const env = { CONSOLE_BASE_URL: BASE, CONSOLE_API_KEY: KEY, CONSOLE_MAX_RETRIES: '0' };

  const toolError = async (responder) => {
    const { service } = makeClient(responder, env);
    const sent = [];
    await createServer(service, (m) => sent.push(m))({
      id: 1,
      method: 'tools/call',
      params: { name: 'list_topics', arguments: {} },
    });
    assert.equal(sent[0].result.isError, true);
    return sent[0].result.content[0].text;
  };

  test('an error body echoing the API key is scrubbed', async () => {
    const text = await toolError(reply({ status: 400, text: `rejected key ${KEY}` }));
    assert.doesNotMatch(text, new RegExp(KEY));
    assert.match(text, /<redacted>/);
  });

  test('an echoed Authorization header is scrubbed', async () => {
    const text = await toolError(reply({ status: 400, text: 'got Authorization: Bearer abc.def-ghi' }));
    assert.doesNotMatch(text, /abc\.def-ghi/);
  });

  test('a network error naming the key is scrubbed', async () => {
    const text = await toolError(new Error(`connect failed for ${KEY}`));
    assert.doesNotMatch(text, new RegExp(KEY));
  });

  test('a partial-result reason is scrubbed', async () => {
    const { service } = makeClient(
      reply({ chunks: [dataFrame({ offset: '1' })], failWith: new TypeError(`reset ${KEY}`) }),
      env,
    );
    const out = await service.fetchLatest({ topic: 't' });
    assert.doesNotMatch(out.incomplete.reason, new RegExp(KEY));
  });

  test('message content is returned untouched', async () => {
    const value = 'password=hunter2 Bearer abc';
    const { service } = makeClient(
      reply({
        chunks: [
          concat(
            dataFrame({ value: { normalizedPayload: Buffer.from(value).toString('base64') } }),
            endFrame(),
          ),
        ],
      }),
      env,
    );
    const [message] = await service.fetchLatest({ topic: 't' });
    assert.equal(message.value, value);
  });

  test('scrubSecrets covers the common credential shapes', () => {
    const cases = [
      ['Authorization: Basic dXNlcjpwYXNz', 'dXNlcjpwYXNz'],
      ['https://user:hunter2@host.example.com/x', 'hunter2'],
      ['https://user:p@ss@host.example.com/x', 'p@ss'],
      ['{"password":"hunter2"}', 'hunter2'],
      ['api_key=abc123&x=1', 'abc123'],
      ['access-token: abc123', 'abc123'],
    ];
    for (const [input, leaked] of cases) {
      assert.equal(scrubSecrets(input).includes(leaked), false, input);
    }
  });

  test('scrubSecrets leaves ordinary diagnostics readable', () => {
    for (const text of [
      'HTTP 404 from https://console.example.com/v1/topics: not found',
      'page_token=abc',
      "Unexpected token '<' in JSON",
    ]) {
      assert.equal(scrubSecrets(text), text);
    }
  });

  test('an empty API key does not corrupt the text', () => {
    assert.equal(scrubSecrets('hello', ['']), 'hello');
  });

  test('configuration errors do not print URL credentials', () => {
    assert.throws(
      () => loadConfig({ CONSOLE_BASE_URL: 'ftp://user:hunter2@host.example.com' }),
      (e) => e instanceof ConfigError && !e.message.includes('hunter2'),
    );
    assert.throws(
      () => loadConfig({ CONSOLE_BASE_URL: 'https://user:hunter2@bad host' }),
      (e) => e instanceof ConfigError && !e.message.includes('hunter2'),
    );
  });

  test('URL credentials are flagged and kept out of the base URL', () => {
    const config = loadConfig({ CONSOLE_BASE_URL: 'https://user:hunter2@host.example.com' });
    assert.equal(config.urlCredentialsIgnored, true);
    assert.equal(config.baseUrl, 'https://host.example.com');
    assert.equal(JSON.stringify(redact(config)).includes('hunter2'), false);
  });

  test('a plain URL is not flagged', () => {
    assert.equal(loadConfig({ CONSOLE_BASE_URL: BASE }).urlCredentialsIgnored, false);
  });
});

describe('configuration limits', () => {
  test('CONSOLE_MAX_RETRIES defaults to 2', () => {
    assert.equal(loadConfig({ CONSOLE_BASE_URL: BASE }).maxRetries, 2);
  });

  test('CONSOLE_MAX_RETRIES accepts 0 to 5', () => {
    for (const n of [0, 5]) {
      assert.equal(loadConfig({ CONSOLE_BASE_URL: BASE, CONSOLE_MAX_RETRIES: String(n) }).maxRetries, n);
    }
  });

  test('CONSOLE_MAX_RETRIES rejects anything else', () => {
    for (const bad of ['-1', '6', '1.5', 'many']) {
      assert.throws(
        () => loadConfig({ CONSOLE_BASE_URL: BASE, CONSOLE_MAX_RETRIES: bad }),
        /CONSOLE_MAX_RETRIES must be an integer from 0 to 5/,
      );
    }
  });

  test('CONSOLE_TIMEOUT rejects a value that would overflow the timer', () => {
    assert.throws(
      () => loadConfig({ CONSOLE_BASE_URL: BASE, CONSOLE_TIMEOUT: '3000000' }),
      /CONSOLE_TIMEOUT must not exceed/,
    );
  });
});

describe('malformed streams', () => {
  test('an HTML 200 on a stream points at the misconfiguration', async () => {
    const { client } = makeClient(
      reply({ chunks: [new TextEncoder().encode('<html>')], headers: { 'content-type': 'text/html' } }),
    );
    await assert.rejects(collect(client.streamRpc('/svc/List', {})), (e) => {
      assert.ok(e instanceof ConsoleProtocolError);
      assert.match(e.message, /text\/html/);
      assert.match(e.message, /CONSOLE_BASE_URL/);
      return true;
    });
  });

  test('a Connect content type is accepted', async () => {
    const { client } = makeClient(
      reply({
        chunks: [concat(dataFrame({ offset: '1' }), endFrame())],
        headers: { 'content-type': 'application/connect+json' },
      }),
    );
    assert.equal((await collect(client.streamRpc('/svc/List', {}))).length, 1);
  });

  test('an impossible frame length fails at once instead of buffering', async () => {
    const header = new Uint8Array(5);
    new DataView(header.buffer).setUint32(1, MAX_FRAME_BYTES + 1, false);
    const { client } = makeClient(reply({ chunks: [header] }));
    await assert.rejects(collect(client.streamRpc('/svc/List', {})), /exceeds the \d+ byte limit/);
  });

  test('HTML without a content type is also caught by the frame limit', async () => {
    const { client } = makeClient(reply({ chunks: [new TextEncoder().encode('<!DOCTYPE html><html>')] }));
    await assert.rejects(collect(client.streamRpc('/svc/List', {})), ConsoleProtocolError);
  });

  test('a stream closed without its trailer is reported', async () => {
    const { client } = makeClient(reply({ chunks: [dataFrame({ offset: '1' })] }));
    await assert.rejects(collect(client.streamRpc('/svc/List', {})), ConsoleStreamInterrupted);
  });

  test('messages from a stream closed without its trailer are kept and flagged', async () => {
    const { service } = makeClient(reply({ chunks: [dataFrame({ offset: '1' })] }));
    const out = await service.fetchLatest({ topic: 't' });
    assert.equal(out.messages[0].offset, 1);
    assert.match(out.incomplete.reason, /before it finished/);
  });

  test('an empty end-of-stream frame counts as the trailer', async () => {
    const trailer = new Uint8Array([0b10, 0, 0, 0, 0]);
    const { client } = makeClient(reply({ chunks: [concat(dataFrame({ offset: '1' }), trailer)] }));
    assert.equal((await collect(client.streamRpc('/svc/List', {}))).length, 1);
  });

  test('stopping early is not mistaken for a truncated stream', async () => {
    const { service } = makeClient(
      reply({ chunks: [concat(dataFrame({ offset: '1' }), dataFrame({ offset: '2' }))] }),
    );
    const out = await service.fetchLatest({ topic: 't', maxResults: 1 });
    assert.equal(out.length, 1);
  });

  test('an error frame is raised instead of looking like no matches', async () => {
    const { service } = makeClient(
      reply({ chunks: [concat(envelope({ error: { message: 'filter failed' } }), endFrame())] }),
    );
    await assert.rejects(service.searchMessages({ topic: 't', text: 'x' }), (e) => {
      assert.ok(e instanceof ConsoleRPCError);
      assert.match(e.message, /filter failed/);
      return true;
    });
  });

  test('an error frame without a message is still reported', async () => {
    const { service } = makeClient(reply({ chunks: [concat(envelope({ error: {} }), endFrame())] }));
    await assert.rejects(service.fetchLatest({ topic: 't' }), /reported an error/);
  });
});

describe('actionable errors', () => {
  const hints = [
    [401, /CONSOLE_API_KEY/],
    [403, /read access/],
    [404, /list_topics/],
    [429, /rate limiting/],
    [502, /retry shortly/],
    [503, /retry shortly/],
    [504, /narrow the request/],
  ];
  for (const [status, hint] of hints) {
    test(`HTTP ${status} says what to do next`, async () => {
      const { client } = makeClient(reply({ status, text: 'failed' }));
      await assert.rejects(client.getJson('/v1/topics'), hint);
    });
  }

  test('an unlisted status keeps the plain message', async () => {
    const { client } = makeClient(reply({ status: 418, text: 'teapot' }));
    await assert.rejects(client.getJson('/v1/topics'), (e) => e.message.endsWith('teapot'));
  });

  test('an HTML error page is summarised, not pasted', async () => {
    const { client } = makeClient(reply({ status: 401, text: '<!DOCTYPE html><html>secret page</html>' }));
    await assert.rejects(client.getJson('/v1/topics'), (e) => {
      assert.match(e.message, /HTML page/);
      assert.doesNotMatch(e.message, /secret page/);
      return true;
    });
  });

  test('a server-supplied code cannot pull in an inherited property', async () => {
    const { client } = makeClient({ error: { code: 'constructor', message: 'no' } });
    await assert.rejects(client.unaryRpc('/svc/M'), (e) => e.message === 'constructor: no');
  });

  test('Connect permission errors say what to do next', async () => {
    const { client } = makeClient({ error: { code: 'permission_denied', message: 'no' } });
    await assert.rejects(client.unaryRpc('/svc/M'), /permission_denied: no .*read access/);
  });

  test('a timeout says how to narrow the read', async () => {
    const { client } = makeClient(timeoutError());
    await assert.rejects(client.getJson('/v1/topics'), /CONSOLE_TIMEOUT/);
  });

  test('a network failure says what to check', async () => {
    const { client } = makeClient(new Error('ENOTFOUND'));
    await assert.rejects(client.getJson('/v1/topics'), (e) => {
      assert.ok(e instanceof ConsoleError);
      assert.match(e.message, /VPN/);
      return true;
    });
  });
});

describe('input validation', () => {
  test('dot segments cannot redirect describe_topic to another endpoint', async () => {
    for (const topic of ['.', '..']) {
      const { service, calls } = makeClient({});
      await assert.rejects(service.describeTopic(topic), ValidationError);
      assert.equal(calls.length, 0);
    }
  });

  test('dotted names that Kafka allows still work', async () => {
    const { service, calls } = makeClient({});
    await service.describeTopic('orders.v1..dlq');
    assert.equal(calls[0].url.pathname, '/v1/topics/orders.v1..dlq/configurations');
  });

  test('a topic name longer than Kafka allows is rejected', async () => {
    const { service, calls } = makeClient({});
    await assert.rejects(service.describeTopic('t'.repeat(250)), /must not exceed 249/);
    assert.equal(calls.length, 0);
  });

  test('a topic name at the limit is accepted', async () => {
    const { service } = makeClient({});
    await assert.doesNotReject(service.describeTopic('t'.repeat(249)));
  });

  test('search_messages requires a timestamp for start_offset -4', async () => {
    const { handlers, calls } = makeClient({});
    await assert.rejects(
      handlers.search_messages({ topic: 't', text: 'x', start_offset: -4 }),
      /start_timestamp_ms is required/,
    );
    assert.equal(calls.length, 0);
  });

  test('search_messages accepts start_offset -4 with a timestamp', async () => {
    const { handlers } = makeClient(reply({ chunks: [endFrame()] }));
    await assert.doesNotReject(
      handlers.search_messages({ topic: 't', text: 'x', start_offset: -4, start_timestamp_ms: 5 }),
    );
  });

  test('intArg rejects integers that cannot be represented exactly', () => {
    assert.throws(() => intArg(2 ** 53, 'offset'), ToolError);
    assert.throws(() => intArg('9007199254740993', 'offset'), ToolError);
  });

  test('intArg rejects numeric notations that are not plain integers', () => {
    for (const bad of ['1e3', '0x10', '1.0', 'Infinity']) {
      assert.throws(() => intArg(bad, 'offset'), ToolError, bad);
    }
  });

  test('intArg still accepts integer strings', () => {
    assert.equal(intArg('42', 'offset'), 42);
    assert.equal(intArg(' -1 ', 'partition_id'), -1);
  });
});

describe('list_topics pagination', () => {
  test('page_token is declared in the schema', () => {
    const tool = TOOLS.find((t) => t.name === 'list_topics');
    assert.equal(tool.inputSchema.properties.page_token.type, 'string');
    assert.equal(tool.inputSchema.required, undefined);
  });

  test('page_token is forwarded upstream', async () => {
    const { handlers, calls } = makeClient(topicList(1));
    await handlers.list_topics({ page_token: 'next+/=' });
    assert.equal(calls[0].url.searchParams.get('page_token'), 'next+/=');
  });

  test('page_token is omitted on the first page', async () => {
    const { handlers, calls } = makeClient(topicList(1));
    await handlers.list_topics({});
    assert.equal(calls[0].url.searchParams.has('page_token'), false);
  });

  test('a non-string page_token is rejected', async () => {
    const { handlers } = makeClient(topicList(1));
    await assert.rejects(handlers.list_topics({ page_token: 5 }), /page_token must be a string/);
  });

  test('an empty next token means there are no more pages', async () => {
    const { service } = makeClient({ topics: [], next_page_token: '' });
    assert.equal((await service.listTopics()).next_page_token, null);
  });

  test('a camelCase next token is understood', async () => {
    const { service } = makeClient({ topics: [], nextPageToken: 'abc' });
    assert.equal((await service.listTopics()).next_page_token, 'abc');
  });

  test('the token from one page fetches the next through the MCP layer', async () => {
    const { service, calls } = makeClient((url) =>
      url.searchParams.get('page_token') === 'p2'
        ? { topics: [{ name: 'b' }] }
        : { topics: [{ name: 'a' }], next_page_token: 'p2' },
    );
    const sent = [];
    const handle = createServer(service, (m) => sent.push(m));
    const page = async (id, args) => {
      await handle({ id, method: 'tools/call', params: { name: 'list_topics', arguments: args } });
      return JSON.parse(sent.at(-1).result.content[0].text);
    };
    const first = await page(1, {});
    const second = await page(2, { page_token: first.next_page_token });
    assert.equal(second.topics[0].name, 'b');
    assert.equal(second.next_page_token, null);
    assert.equal(calls.length, 2);
  });
});

describe('serializeBounded output', () => {
  const message = (i, size = 200, partition = i % 2) => ({
    partition,
    offset: i,
    value: 'x'.repeat(size),
  });

  test('measures bytes, not characters', () => {
    const text = serializeBounded(Array.from({ length: 50 }, () => ({ value: '日本語'.repeat(50) })), 5000);
    assert.ok(Buffer.byteLength(text) <= 5000);
    assert.ok(JSON.parse(text).truncated);
  });

  test('a result that fits is returned unchanged', () => {
    const items = [message(0), message(1)];
    assert.equal(serializeBounded(items), JSON.stringify(items));
  });

  test('says where to resume, per partition', () => {
    const items = Array.from({ length: 100 }, (_, i) => message(i));
    const { truncated, messages } = JSON.parse(serializeBounded(items, 3000));
    assert.equal(truncated.returned, messages.length);
    const dropped = items.slice(messages.length);
    for (const point of truncated.resume_from) {
      const lowest = Math.min(
        ...dropped.filter((m) => m.partition === point.partition_id).map((m) => m.offset),
      );
      assert.equal(point.offset, lowest);
    }
    assert.equal(truncated.resume_from.length, 2);
    assert.match(truncated.hint, /fetch_by_offset/);
  });

  test('caps the resume list on topics with many partitions', () => {
    const items = Array.from({ length: 200 }, (_, i) => message(i, 100, i));
    const { truncated } = JSON.parse(serializeBounded(items, 4000));
    assert.equal(truncated.resume_from.length, 16);
  });

  test('keeps the incomplete marker while reporting resume points', () => {
    const items = Array.from({ length: 100 }, (_, i) => message(i));
    const out = JSON.parse(
      serializeBounded({ messages: items, incomplete: { returned: 100, requested: 500, reason: 'r' } }, 3000),
    );
    assert.equal(out.incomplete.reason, 'r');
    assert.ok(out.truncated.resume_from.length > 0);
  });

  test('clips a single oversized message instead of returning nothing', () => {
    const items = [message(7, 50_000), message(8)];
    const text = serializeBounded(items, 2000);
    const out = JSON.parse(text);
    assert.ok(Buffer.byteLength(text) <= 2000);
    assert.equal(out.messages.length, 1);
    assert.equal(out.messages[0].offset, 7);
    assert.equal(out.messages[0].value_truncated, true);
    assert.equal(out.messages[0].value_total_chars, 50_000);
    assert.ok(out.messages[0].value.length > 0);
    assert.deepEqual(out.truncated.resume_from, [{ partition_id: 0, offset: 8 }]);
  });

  test('clipping accounts for JSON escaping and multi-byte text', () => {
    for (const unit of ['"\\\n', '日本語', '😀']) {
      const text = serializeBounded([{ partition: 0, offset: 1, value: unit.repeat(5000) }], 2000);
      assert.ok(Buffer.byteLength(text) <= 2000, JSON.stringify(unit));
      assert.equal(JSON.parse(text).messages[0].value_truncated, true);
    }
  });

  test('reports zero messages when even a clipped message cannot fit', () => {
    const huge = { partition: 0, offset: 1, value: 'x', headers: 'h'.repeat(5000) };
    const out = JSON.parse(serializeBounded([huge], 1000));
    assert.equal(out.truncated.returned, 0);
    assert.deepEqual(out.messages, []);
  });

  test('a shed topic list reports the count it actually returned', () => {
    const out = JSON.parse(serializeBounded({ ...topicList(400), count: 400, next_page_token: 'n' }, 4000));
    assert.equal(out.count, out.topics.length);
    assert.equal(out.truncated.of, 400);
    assert.match(out.truncated.hint, /page_size/);
    assert.equal(out.truncated.resume_from, undefined);
  });

  test('an empty list beside an oversized field still respects the cap', () => {
    const text = serializeBounded({ messages: [], incomplete: { reason: 'r'.repeat(5000) } }, 1000);
    assert.ok(Buffer.byteLength(text) <= 1000);
    assert.match(JSON.parse(text).error, /exceeded the size limit/);
  });

  test('a page of very large messages is reduced quickly', () => {
    const items = Array.from({ length: 500 }, (_, i) => message(i, 1_000_000));
    const started = Date.now();
    const text = serializeBounded(items);
    assert.ok(Buffer.byteLength(text) <= 200_000);
    assert.ok(Date.now() - started < 5000, 'shedding must stay linear in the page size');
  });
});

describe('message shape', () => {
  test('a binary key is flagged', () => {
    const raw = Buffer.from([0xff, 0xfe, 0x00, 0x81]).toString('base64');
    assert.equal(normalizeMessage({ key: { normalizedPayload: raw } }).key_is_binary, true);
  });

  test('a text key keeps the existing shape', () => {
    const raw = Buffer.from('order-1').toString('base64');
    assert.equal('key_is_binary' in normalizeMessage({ key: { normalizedPayload: raw } }), false);
  });
});
