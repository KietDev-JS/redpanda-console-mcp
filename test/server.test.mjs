import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import {
  createServer,
  listen,
  serializeBounded,
  SUPPORTED_PROTOCOLS,
  SERVER_INFO,
  MAX_OUT,
} from '../src/server.mjs';
import { readFileSync } from 'node:fs';
import { TOOLS } from '../src/tools.mjs';
import {
  makeClient,
  reply,
  dataFrame,
  endFrame,
  concat,
  topicList,
  timeoutError,
} from '../test-utils/helpers.mjs';

function messages(n, pad = '') {
  const frames = Array.from({ length: n }, (_, i) =>
    dataFrame({ offset: String(i), value: { normalizedPayload: Buffer.from(pad || `m${i}`).toString('base64') } }),
  );
  return reply({ chunks: [concat(...frames, endFrame())] });
}

/** Drive a server with one message and capture what it sends. */
async function exchange(responder, msg) {
  const { service } = makeClient(responder);
  const sent = [];
  const handle = createServer(service, (m) => sent.push(m));
  await handle(msg);
  return sent;
}

/** Parse the text payload of a tools/call result. */
function payload(result) {
  return JSON.parse(result.content[0].text);
}

describe('initialize', () => {
  test('echoes a supported protocol version', async () => {
    const [res] = await exchange({}, { id: 1, method: 'initialize', params: { protocolVersion: '2025-06-18' } });
    assert.equal(res.result.protocolVersion, '2025-06-18');
  });

  test('falls back to the newest version for an unknown request', async () => {
    const [res] = await exchange({}, { id: 1, method: 'initialize', params: { protocolVersion: '1999-01-01' } });
    assert.equal(res.result.protocolVersion, SUPPORTED_PROTOCOLS[0]);
  });

  test('advertises tool capability and identity', async () => {
    const [res] = await exchange({}, { id: 1, method: 'initialize', params: {} });
    assert.deepEqual(res.result.capabilities, { tools: {} });
    assert.equal(res.result.serverInfo.name, SERVER_INFO.name);
    assert.match(res.result.serverInfo.version, /^\d+\.\d+\.\d+$/);
  });
});

describe('protocol basics', () => {
  test('ping returns an empty result', async () => {
    const [res] = await exchange({}, { id: 2, method: 'ping' });
    assert.deepEqual(res.result, {});
  });

  test('tools/list returns all seven tools', async () => {
    const [res] = await exchange({}, { id: 3, method: 'tools/list' });
    assert.equal(res.result.tools.length, 7);
  });

  test('an unknown method is a JSON-RPC error', async () => {
    const [res] = await exchange({}, { id: 4, method: 'nope/nope' });
    assert.equal(res.error.code, -32601);
  });

  test('a notification is never answered', async () => {
    const sent = await exchange({}, { method: 'notifications/initialized' });
    assert.equal(sent.length, 0, 'notifications must not get a response');
  });

  test('an unknown notification is also silent', async () => {
    const sent = await exchange({}, { method: 'unknown/thing' });
    assert.equal(sent.length, 0);
  });

  test('a null id is treated as a notification', async () => {
    const sent = await exchange({}, { id: null, method: 'unknown' });
    assert.equal(sent.length, 0);
  });
});

describe('tools/call', () => {
  test('returns tool output as JSON text', async () => {
    const [res] = await exchange(topicList(2), {
      id: 5,
      method: 'tools/call',
      params: { name: 'list_topics', arguments: {} },
    });
    assert.equal(res.result.isError, undefined);
    assert.equal(payload(res.result).count, 2);
  });

  test('missing arguments default to an empty object', async () => {
    const [res] = await exchange({ brokers: [] }, {
      id: 6,
      method: 'tools/call',
      params: { name: 'cluster_info' },
    });
    assert.equal(res.result.isError, undefined);
  });

  test('an unknown tool is reported as a tool error, not a protocol error', async () => {
    const [res] = await exchange({}, {
      id: 7,
      method: 'tools/call',
      params: { name: 'drop_database', arguments: {} },
    });
    assert.equal(res.result.isError, true);
    assert.match(res.result.content[0].text, /Unknown tool/);
    assert.equal(res.error, undefined);
  });

  test('a validation failure is a tool error the model can correct', async () => {
    const [res] = await exchange({}, {
      id: 8,
      method: 'tools/call',
      params: { name: 'fetch_latest', arguments: { topic: 't', max_results: 99999 } },
    });
    assert.equal(res.result.isError, true);
    assert.match(res.result.content[0].text, /must not exceed 500/);
  });

  test('an upstream HTTP failure is surfaced verbatim', async () => {
    const [res] = await exchange(reply({ status: 503, text: 'unavailable' }), {
      id: 9,
      method: 'tools/call',
      params: { name: 'list_topics', arguments: {} },
    });
    assert.equal(res.result.isError, true);
    assert.match(res.result.content[0].text, /503/);
  });

  test('a network failure is surfaced, not swallowed', async () => {
    const [res] = await exchange(new Error('ECONNREFUSED'), {
      id: 10,
      method: 'tools/call',
      params: { name: 'cluster_info', arguments: {} },
    });
    assert.equal(res.result.isError, true);
    assert.match(res.result.content[0].text, /cannot reach Redpanda Console/);
  });

  test('an unexpected error is labelled as such', async () => {
    const { service } = makeClient({});
    service.listTopics = async () => {
      throw new TypeError('bug');
    };
    const sent = [];
    const handle = createServer(service, (m) => sent.push(m));
    await handle({ id: 11, method: 'tools/call', params: { name: 'list_topics', arguments: {} } });
    assert.equal(sent[0].result.isError, true);
    assert.match(sent[0].result.content[0].text, /Unexpected error: bug/);
  });

  test('fetches messages end to end', async () => {
    const [res] = await exchange(messages(3), {
      id: 12,
      method: 'tools/call',
      params: { name: 'fetch_latest', arguments: { topic: 't', max_results: 3 } },
    });
    const out = payload(res.result);
    assert.equal(out.length, 3);
    assert.equal(out[0].offset, 0);
  });

  test('rejects unknown arguments instead of silently ignoring them', async () => {
    const { service, calls } = makeClient(messages(1));
    const sent = [];
    await createServer(service, (m) => sent.push(m))({
      id: 13,
      method: 'tools/call',
      params: { name: 'fetch_latest', arguments: { topic: 't', partition: 2 } },
    });
    assert.equal(sent[0].result.isError, true);
    assert.match(sent[0].result.content[0].text, /unknown argument\(s\) for fetch_latest: partition/);
    assert.match(sent[0].result.content[0].text, /Expected: topic, max_results, partition_id/);
    assert.equal(calls.length, 0, 'a rejected call must not reach the network');
  });

  test('rejects non-object arguments', async () => {
    for (const bad of [['t'], 'orders', 7]) {
      const [res] = await exchange({}, {
        id: 14,
        method: 'tools/call',
        params: { name: 'describe_topic', arguments: bad },
      });
      assert.equal(res.result.isError, true);
      assert.match(res.result.content[0].text, /arguments must be a JSON object/);
    }
  });

  test('does not resolve inherited properties as tool names', async () => {
    const [res] = await exchange({}, {
      id: 15,
      method: 'tools/call',
      params: { name: 'toString', arguments: {} },
    });
    assert.match(res.result.content[0].text, /Unknown tool: toString/);
  });

  test('a mid-stream timeout returns the messages received so far', async () => {
    const [res] = await exchange(
      reply({ chunks: [concat(dataFrame({ offset: '5' }))], failWith: timeoutError() }),
      {
        id: 16,
        method: 'tools/call',
        params: { name: 'search_messages', arguments: { topic: 't', text: 'x', start_offset: -3 } },
      },
    );
    assert.equal(res.result.isError, undefined);
    const out = payload(res.result);
    assert.equal(out.messages[0].offset, 5);
    assert.match(out.incomplete.reason, /timed out/);
  });

  test('a timeout with nothing received is a tool error, not "Unexpected error"', async () => {
    const [res] = await exchange(reply({ chunks: [], failWith: timeoutError() }), {
      id: 17,
      method: 'tools/call',
      params: { name: 'fetch_latest', arguments: { topic: 't' } },
    });
    assert.equal(res.result.isError, true);
    assert.doesNotMatch(res.result.content[0].text, /Unexpected error/);
    assert.match(res.result.content[0].text, /timed out after/);
  });

  test('every tool is annotated read-only and non-destructive', () => {
    for (const t of TOOLS) {
      assert.equal(t.annotations?.readOnlyHint, true, `${t.name} readOnlyHint`);
      assert.equal(t.annotations?.destructiveHint, false, `${t.name} destructiveHint`);
    }
  });

  test('serverInfo reports the package.json version', () => {
    const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
    assert.equal(SERVER_INFO.version, pkg.version);
    assert.equal(SERVER_INFO.name, pkg.name);
  });
});

describe('serializeBounded', () => {
  test('small results pass through unchanged', () => {
    assert.equal(serializeBounded({ a: 1 }), '{"a":1}');
  });

  test('null is serialised, not dropped', () => {
    assert.equal(serializeBounded(undefined), 'null');
  });

  test('an oversized array sheds elements and stays valid JSON', () => {
    const big = Array.from({ length: 500 }, (_, i) => ({ i, pad: 'x'.repeat(200) }));
    const text = serializeBounded(big, 5000);
    const parsed = JSON.parse(text); // must not throw
    assert.ok(text.length <= 5000);
    assert.ok(parsed.truncated.returned < parsed.truncated.of);
    assert.equal(parsed.truncated.of, 500);
    assert.ok(Array.isArray(parsed.messages));
  });

  test('an oversized partial result sheds messages and keeps the incomplete marker', () => {
    const msgs = Array.from({ length: 300 }, (_, i) => ({ i, pad: 'x'.repeat(200) }));
    const text = serializeBounded({ messages: msgs, incomplete: { returned: 300, requested: 500, reason: 'r' } }, 5000);
    const parsed = JSON.parse(text);
    assert.ok(text.length <= 5000);
    assert.equal(parsed.incomplete.reason, 'r');
    assert.ok(parsed.truncated.returned < 300);
  });

  test('an oversized topic list sheds topics and stays valid JSON', () => {
    const body = { ...topicList(400), count: 400 };
    const text = serializeBounded(body, 4000);
    const parsed = JSON.parse(text);
    assert.ok(text.length <= 4000);
    assert.ok(parsed.truncated.returned < 400);
  });

  test('an irreducible oversized object reports the limit instead of emitting broken JSON', () => {
    const text = serializeBounded({ blob: 'x'.repeat(10_000) }, 1000);
    const parsed = JSON.parse(text);
    assert.match(parsed.error, /exceeded the size limit/);
  });

  test('the default cap is generous enough for a full 500-message page', () => {
    assert.ok(MAX_OUT >= 200_000);
  });
});

describe('listen', () => {
  test('frames responses as newline-delimited JSON-RPC', async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const chunks = [];
    output.on('data', (c) => chunks.push(c.toString()));

    const handlerRef = { current: null };
    const { send } = listen((m) => handlerRef.current(m), { input, output });
    const { service } = makeClient({});
    handlerRef.current = createServer(service, send);

    input.write(`${JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'ping' })}\n`);
    await new Promise((r) => setTimeout(r, 20));

    const lines = chunks.join('').trim().split('\n');
    const parsed = JSON.parse(lines[0]);
    assert.equal(parsed.jsonrpc, '2.0');
    assert.equal(parsed.id, 1);
  });

  test('malformed JSON yields a parse error and keeps the reader alive', async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const chunks = [];
    output.on('data', (c) => chunks.push(c.toString()));

    const handlerRef = { current: null };
    const { send } = listen((m) => handlerRef.current(m), { input, output });
    const { service } = makeClient({});
    handlerRef.current = createServer(service, send);

    input.write('{not json\n');
    input.write(`${JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'ping' })}\n`);
    await new Promise((r) => setTimeout(r, 20));

    const lines = chunks.join('').trim().split('\n').map((l) => JSON.parse(l));
    assert.equal(lines[0].error.code, -32700);
    assert.equal(lines[1].id, 2, 'reader must survive a bad line');
  });

  test('stops writing after stdout errors (client went away) instead of crashing', () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const writes = [];
    output.write = (s) => writes.push(s);
    const { send } = listen(() => {}, { input, output });
    send({ id: 1, result: {} });
    output.emit('error', Object.assign(new Error('write EPIPE'), { code: 'EPIPE' }));
    send({ id: 2, result: {} });
    assert.equal(writes.length, 1);
    input.end();
  });

  test('blank lines are ignored', async () => {
    const input = new PassThrough();
    const output = new PassThrough();
    const chunks = [];
    output.on('data', (c) => chunks.push(c.toString()));

    const handlerRef = { current: null };
    const { send } = listen((m) => handlerRef.current(m), { input, output });
    const { service } = makeClient({});
    handlerRef.current = createServer(service, send);

    input.write('\n   \n');
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(chunks.length, 0);
  });
});
