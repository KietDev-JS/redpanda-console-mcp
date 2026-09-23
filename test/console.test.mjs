import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import {
  ConsoleClient,
  ConsoleError,
  ConsoleHTTPError,
  ConsoleProtocolError,
  ConsoleRPCError,
  encodeEnvelope,
  iterEnvelopes,
} from '../src/console.mjs';
import { loadConfig } from '../src/config.mjs';
import {
  makeClient,
  reply,
  dataFrame,
  endFrame,
  concat,
  chunked,
  envelope,
  sentEnvelope,
  TEST_ENV,
} from '../test-utils/helpers.mjs';

/** Turn an array of byte chunks into an async iterable. */
async function* iter(chunks) {
  for (const c of chunks) yield c;
}

async function collect(gen) {
  const out = [];
  for await (const v of gen) out.push(v);
  return out;
}

describe('encodeEnvelope', () => {
  test('prefixes a 5-byte header with a big-endian length', () => {
    const frame = encodeEnvelope({ a: 1 });
    const payload = JSON.stringify({ a: 1 });
    assert.equal(frame[0], 0, 'flag byte is zero for a data frame');
    const view = new DataView(frame.buffer);
    assert.equal(view.getUint32(1, false), payload.length);
    assert.equal(Buffer.from(frame.subarray(5)).toString(), payload);
  });

  test('length is measured in bytes, not characters', () => {
    const frame = encodeEnvelope({ a: 'é' });
    const view = new DataView(frame.buffer);
    const expected = Buffer.byteLength(JSON.stringify({ a: 'é' }), 'utf-8');
    assert.equal(view.getUint32(1, false), expected);
    assert.equal(frame.length, 5 + expected);
  });
});

describe('iterEnvelopes', () => {
  test('reads a single whole frame', async () => {
    const frames = await collect(iterEnvelopes(iter([dataFrame({ offset: '1' })])));
    assert.equal(frames.length, 1);
    assert.deepEqual(JSON.parse(Buffer.from(frames[0].payload).toString()), { data: { offset: '1' } });
  });

  test('reads several frames delivered in one chunk', async () => {
    const bytes = concat(dataFrame({ offset: '1' }), dataFrame({ offset: '2' }), endFrame());
    const frames = await collect(iterEnvelopes(iter([bytes])));
    assert.equal(frames.length, 3);
    assert.equal(frames[2].flags & 0b10, 0b10, 'trailer carries the end-of-stream flag');
  });

  test('reassembles frames split across chunk boundaries', async () => {
    const bytes = concat(dataFrame({ offset: '1' }), dataFrame({ offset: '2' }), endFrame());
    // Every split size must produce identical output, including splits that
    // land inside the 5-byte header.
    for (const size of [1, 2, 3, 5, 7, 13, 64]) {
      const frames = await collect(iterEnvelopes(iter(chunked(bytes, size))));
      assert.equal(frames.length, 3, `chunk size ${size}`);
      assert.deepEqual(
        JSON.parse(Buffer.from(frames[1].payload).toString()),
        { data: { offset: '2' } },
        `chunk size ${size}`,
      );
    }
  });

  test('handles an empty stream', async () => {
    assert.deepEqual(await collect(iterEnvelopes(iter([]))), []);
  });

  test('handles a zero-length payload', async () => {
    const frames = await collect(iterEnvelopes(iter([new Uint8Array([0, 0, 0, 0, 0])])));
    assert.equal(frames.length, 1);
    assert.equal(frames[0].payload.length, 0);
  });

  test('a truncated header is reported, not silently dropped', async () => {
    await assert.rejects(
      collect(iterEnvelopes(iter([new Uint8Array([0, 0, 0])]))),
      /ended mid-frame with 3 trailing bytes/,
    );
  });

  test('a truncated payload is reported', async () => {
    const full = dataFrame({ offset: '1' });
    await assert.rejects(
      collect(iterEnvelopes(iter([full.subarray(0, full.length - 2)]))),
      ConsoleProtocolError,
    );
  });

  test('surfaces the compressed flag to the caller', async () => {
    const frames = await collect(iterEnvelopes(iter([envelope({ x: 1 }, 0b01)])));
    assert.equal(frames[0].flags & 0b01, 0b01);
  });
});

describe('ConsoleClient REST', () => {
  test('GETs the expected URL with query params', async () => {
    const { client, calls } = makeClient({ topics: [] });
    await client.getJson('/v1/topics', { page_size: 10 });
    assert.equal(calls[0].method, 'GET');
    assert.equal(calls[0].url.pathname, '/v1/topics');
    assert.equal(calls[0].url.searchParams.get('page_size'), '10');
  });

  test('omits empty query params', async () => {
    const { client, calls } = makeClient({});
    await client.getJson('/v1/topics', { page_size: 10, 'filter.name_contains': '' });
    assert.equal(calls[0].url.searchParams.has('filter.name_contains'), false);
  });

  test('sends no Authorization header without an api key', async () => {
    const { client, calls } = makeClient({});
    await client.getJson('/v1/topics');
    assert.equal(calls[0].headers.Authorization, undefined);
  });

  test('sends a bearer token when configured', async () => {
    const { client, calls } = makeClient({}, { ...TEST_ENV, CONSOLE_API_KEY: 'tok' });
    await client.getJson('/v1/topics');
    assert.equal(calls[0].headers.Authorization, 'Bearer tok');
  });

  test('identifies itself with a User-Agent', async () => {
    const { client, calls } = makeClient({});
    await client.getJson('/v1/topics');
    assert.equal(calls[0].headers['User-Agent'], 'redpanda-console-mcp');
  });

  test('preserves a reverse-proxy subpath', async () => {
    const { client, calls } = makeClient({}, { CONSOLE_BASE_URL: 'https://p.example.com/rp' });
    await client.getJson('/v1/topics');
    assert.equal(calls[0].url.pathname, '/rp/v1/topics');
  });

  test('raises ConsoleHTTPError on 4xx', async () => {
    const { client } = makeClient(reply({ status: 404, text: 'nope' }));
    await assert.rejects(client.getJson('/v1/topics'), ConsoleHTTPError);
  });

  test('extracts a structured error message from the body', async () => {
    const { client } = makeClient(
      reply({ status: 400, text: JSON.stringify({ code: 'invalid', message: 'bad topic' }) }),
    );
    await assert.rejects(client.getJson('/v1/topics'), /invalid: bad topic/);
  });

  test('extracts a nested error object', async () => {
    const { client } = makeClient(
      reply({ status: 500, text: JSON.stringify({ error: { code: 'internal', message: 'boom' } }) }),
    );
    await assert.rejects(client.getJson('/v1/topics'), /internal: boom/);
  });

  test('falls back to the raw body when it is not structured', async () => {
    const { client } = makeClient(reply({ status: 502, text: 'upstream down' }));
    await assert.rejects(client.getJson('/v1/topics'), /upstream down/);
  });

  test('describes an empty error body rather than printing nothing', async () => {
    const { client } = makeClient(reply({ status: 500, text: '' }));
    await assert.rejects(client.getJson('/v1/topics'), /\(empty response\)/);
  });

  test('a non-JSON 200 points at the likely misconfiguration', async () => {
    const { client } = makeClient(reply({ status: 200, text: '<html>login</html>' }));
    await assert.rejects(client.getJson('/v1/topics'), (e) => {
      assert.ok(e instanceof ConsoleProtocolError);
      assert.match(e.message, /CONSOLE_BASE_URL points at a Redpanda Console API root/);
      return true;
    });
  });

  test('a network failure names the target', async () => {
    const { client } = makeClient(new Error('ECONNREFUSED'));
    await assert.rejects(client.getJson('/v1/topics'), (e) => {
      assert.ok(e instanceof ConsoleError);
      assert.match(e.message, /cannot reach Redpanda Console at https:\/\/console\.example\.com/);
      return true;
    });
  });

  test('a timeout is reported as a timeout', async () => {
    const abort = new Error('aborted');
    abort.name = 'TimeoutError';
    const { client } = makeClient(abort);
    await assert.rejects(client.getJson('/v1/topics'), /timed out after 60000ms/);
  });

  test('passes an abort signal so a hung server cannot stall forever', async () => {
    const { client, calls } = makeClient({});
    await client.getJson('/v1/topics');
    assert.ok(calls[0].signal, 'expected an AbortSignal');
  });
});

describe('ConsoleClient unary RPC', () => {
  test('uses application/json, because streaming content type yields 415', async () => {
    const { client, calls } = makeClient({ brokers: [] });
    await client.unaryRpc('/svc/Method', { a: 1 });
    assert.equal(calls[0].method, 'POST');
    assert.equal(calls[0].headers['Content-Type'], 'application/json');
    assert.equal(calls[0].body, JSON.stringify({ a: 1 }));
  });

  test('sends an empty object when no message is given', async () => {
    const { client, calls } = makeClient({});
    await client.unaryRpc('/svc/Method');
    assert.equal(calls[0].body, '{}');
  });

  test('returns the decoded payload', async () => {
    const { client } = makeClient({ brokers: [{ nodeId: 1 }] });
    assert.deepEqual(await client.unaryRpc('/svc/Method'), { brokers: [{ nodeId: 1 }] });
  });

  test('raises an in-band Connect error', async () => {
    const { client } = makeClient({ error: { code: 'permission_denied', message: 'no' } });
    await assert.rejects(client.unaryRpc('/svc/Method'), (e) => {
      assert.ok(e instanceof ConsoleRPCError);
      assert.equal(e.code, 'permission_denied');
      return true;
    });
  });

  test('defaults a code-less in-band error to "unknown"', async () => {
    const { client } = makeClient({ error: { message: 'no' } });
    await assert.rejects(client.unaryRpc('/svc/Method'), /unknown: no/);
  });

  test('an array response is returned untouched', async () => {
    const { client } = makeClient([1, 2]);
    assert.deepEqual(await client.unaryRpc('/svc/Method'), [1, 2]);
  });

  test('raises on HTTP failure', async () => {
    const { client } = makeClient(reply({ status: 415, text: 'unsupported media type' }));
    await assert.rejects(client.unaryRpc('/svc/Method'), ConsoleHTTPError);
  });
});

describe('ConsoleClient streaming RPC', () => {
  test('sends a Connect-framed body with the streaming content type', async () => {
    const { client, calls } = makeClient(reply({ chunks: [endFrame()] }));
    await collect(client.streamRpc('/svc/List', { topic: 't' }));
    assert.equal(calls[0].headers['Content-Type'], 'application/connect+json');
    assert.deepEqual(sentEnvelope(calls[0]), { topic: 't' });
  });

  test('yields data frames and swallows the trailer', async () => {
    const { client } = makeClient(
      reply({ chunks: [concat(dataFrame({ offset: '1' }), dataFrame({ offset: '2' }), endFrame())] }),
    );
    const frames = await collect(client.streamRpc('/svc/List', {}));
    assert.deepEqual(frames, [{ data: { offset: '1' } }, { data: { offset: '2' } }]);
  });

  test('reassembles across realistic chunk boundaries', async () => {
    const bytes = concat(dataFrame({ offset: '1' }), dataFrame({ offset: '2' }), endFrame());
    const { client } = makeClient(reply({ chunks: chunked(bytes, 3) }));
    const frames = await collect(client.streamRpc('/svc/List', {}));
    assert.equal(frames.length, 2);
  });

  test('raises an error carried in the end-of-stream trailer', async () => {
    const { client } = makeClient(
      reply({
        chunks: [
          concat(
            dataFrame({ offset: '1' }),
            endFrame({ error: { code: 'resource_exhausted', message: 'too many' } }),
          ),
        ],
      }),
    );
    await assert.rejects(collect(client.streamRpc('/svc/List', {})), (e) => {
      assert.ok(e instanceof ConsoleRPCError);
      assert.equal(e.code, 'resource_exhausted');
      assert.match(e.message, /too many/);
      return true;
    });
  });

  test('rejects a compressed frame instead of emitting garbage', async () => {
    const { client } = makeClient(reply({ chunks: [envelope({ data: {} }, 0b01)] }));
    await assert.rejects(collect(client.streamRpc('/svc/List', {})), /compressed Connect frame/);
  });

  test('rejects malformed JSON inside a frame', async () => {
    const bad = new Uint8Array([0, 0, 0, 0, 3, 0x7b, 0x7b, 0x7b]); // "{{{"
    const { client } = makeClient(reply({ chunks: [bad] }));
    await assert.rejects(collect(client.streamRpc('/svc/List', {})), /Malformed JSON/);
  });

  test('skips empty and non-object frames', async () => {
    const empty = new Uint8Array([0, 0, 0, 0, 0]);
    const scalar = envelope(42);
    const { client } = makeClient(
      reply({ chunks: [concat(empty, scalar, dataFrame({ offset: '9' }), endFrame())] }),
    );
    const frames = await collect(client.streamRpc('/svc/List', {}));
    assert.deepEqual(frames, [{ data: { offset: '9' } }]);
  });

  test('raises on HTTP failure before streaming starts', async () => {
    const { client } = makeClient(reply({ status: 403, text: 'forbidden' }));
    await assert.rejects(collect(client.streamRpc('/svc/List', {})), ConsoleHTTPError);
  });

  test('tolerates a response with no body', async () => {
    const { client } = makeClient(reply({ status: 200, chunks: [] }));
    assert.deepEqual(await collect(client.streamRpc('/svc/List', {})), []);
  });

  test('releases the reader when the caller stops early', async () => {
    const bytes = concat(
      dataFrame({ offset: '1' }),
      dataFrame({ offset: '2' }),
      dataFrame({ offset: '3' }),
      endFrame(),
    );
    const { client } = makeClient(reply({ chunks: chunked(bytes, 4) }));
    const gen = client.streamRpc('/svc/List', {});
    const first = await gen.next();
    assert.deepEqual(first.value, { data: { offset: '1' } });
    // Abandoning the generator must not throw.
    await gen.return();
  });
});

describe('ConsoleClient url building', () => {
  test('rejects nothing at transport level - path safety is the service layer job', async () => {
    const client = new ConsoleClient(loadConfig(TEST_ENV), { fetch: async () => ({ ok: true, status: 200, text: async () => '{}' }) });
    assert.equal(client.url('/v1/topics').pathname, '/v1/topics');
  });
});
