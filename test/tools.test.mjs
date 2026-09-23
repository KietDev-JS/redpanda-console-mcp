import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { TOOLS, intArg, stringArg, boolArg, ToolError } from '../src/tools.mjs';
import { MAX_RESULTS_LIMIT } from '../src/config.mjs';
import { makeClient, reply, dataFrame, endFrame, concat, topicList, sentEnvelope } from '../test-utils/helpers.mjs';

function messages(n) {
  const frames = Array.from({ length: n }, (_, i) => dataFrame({ offset: String(i) }));
  return reply({ chunks: [concat(...frames, endFrame())] });
}

describe('intArg', () => {
  test('accepts integers', () => {
    assert.equal(intArg(5, 'n'), 5);
    assert.equal(intArg(0, 'n'), 0);
    assert.equal(intArg(-1, 'n', { min: -1 }), -1);
  });

  test('accepts numeric strings, which some clients send', () => {
    assert.equal(intArg('42', 'n'), 42);
  });

  test('uses the fallback for absent values', () => {
    assert.equal(intArg(undefined, 'n', { fallback: 7 }), 7);
    assert.equal(intArg(null, 'n', { fallback: 7 }), 7);
    assert.equal(intArg('', 'n', { fallback: 7 }), 7);
  });

  test('requires a value when there is no fallback', () => {
    assert.throws(() => intArg(undefined, 'offset'), /offset is required/);
  });

  test('rejects fractions rather than silently truncating', () => {
    assert.throws(() => intArg(2.7, 'n'), /must be an integer/);
    assert.throws(() => intArg('2.7', 'n'), /must be an integer/);
  });

  test('rejects non-numeric input', () => {
    assert.throws(() => intArg('abc', 'n'), ToolError);
    assert.throws(() => intArg({}, 'n'), ToolError);
    assert.throws(() => intArg([], 'n'), ToolError);
    assert.throws(() => intArg(true, 'n'), ToolError);
  });

  test('rejects NaN and Infinity', () => {
    assert.throws(() => intArg(NaN, 'n'), ToolError);
    assert.throws(() => intArg(Infinity, 'n'), ToolError);
  });

  test('enforces bounds', () => {
    assert.throws(() => intArg(0, 'n', { min: 1 }), /at least 1/);
    assert.throws(() => intArg(501, 'n', { max: 500 }), /must not exceed 500/);
  });
});

describe('stringArg', () => {
  test('returns strings', () => {
    assert.equal(stringArg('x', 'topic'), 'x');
  });

  test('required rejects missing and blank', () => {
    assert.throws(() => stringArg(undefined, 'topic', { required: true }), /required/);
    assert.throws(() => stringArg('  ', 'topic', { required: true }), /must not be empty/);
  });

  test('rejects non-strings', () => {
    assert.throws(() => stringArg(5, 'topic'), /must be a string/);
  });

  test('falls back when optional', () => {
    assert.equal(stringArg(undefined, 'q', { fallback: '' }), '');
  });
});

describe('boolArg', () => {
  test('passes booleans through and falls back otherwise', () => {
    assert.equal(boolArg(true, 'b', false), true);
    assert.equal(boolArg(undefined, 'b', true), true);
  });

  test('rejects truthy strings rather than guessing', () => {
    assert.throws(() => boolArg('true', 'b', true), /must be a boolean/);
  });
});

describe('tool declarations', () => {
  test('exposes exactly the seven documented tools', () => {
    assert.deepEqual(
      TOOLS.map((t) => t.name).sort(),
      [
        'cluster_info',
        'describe_topic',
        'fetch_by_offset',
        'fetch_by_time',
        'fetch_latest',
        'list_topics',
        'search_messages',
      ],
    );
  });

  test('every tool has a description and an object schema', () => {
    for (const t of TOOLS) {
      assert.ok(t.description && t.description.length > 20, `${t.name} needs a description`);
      assert.equal(t.inputSchema.type, 'object', t.name);
      assert.equal(t.inputSchema.additionalProperties, false, `${t.name} must reject extra args`);
    }
  });

  test('message-fetching tools cap max_results at the Console limit', () => {
    for (const name of ['fetch_latest', 'fetch_by_offset', 'fetch_by_time', 'search_messages']) {
      const t = TOOLS.find((x) => x.name === name);
      assert.equal(t.inputSchema.properties.max_results.maximum, MAX_RESULTS_LIMIT, name);
    }
  });

  test('required arguments are declared', () => {
    const byName = Object.fromEntries(TOOLS.map((t) => [t.name, t]));
    assert.deepEqual(byName.describe_topic.inputSchema.required, ['topic']);
    assert.deepEqual(byName.fetch_by_offset.inputSchema.required, ['topic', 'offset']);
    assert.deepEqual(byName.fetch_by_time.inputSchema.required, ['topic', 'timestamp_ms']);
    assert.deepEqual(byName.search_messages.inputSchema.required, ['topic', 'text']);
  });

  test('cluster_info takes no arguments', () => {
    const t = TOOLS.find((x) => x.name === 'cluster_info');
    assert.deepEqual(t.inputSchema.properties, {});
  });
});

describe('handlers', () => {
  test('list_topics applies defaults', async () => {
    const { handlers, calls } = makeClient(topicList(2));
    const out = await handlers.list_topics({});
    assert.equal(out.count, 2);
    assert.equal(calls[0].url.searchParams.get('page_size'), '100');
  });

  test('list_topics rejects a bad page size', async () => {
    const { handlers } = makeClient(topicList(0));
    await assert.rejects(handlers.list_topics({ page_size: 'lots' }), ToolError);
    await assert.rejects(handlers.list_topics({ page_size: 0 }), /at least 1/);
  });

  test('describe_topic requires a topic', async () => {
    const { handlers } = makeClient({});
    await assert.rejects(handlers.describe_topic({}), /topic is required/);
  });

  test('fetch_latest defaults to 10 and honours an override', async () => {
    const { handlers, calls } = makeClient(messages(1));
    await handlers.fetch_latest({ topic: 't' });
    assert.equal(sentEnvelope(calls[0]).maxResults, 10);
    await handlers.fetch_latest({ topic: 't', max_results: 3 });
    assert.equal(sentEnvelope(calls[1]).maxResults, 3);
  });

  test('fetch_latest rejects an over-cap max_results at the tool boundary', async () => {
    const { handlers } = makeClient(messages(0));
    await assert.rejects(handlers.fetch_latest({ topic: 't', max_results: 9999 }), /must not exceed 500/);
  });

  test('fetch_by_offset requires an offset', async () => {
    const { handlers } = makeClient(messages(0));
    await assert.rejects(handlers.fetch_by_offset({ topic: 't' }), /offset is required/);
  });

  test('fetch_by_offset rejects a negative offset', async () => {
    const { handlers } = makeClient(messages(0));
    await assert.rejects(handlers.fetch_by_offset({ topic: 't', offset: -1 }), /at least 0/);
  });

  test('fetch_by_time requires a timestamp', async () => {
    const { handlers } = makeClient(messages(0));
    await assert.rejects(handlers.fetch_by_time({ topic: 't' }), /timestamp_ms is required/);
  });

  test('search_messages requires non-empty text', async () => {
    const { handlers } = makeClient(messages(0));
    await assert.rejects(handlers.search_messages({ topic: 't' }), /text is required/);
    await assert.rejects(handlers.search_messages({ topic: 't', text: '  ' }), /must not be empty/);
  });

  test('search_messages passes case_sensitive through', async () => {
    const { handlers, calls } = makeClient(messages(1));
    await handlers.search_messages({ topic: 't', text: 'AbC', case_sensitive: false });
    const code = Buffer.from(sentEnvelope(calls[0]).filterInterpreterCode, 'base64').toString();
    assert.match(code, /"abc"/);
  });

  test('search_messages treats a null timestamp as absent', async () => {
    const { handlers, calls } = makeClient(messages(1));
    await handlers.search_messages({ topic: 't', text: 'x', start_timestamp_ms: null });
    assert.equal('startTimestamp' in sentEnvelope(calls[0]), false);
  });

  test('partition_id defaults to all partitions', async () => {
    const { handlers, calls } = makeClient(messages(1));
    await handlers.fetch_latest({ topic: 't' });
    assert.equal(sentEnvelope(calls[0]).partitionId, -1);
  });

  test('partition_id rejects values below -1', async () => {
    const { handlers } = makeClient(messages(0));
    await assert.rejects(handlers.fetch_latest({ topic: 't', partition_id: -5 }), /at least -1/);
  });
});
