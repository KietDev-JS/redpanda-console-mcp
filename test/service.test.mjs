import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { ValidationError, StartOffset, ALL_PARTITIONS } from '../src/service.mjs';
import { MAX_RESULTS_LIMIT } from '../src/config.mjs';
import {
  makeClient,
  reply,
  dataFrame,
  endFrame,
  concat,
  topicList,
  sentEnvelope,
} from '../test-utils/helpers.mjs';

/** A streaming response carrying `n` messages then a clean trailer. */
function messages(n, make = (i) => ({ partitionId: i % 3, offset: String(i) })) {
  const frames = Array.from({ length: n }, (_, i) => dataFrame(make(i)));
  return reply({ chunks: [concat(...frames, endFrame())] });
}

describe('listTopics', () => {
  test('returns topics with a count', async () => {
    const { service } = makeClient(topicList(3));
    const out = await service.listTopics();
    assert.equal(out.count, 3);
    assert.equal(out.topics.length, 3);
  });

  test('sends the default page size', async () => {
    const { service, calls } = makeClient(topicList(0));
    await service.listTopics();
    assert.equal(calls[0].url.searchParams.get('page_size'), '100');
  });

  test('passes a name filter through', async () => {
    const { service, calls } = makeClient(topicList(0));
    await service.listTopics({ nameContains: 'orders' });
    assert.equal(calls[0].url.searchParams.get('filter.name_contains'), 'orders');
  });

  test('omits the filter when empty', async () => {
    const { service, calls } = makeClient(topicList(0));
    await service.listTopics({ nameContains: '' });
    assert.equal(calls[0].url.searchParams.has('filter.name_contains'), false);
  });

  test('surfaces the pagination token', async () => {
    const { service } = makeClient({ topics: [], next_page_token: 'abc' });
    assert.equal((await service.listTopics()).next_page_token, 'abc');
  });

  test('missing token is null, not undefined', async () => {
    const { service } = makeClient({ topics: [] });
    assert.equal((await service.listTopics()).next_page_token, null);
  });

  test('a malformed payload degrades to an empty list', async () => {
    for (const body of [null, [], 'text', { topics: 'nope' }]) {
      const { service } = makeClient(body);
      const out = await service.listTopics();
      assert.deepEqual(out.topics, []);
      assert.equal(out.count, 0);
    }
  });

  test('rejects a non-positive page size', async () => {
    const { service } = makeClient(topicList(0));
    await assert.rejects(service.listTopics({ pageSize: 0 }), ValidationError);
    await assert.rejects(service.listTopics({ pageSize: -1 }), /at least 1/);
  });

  test('rejects an oversized page size', async () => {
    const { service } = makeClient(topicList(0));
    await assert.rejects(service.listTopics({ pageSize: 1001 }), /must not exceed 1000/);
  });

  test('rejects a fractional page size instead of truncating', async () => {
    const { service } = makeClient(topicList(0));
    await assert.rejects(service.listTopics({ pageSize: 2.5 }), /must be an integer/);
  });
});

describe('clusterInfo', () => {
  test('calls the Connect endpoint with an empty message', async () => {
    const { service, calls } = makeClient({ brokers: [{ nodeId: 1 }] });
    const out = await service.clusterInfo();
    assert.match(calls[0].url.pathname, /ClusterStatusService\/GetKafkaInfo$/);
    assert.equal(calls[0].body, '{}');
    assert.deepEqual(out, { brokers: [{ nodeId: 1 }] });
  });
});

describe('describeTopic', () => {
  test('requests the configurations subresource', async () => {
    const { service, calls } = makeClient({ configs: [] });
    await service.describeTopic('orders');
    assert.equal(calls[0].url.pathname, '/v1/topics/orders/configurations');
  });

  test('percent-encodes a topic name so it cannot escape the path', async () => {
    const { service, calls } = makeClient({ configs: [] });
    await service.describeTopic('../../admin');
    assert.equal(calls[0].url.pathname, '/v1/topics/..%2F..%2Fadmin/configurations');
    assert.equal(calls[0].url.pathname.includes('/admin/'), false);
  });

  test('encodes other reserved characters', async () => {
    const { service, calls } = makeClient({ configs: [] });
    await service.describeTopic('a b%c?d#e');
    assert.equal(calls[0].url.pathname, '/v1/topics/a%20b%25c%3Fd%23e/configurations');
  });

  test('rejects an empty topic', async () => {
    const { service } = makeClient({});
    await assert.rejects(service.describeTopic(''), ValidationError);
    await assert.rejects(service.describeTopic('   '), /non-empty/);
    await assert.rejects(service.describeTopic(null), /non-empty/);
  });
});

describe('fetchMessages', () => {
  test('normalises each streamed message', async () => {
    const { service } = makeClient(messages(2));
    const out = await service.fetchMessages({ topic: 't', startOffset: -2, maxResults: 10 });
    assert.equal(out.length, 2);
    assert.equal(out[0].offset, 0);
    assert.equal(out[1].offset, 1);
  });

  test('sends the documented request shape', async () => {
    const { service, calls } = makeClient(messages(0));
    await service.fetchMessages({ topic: 't', startOffset: -2, maxResults: 5, partitionId: 3 });
    assert.deepEqual(sentEnvelope(calls[0]), {
      topic: 't',
      startOffset: -2,
      partitionId: 3,
      maxResults: 5,
    });
  });

  test('stops reading once max_results is reached, even if the server sends more', async () => {
    const { service } = makeClient(messages(50));
    const out = await service.fetchMessages({ topic: 't', startOffset: -2, maxResults: 3 });
    assert.equal(out.length, 3, 'must not buffer more than the caller asked for');
  });

  test('ignores frames without a data object', async () => {
    const { service } = makeClient(
      reply({
        chunks: [
          concat(
            dataFrame({ offset: '1' }),
            // A progress/phase frame, which ListMessages interleaves.
            (() => {
              const e = new TextEncoder().encode(JSON.stringify({ phase: { phase: 'Consuming' } }));
              const f = new Uint8Array(5 + e.length);
              new DataView(f.buffer).setUint32(1, e.length, false);
              f.set(e, 5);
              return f;
            })(),
            endFrame(),
          ),
        ],
      }),
    );
    const out = await service.fetchMessages({ topic: 't', startOffset: -2, maxResults: 10 });
    assert.equal(out.length, 1);
  });

  test('includes startTimestamp only when supplied', async () => {
    const { service, calls } = makeClient(messages(0));
    await service.fetchMessages({
      topic: 't',
      startOffset: -4,
      maxResults: 5,
      startTimestampMs: 1700000000000,
    });
    assert.equal(sentEnvelope(calls[0]).startTimestamp, 1700000000000);
  });

  test('includes a base64 filter only when supplied', async () => {
    const { service, calls } = makeClient(messages(0));
    await service.fetchMessages({
      topic: 't',
      startOffset: -2,
      maxResults: 5,
      filterCode: 'return true;',
    });
    const sent = sentEnvelope(calls[0]);
    assert.equal(
      Buffer.from(sent.filterInterpreterCode, 'base64').toString('utf-8'),
      'return true;',
    );
  });

  test('rejects max_results above the Console cap with actionable advice', async () => {
    const { service } = makeClient(messages(0));
    await assert.rejects(
      service.fetchMessages({ topic: 't', startOffset: -2, maxResults: MAX_RESULTS_LIMIT + 1 }),
      (e) => {
        assert.ok(e instanceof ValidationError);
        assert.match(e.message, /must not exceed 500/);
        assert.match(e.message, /fetch_by_offset/);
        return true;
      },
    );
  });

  test('accepts exactly the cap', async () => {
    const { service } = makeClient(messages(1));
    await assert.doesNotReject(
      service.fetchMessages({ topic: 't', startOffset: -2, maxResults: MAX_RESULTS_LIMIT }),
    );
  });

  test('rejects max_results below 1', async () => {
    const { service } = makeClient(messages(0));
    await assert.rejects(
      service.fetchMessages({ topic: 't', startOffset: -2, maxResults: 0 }),
      /at least 1/,
    );
  });

  test('rejects a partition below -1', async () => {
    const { service } = makeClient(messages(0));
    await assert.rejects(
      service.fetchMessages({ topic: 't', startOffset: -2, maxResults: 5, partitionId: -2 }),
      /partition_id must be -1/,
    );
  });

  test('accepts -1 for all partitions', async () => {
    const { service, calls } = makeClient(messages(0));
    await service.fetchMessages({
      topic: 't',
      startOffset: -2,
      maxResults: 5,
      partitionId: ALL_PARTITIONS,
    });
    assert.equal(sentEnvelope(calls[0]).partitionId, -1);
  });

  test('rejects an empty topic', async () => {
    const { service } = makeClient(messages(0));
    await assert.rejects(
      service.fetchMessages({ topic: '', startOffset: -2, maxResults: 5 }),
      /non-empty/,
    );
  });
});

describe('fetchLatest', () => {
  test('uses the RECENT sentinel', async () => {
    const { service, calls } = makeClient(messages(1));
    await service.fetchLatest({ topic: 't' });
    assert.equal(sentEnvelope(calls[0]).startOffset, StartOffset.RECENT);
  });

  test('defaults to 10 messages', async () => {
    const { service, calls } = makeClient(messages(1));
    await service.fetchLatest({ topic: 't' });
    assert.equal(sentEnvelope(calls[0]).maxResults, 10);
  });
});

describe('fetchByOffset', () => {
  test('passes the offset through as the start offset', async () => {
    const { service, calls } = makeClient(messages(1));
    await service.fetchByOffset({ topic: 't', offset: 4321 });
    assert.equal(sentEnvelope(calls[0]).startOffset, 4321);
  });

  test('offset 0 is valid and is not confused with a sentinel', async () => {
    const { service, calls } = makeClient(messages(1));
    await service.fetchByOffset({ topic: 't', offset: 0 });
    assert.equal(sentEnvelope(calls[0]).startOffset, 0);
  });

  test('rejects a negative offset and names the alternative', async () => {
    const { service } = makeClient(messages(0));
    await assert.rejects(service.fetchByOffset({ topic: 't', offset: -1 }), (e) => {
      assert.match(e.message, /must be non-negative/);
      assert.match(e.message, /fetch_latest|search_messages/);
      return true;
    });
  });
});

describe('fetchByTime', () => {
  test('uses the TIMESTAMP sentinel and sends the time', async () => {
    const { service, calls } = makeClient(messages(1));
    await service.fetchByTime({ topic: 't', timestampMs: 1700000000000 });
    const sent = sentEnvelope(calls[0]);
    assert.equal(sent.startOffset, StartOffset.TIMESTAMP);
    assert.equal(sent.startTimestamp, 1700000000000);
  });

  test('defaults to 20 messages', async () => {
    const { service, calls } = makeClient(messages(1));
    await service.fetchByTime({ topic: 't', timestampMs: 0 });
    assert.equal(sentEnvelope(calls[0]).maxResults, 20);
  });

  test('rejects a negative timestamp', async () => {
    const { service } = makeClient(messages(0));
    await assert.rejects(service.fetchByTime({ topic: 't', timestampMs: -1 }), /non-negative/);
  });
});

describe('searchMessages', () => {
  test('scans from the oldest message by default', async () => {
    const { service, calls } = makeClient(messages(1));
    await service.searchMessages({ topic: 't', text: 'x' });
    assert.equal(sentEnvelope(calls[0]).startOffset, StartOffset.OLDEST);
  });

  test('attaches a filter that searches key and value', async () => {
    const { service, calls } = makeClient(messages(1));
    await service.searchMessages({ topic: 't', text: 'needle' });
    const code = Buffer.from(sentEnvelope(calls[0]).filterInterpreterCode, 'base64').toString();
    assert.match(code, /__rp_needle = "needle"/);
    assert.match(code, /__rp_str\(key\)/);
    assert.match(code, /__rp_str\(value\)/);
  });

  test('a timestamp overrides the start offset', async () => {
    const { service, calls } = makeClient(messages(1));
    await service.searchMessages({
      topic: 't',
      text: 'x',
      startOffset: StartOffset.OLDEST,
      startTimestampMs: 1700000000000,
    });
    assert.equal(sentEnvelope(calls[0]).startOffset, StartOffset.TIMESTAMP);
  });

  test('an explicit start offset is honoured', async () => {
    const { service, calls } = makeClient(messages(1));
    await service.searchMessages({ topic: 't', text: 'x', startOffset: 500 });
    assert.equal(sentEnvelope(calls[0]).startOffset, 500);
  });

  test('case-insensitive search folds the needle', async () => {
    const { service, calls } = makeClient(messages(1));
    await service.searchMessages({ topic: 't', text: 'MiXeD', caseSensitive: false });
    const code = Buffer.from(sentEnvelope(calls[0]).filterInterpreterCode, 'base64').toString();
    assert.match(code, /__rp_needle = "mixed"/);
    assert.match(code, /toLowerCase\(\)/);
  });

  test('rejects empty search text as a validation error', async () => {
    const { service } = makeClient(messages(0));
    await assert.rejects(service.searchMessages({ topic: 't', text: '' }), ValidationError);
  });

  test('rejects oversized search text as a validation error', async () => {
    const { service } = makeClient(messages(0));
    await assert.rejects(
      service.searchMessages({ topic: 't', text: 'x'.repeat(5000) }),
      /too long/,
    );
  });

  test('rejects a negative start timestamp', async () => {
    const { service } = makeClient(messages(0));
    await assert.rejects(
      service.searchMessages({ topic: 't', text: 'x', startTimestampMs: -5 }),
      /non-negative/,
    );
  });
});
