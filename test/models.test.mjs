import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import {
  asInt,
  b64ToText,
  decodePayload,
  formatTimestamp,
  normalizeMessage,
} from '../src/models.mjs';

const b64 = (s) => Buffer.from(s, 'utf-8').toString('base64');

describe('asInt', () => {
  test('passes through numbers', () => {
    assert.equal(asInt(5), 5);
    assert.equal(asInt(0), 0);
    assert.equal(asInt(-7), -7);
  });

  test('parses int64-as-string, which is how proto3 sends large values', () => {
    assert.equal(asInt('105825'), 105825);
    assert.equal(asInt('0'), 0);
    assert.equal(asInt('-1'), -1);
  });

  test('returns the fallback for missing values', () => {
    assert.equal(asInt(undefined, 0), 0);
    assert.equal(asInt(null, 0), 0);
    assert.equal(asInt(undefined), null);
  });

  test('rejects booleans, which would otherwise coerce to 0/1', () => {
    assert.equal(asInt(true, 99), 99);
    assert.equal(asInt(false, 99), 99);
  });

  test('rejects non-integer text rather than producing NaN', () => {
    assert.equal(asInt('abc', -1), -1);
    assert.equal(asInt('', -1), -1);
    assert.equal(asInt('  ', -1), -1);
    assert.equal(asInt('1.5', -1), -1);
    assert.equal(asInt('1e3', -1), -1);
  });

  test('rejects objects and arrays', () => {
    assert.equal(asInt({}, -1), -1);
    assert.equal(asInt([], -1), -1);
  });

  test('rejects non-finite numbers', () => {
    assert.equal(asInt(Infinity, -1), -1);
    assert.equal(asInt(NaN, -1), -1);
  });
});

describe('b64ToText', () => {
  test('decodes base64 to utf-8 text', () => {
    assert.deepEqual(b64ToText(b64('hello')), { text: 'hello', binary: false });
  });

  test('round-trips multi-byte utf-8', () => {
    const s = 'héllo wörld — ok';
    assert.deepEqual(b64ToText(b64(s)), { text: s, binary: false });
  });

  test('returns non-base64 input unchanged', () => {
    assert.deepEqual(b64ToText('not base64!'), { text: 'not base64!', binary: false });
    assert.deepEqual(b64ToText('{"a":1}'), { text: '{"a":1}', binary: false });
  });

  test('re-encodes undecodable bytes as base64 instead of mangling them', () => {
    // 0xFF 0xFE is not valid UTF-8.
    const raw = Buffer.from([0xff, 0xfe, 0x00, 0x01]).toString('base64');
    const out = b64ToText(raw);
    assert.equal(out.binary, true);
    assert.equal(out.text, raw);
  });

  test('a genuine replacement char in the source is not misread as binary', () => {
    const s = 'literal \uFFFD here';
    const out = b64ToText(b64(s));
    // The input really did contain U+FFFD, so round-tripping it is correct
    // even though the heuristic flags it.
    assert.equal(out.binary, true);
  });

  test('handles empty string', () => {
    assert.deepEqual(b64ToText(''), { text: '', binary: false });
  });
});

describe('decodePayload', () => {
  test('null payload yields an all-empty shape', () => {
    assert.deepEqual(decodePayload(null), {
      text: null,
      encoding: null,
      sizeBytes: null,
      binary: false,
    });
    assert.deepEqual(decodePayload(undefined).text, null);
  });

  test('PAYLOAD_ENCODING_NULL means a tombstone, not text', () => {
    const out = decodePayload({ encoding: 'PAYLOAD_ENCODING_NULL', normalizedPayload: b64('x') });
    assert.equal(out.text, null);
    assert.equal(out.encoding, 'PAYLOAD_ENCODING_NULL');
  });

  test('prefers normalizedPayload over originalPayload', () => {
    const out = decodePayload({
      encoding: 'PAYLOAD_ENCODING_JSON',
      normalizedPayload: b64('{"normalized":true}'),
      originalPayload: b64('{"original":true}'),
    });
    assert.equal(out.text, '{"normalized":true}');
  });

  test('falls back to originalPayload', () => {
    const out = decodePayload({ originalPayload: b64('raw') });
    assert.equal(out.text, 'raw');
  });

  test('reads payloadSize as an int64 string', () => {
    assert.equal(decodePayload({ payloadSize: '4096' }).sizeBytes, 4096);
    assert.equal(decodePayload({ payloadSize: 12 }).sizeBytes, 12);
  });

  test('absent payload text yields null, not empty string', () => {
    assert.equal(decodePayload({ encoding: 'PAYLOAD_ENCODING_JSON' }).text, null);
    assert.equal(decodePayload({ normalizedPayload: '' }).text, null);
  });

  test('flags binary payloads', () => {
    const raw = Buffer.from([0xc3, 0x28]).toString('base64');
    assert.equal(decodePayload({ originalPayload: raw }).binary, true);
  });
});

describe('formatTimestamp', () => {
  test('renders epoch millis as ISO-8601 UTC', () => {
    assert.equal(formatTimestamp(0), '1970-01-01T00:00:00.000Z');
    assert.equal(formatTimestamp('1700000000000'), '2023-11-14T22:13:20.000Z');
  });

  test('null for missing or unparseable input', () => {
    assert.equal(formatTimestamp(undefined), null);
    assert.equal(formatTimestamp('later'), null);
  });

  test('null rather than throwing on out-of-range values', () => {
    assert.equal(formatTimestamp('99999999999999999'), null);
    assert.equal(formatTimestamp(8.64e15 + 1), null);
  });
});

describe('normalizeMessage', () => {
  test('proto3 omits zero values, so a missing partitionId means partition 0', () => {
    const out = normalizeMessage({ offset: '5' });
    assert.equal(out.partition, 0, 'missing partitionId must mean 0, not unknown');
    assert.equal(out.offset, 5);
  });

  test('a missing offset likewise means offset 0', () => {
    assert.equal(normalizeMessage({ partitionId: 3 }).offset, 0);
  });

  test('maps a full frame', () => {
    const out = normalizeMessage({
      partitionId: 2,
      offset: '105825',
      timestamp: '1700000000000',
      compression: 'COMPRESSION_TYPE_GZIP',
      key: { encoding: 'PAYLOAD_ENCODING_TEXT', normalizedPayload: b64('k1') },
      value: {
        encoding: 'PAYLOAD_ENCODING_JSON',
        normalizedPayload: b64('{"a":1}'),
        payloadSize: '7',
      },
      headers: [{ key: 'trace', value: b64('abc') }],
    });

    assert.deepEqual(out, {
      partition: 2,
      offset: 105825,
      timestamp: '2023-11-14T22:13:20.000Z',
      timestamp_ms: 1700000000000,
      key: 'k1',
      value: '{"a":1}',
      headers: [{ key: 'trace', value: 'abc', binary: false }],
      key_encoding: 'PAYLOAD_ENCODING_TEXT',
      value_encoding: 'PAYLOAD_ENCODING_JSON',
      value_size_bytes: 7,
      compression: 'COMPRESSION_TYPE_GZIP',
      value_is_binary: false,
    });
  });

  test('tolerates a completely empty frame', () => {
    const out = normalizeMessage({});
    assert.equal(out.partition, 0);
    assert.equal(out.offset, 0);
    assert.equal(out.key, null);
    assert.deepEqual(out.headers, []);
  });

  test('tolerates a non-object frame', () => {
    assert.equal(normalizeMessage(null).partition, 0);
    assert.equal(normalizeMessage('nonsense').offset, 0);
  });

  test('non-array headers degrade to an empty list', () => {
    assert.deepEqual(normalizeMessage({ headers: 'bad' }).headers, []);
    assert.deepEqual(normalizeMessage({ headers: null }).headers, []);
  });

  test('skips malformed header entries', () => {
    const out = normalizeMessage({ headers: [null, 'x', { key: 'ok', value: b64('v') }] });
    assert.deepEqual(out.headers, [{ key: 'ok', value: 'v', binary: false }]);
  });

  test('a valueless header yields null, not empty string', () => {
    const out = normalizeMessage({ headers: [{ key: 'flag' }] });
    assert.deepEqual(out.headers, [{ key: 'flag', value: null, binary: false }]);
  });

  test('tombstone value is null', () => {
    const out = normalizeMessage({ value: { encoding: 'PAYLOAD_ENCODING_NULL' } });
    assert.equal(out.value, null);
    assert.equal(out.value_encoding, 'PAYLOAD_ENCODING_NULL');
  });
});
