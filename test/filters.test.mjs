import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { buildContainsFilter, encodeFilter, MAX_FILTER_LENGTH } from '../src/filters.mjs';

/**
 * Evaluate a generated filter body the way the Console's goja VM would:
 * as the body of `function (key, value) { ... }`.
 */
function runFilter(body, key, value) {
  // eslint-disable-next-line no-new-func
  return new Function('key', 'value', body)(key, value);
}

describe('buildContainsFilter', () => {
  test('matches on the value', () => {
    const f = buildContainsFilter('needle');
    assert.equal(runFilter(f, null, 'a needle here'), true);
    assert.equal(runFilter(f, null, 'nothing'), false);
  });

  test('matches on the key', () => {
    const f = buildContainsFilter('abc');
    assert.equal(runFilter(f, 'xx-abc-xx', 'unrelated'), true);
  });

  test('searches inside JSON objects by stringifying them', () => {
    const f = buildContainsFilter('order-42');
    assert.equal(runFilter(f, null, { id: 'order-42', ok: true }), true);
    assert.equal(runFilter(f, null, { id: 'order-43' }), false);
  });

  test('null and undefined payloads do not throw, because a throw aborts the consume', () => {
    const f = buildContainsFilter('x');
    assert.doesNotThrow(() => runFilter(f, null, null));
    assert.doesNotThrow(() => runFilter(f, undefined, undefined));
    assert.equal(runFilter(f, null, null), false);
  });

  test('a circular payload degrades instead of throwing', () => {
    const circular = { a: 1 };
    circular.self = circular;
    const f = buildContainsFilter('object');
    assert.doesNotThrow(() => runFilter(f, null, circular));
  });

  test('case-insensitive matching folds both sides', () => {
    const f = buildContainsFilter('NeEdLe', { caseSensitive: false });
    assert.equal(runFilter(f, null, 'a NEEDLE here'), true);
    assert.equal(runFilter(f, null, 'a needle here'), true);
  });

  test('case-sensitive is the default', () => {
    const f = buildContainsFilter('Needle');
    assert.equal(runFilter(f, null, 'needle'), false);
  });

  test('a quote in the search text cannot break out of the string literal', () => {
    const f = buildContainsFilter('say "hi"');
    assert.equal(runFilter(f, null, 'they say "hi" often'), true);
    assert.equal(runFilter(f, null, 'nope'), false);
  });

  test('an injection attempt is treated as literal text, not code', () => {
    const attack = '"; globalThis.__pwned = true; var x = "';
    const f = buildContainsFilter(attack);
    delete globalThis.__pwned;
    assert.equal(runFilter(f, null, 'harmless'), false);
    assert.equal(globalThis.__pwned, undefined, 'filter must not execute injected code');
    // And it still matches the literal string.
    assert.equal(runFilter(f, null, `prefix ${attack} suffix`), true);
  });

  test('backslashes survive as data', () => {
    const f = buildContainsFilter('C:\\path\\to');
    assert.equal(runFilter(f, null, 'at C:\\path\\to\\file'), true);
  });

  test('newlines and control characters are escaped', () => {
    const f = buildContainsFilter('line1\nline2');
    assert.equal(runFilter(f, null, 'x line1\nline2 y'), true);
  });

  test('line separators are escaped so the snippet stays one statement', () => {
    const f = buildContainsFilter('a\u2028b');
    assert.equal(f.includes('\u2028'), false, 'raw U+2028 must not appear in source');
    assert.equal(runFilter(f, null, 'x a\u2028b y'), true);
  });

  test('a script-close sequence is escaped', () => {
    const f = buildContainsFilter('</script>');
    assert.equal(f.includes('</script>'), false);
    assert.equal(runFilter(f, null, 'a </script> b'), true);
  });

  test('unicode search terms work', () => {
    const f = buildContainsFilter('日本語');
    assert.equal(runFilter(f, null, 'テスト 日本語 テスト'), true);
  });

  test('rejects empty text', () => {
    assert.throws(() => buildContainsFilter(''), /must not be empty/);
  });

  test('rejects oversized text before it reaches the remote VM', () => {
    assert.throws(() => buildContainsFilter('x'.repeat(MAX_FILTER_LENGTH + 1)), /too long/);
    assert.doesNotThrow(() => buildContainsFilter('x'.repeat(MAX_FILTER_LENGTH)));
  });

  test('the key and value are joined by a newline, so a match cannot span them', () => {
    const f = buildContainsFilter('keyvalue');
    assert.equal(runFilter(f, 'key', 'value'), false);
  });
});

describe('encodeFilter', () => {
  test('base64-encodes utf-8 source', () => {
    assert.equal(encodeFilter('return true;'), Buffer.from('return true;').toString('base64'));
  });

  test('round-trips multi-byte characters', () => {
    const code = "return '日本語';";
    assert.equal(Buffer.from(encodeFilter(code), 'base64').toString('utf-8'), code);
  });

  test('output is base64 with no newlines', () => {
    const encoded = encodeFilter(buildContainsFilter('x'));
    assert.match(encoded, /^[A-Za-z0-9+/]+=*$/);
  });
});
