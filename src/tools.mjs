// MCP tool declarations and their argument coercion.
//
// JSON Schema constrains what a well-behaved client sends, but a tool call is
// ultimately arbitrary JSON from a model, so every argument is re-validated
// here rather than trusted.

import { MAX_RESULTS_LIMIT, LIMITS } from './config.mjs';
import { ALL_PARTITIONS, StartOffset } from './service.mjs';

export class ToolError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ToolError';
  }
}

/**
 * Coerce an argument to an integer.
 *
 * Rejects fractional and non-numeric input rather than truncating: a caller
 * that asks for 2.7 messages has a bug, and silently rounding hides it.
 */
export function intArg(value, name, { fallback, min, max } = {}) {
  if (value === undefined || value === null || value === '') {
    if (fallback === undefined) throw new ToolError(`${name} is required.`);
    return fallback;
  }
  const n = typeof value === 'string' && /^\s*[+-]?\d+\s*$/.test(value) ? Number(value) : value;
  // Beyond the safe range a number silently rounds to a different offset.
  if (typeof n !== 'number' || !Number.isSafeInteger(n)) {
    throw new ToolError(`${name} must be an integer (got ${JSON.stringify(value)}).`);
  }
  if (min !== undefined && n < min) {
    throw new ToolError(`${name} must be at least ${min} (got ${n}).`);
  }
  if (max !== undefined && n > max) {
    throw new ToolError(`${name} must not exceed ${max} (got ${n}).`);
  }
  return n;
}

export function stringArg(value, name, { fallback, required = false } = {}) {
  if (value === undefined || value === null) {
    if (required) throw new ToolError(`${name} is required.`);
    return fallback;
  }
  if (typeof value !== 'string') {
    throw new ToolError(`${name} must be a string (got ${JSON.stringify(value)}).`);
  }
  if (required && !value.trim()) throw new ToolError(`${name} must not be empty.`);
  return value;
}

export function boolArg(value, name, fallback) {
  if (value === undefined || value === null) return fallback;
  if (typeof value === 'boolean') return value;
  throw new ToolError(`${name} must be a boolean (got ${JSON.stringify(value)}).`);
}

const partitionSchema = {
  type: 'integer',
  minimum: -1,
  default: ALL_PARTITIONS,
  description: 'Partition to read from, or -1 for all partitions.',
};

const maxResultsSchema = (dflt) => ({
  type: 'integer',
  minimum: 1,
  maximum: MAX_RESULTS_LIMIT,
  default: dflt,
  description: `Maximum messages to return (1-${MAX_RESULTS_LIMIT}).`,
});

const topicSchema = { type: 'string', description: 'Kafka topic name.' };

/**
 * MCP tool annotations. Every tool only reads: consuming through the Console
 * uses no consumer group and commits no offsets, so nothing about the cluster
 * changes. Clients use readOnlyHint to skip confirmation prompts.
 * idempotentHint is false because "latest" and live-tail results change as
 * messages are produced.
 */
const READ_ONLY = Object.freeze({
  readOnlyHint: true,
  destructiveHint: false,
  idempotentHint: false,
  openWorldHint: true,
});

export const TOOLS = [
  {
    name: 'list_topics',
    description:
      'List Kafka topics with partition count and replication factor. Returns ' +
      '{ topics, count, next_page_token }. When next_page_token is not null, pass it ' +
      'back as page_token to get the next page.',
    inputSchema: {
      type: 'object',
      properties: {
        name_contains: {
          type: 'string',
          default: '',
          description: 'Case-sensitive substring filter on the topic name.',
        },
        page_size: {
          type: 'integer',
          minimum: 1,
          maximum: LIMITS.MAX_PAGE_SIZE,
          default: LIMITS.DEFAULT_PAGE_SIZE,
          description: 'Maximum topics to return.',
        },
        page_token: {
          type: 'string',
          description: 'next_page_token from the previous list_topics result.',
        },
      },
      additionalProperties: false,
    },
    annotations: READ_ONLY,
  },
  {
    name: 'cluster_info',
    description: 'Get cluster health: status, version, broker and partition counts.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
    annotations: READ_ONLY,
  },
  {
    name: 'describe_topic',
    description:
      'Get the effective configuration of a topic, such as retention and cleanup policy. ' +
      'Use list_topics to find topic names.',
    inputSchema: {
      type: 'object',
      properties: { topic: topicSchema },
      required: ['topic'],
      additionalProperties: false,
    },
    annotations: READ_ONLY,
  },
  {
    name: 'fetch_latest',
    description:
      'Fetch the most recent messages from a topic. Returns an array of messages with ' +
      'partition, offset, timestamp, key, value and headers. If the stream stops early ' +
      'the result is { messages, incomplete }; if it is too large it includes truncated, ' +
      'with resume_from offsets for fetch_by_offset.',
    inputSchema: {
      type: 'object',
      properties: {
        topic: topicSchema,
        max_results: maxResultsSchema(LIMITS.DEFAULT_MAX_RESULTS),
        partition_id: partitionSchema,
      },
      required: ['topic'],
      additionalProperties: false,
    },
    annotations: READ_ONLY,
  },
  {
    name: 'fetch_by_offset',
    description:
      'Fetch messages starting at an offset, reading forwards. With partition_id=-1 ' +
      'the offset is applied to every partition, so pass an explicit partition to ' +
      'page deterministically through a topic.',
    inputSchema: {
      type: 'object',
      properties: {
        topic: topicSchema,
        offset: { type: 'integer', minimum: 0, description: 'Starting offset, inclusive.' },
        max_results: maxResultsSchema(LIMITS.DEFAULT_MAX_RESULTS),
        partition_id: partitionSchema,
      },
      required: ['topic', 'offset'],
      additionalProperties: false,
    },
    annotations: READ_ONLY,
  },
  {
    name: 'fetch_by_time',
    description:
      'Fetch messages from the first offset at or after a timestamp, reading forwards. ' +
      'Returns the same shape as fetch_latest.',
    inputSchema: {
      type: 'object',
      properties: {
        topic: topicSchema,
        timestamp_ms: {
          type: 'integer',
          minimum: 0,
          description: 'Unix epoch time in milliseconds.',
        },
        max_results: maxResultsSchema(LIMITS.DEFAULT_SEARCH_RESULTS),
        partition_id: partitionSchema,
      },
      required: ['topic', 'timestamp_ms'],
      additionalProperties: false,
    },
    annotations: READ_ONLY,
  },
  {
    name: 'search_messages',
    description:
      'Find messages whose key or value contains the given text. Matching runs ' +
      "server-side in the Console's sandboxed JavaScript interpreter, so only " +
      'matching messages are transferred. Scanning starts from the oldest message ' +
      'by default; narrow the range with start_offset or start_timestamp_ms on ' +
      'high-volume topics. Returns the same shape as fetch_latest; an empty array means ' +
      'nothing matched in the scanned range.',
    inputSchema: {
      type: 'object',
      properties: {
        topic: topicSchema,
        text: { type: 'string', minLength: 1, description: 'Substring to find.' },
        max_results: maxResultsSchema(LIMITS.DEFAULT_SEARCH_RESULTS),
        start_offset: {
          type: 'integer',
          minimum: -4,
          default: StartOffset.OLDEST,
          description:
            'Where to start scanning: -1 recent, -2 oldest (default), -3 newest/live, ' +
            '-4 from start_timestamp_ms, or an explicit offset.',
        },
        start_timestamp_ms: {
          type: 'integer',
          minimum: 0,
          description:
            'Scan from this Unix epoch time in milliseconds. Takes precedence over start_offset.',
        },
        case_sensitive: {
          type: 'boolean',
          default: true,
          description: 'Set false to ignore case when matching.',
        },
        partition_id: partitionSchema,
      },
      required: ['topic', 'text'],
      additionalProperties: false,
    },
    annotations: READ_ONLY,
  },
];

/**
 * Build the tool implementations bound to a service.
 *
 * @param {import('./service.mjs').ConsoleService} service
 */
export function createHandlers(service) {
  const partition = (a) =>
    intArg(a.partition_id, 'partition_id', { fallback: ALL_PARTITIONS, min: -1 });
  const maxResults = (a, dflt) =>
    intArg(a.max_results, 'max_results', { fallback: dflt, min: 1, max: MAX_RESULTS_LIMIT });
  const topic = (a) => stringArg(a.topic, 'topic', { required: true });

  // Every handler is async so that argument validation rejects rather than
  // throwing synchronously. A caller awaiting the result then sees one
  // failure mode instead of two. `signal` cancels the upstream request.
  return {
    list_topics: async (a, { signal } = {}) =>
      service.listTopics({
        nameContains: stringArg(a.name_contains, 'name_contains', { fallback: '' }),
        pageSize: intArg(a.page_size, 'page_size', {
          fallback: LIMITS.DEFAULT_PAGE_SIZE,
          min: 1,
          max: LIMITS.MAX_PAGE_SIZE,
        }),
        pageToken: stringArg(a.page_token, 'page_token', { fallback: '' }),
        signal,
      }),

    cluster_info: async (_a, { signal } = {}) => service.clusterInfo({ signal }),

    describe_topic: async (a, { signal } = {}) => service.describeTopic(topic(a), { signal }),

    fetch_latest: async (a, { signal } = {}) =>
      service.fetchLatest({
        topic: topic(a),
        maxResults: maxResults(a, LIMITS.DEFAULT_MAX_RESULTS),
        partitionId: partition(a),
        signal,
      }),

    fetch_by_offset: async (a, { signal } = {}) =>
      service.fetchByOffset({
        topic: topic(a),
        offset: intArg(a.offset, 'offset', { min: 0 }),
        maxResults: maxResults(a, LIMITS.DEFAULT_MAX_RESULTS),
        partitionId: partition(a),
        signal,
      }),

    fetch_by_time: async (a, { signal } = {}) =>
      service.fetchByTime({
        topic: topic(a),
        timestampMs: intArg(a.timestamp_ms, 'timestamp_ms', { min: 0 }),
        maxResults: maxResults(a, LIMITS.DEFAULT_SEARCH_RESULTS),
        partitionId: partition(a),
        signal,
      }),

    search_messages: async (a, { signal } = {}) =>
      service.searchMessages({
        signal,
        topic: topic(a),
        text: stringArg(a.text, 'text', { required: true }),
        maxResults: maxResults(a, LIMITS.DEFAULT_SEARCH_RESULTS),
        startOffset: intArg(a.start_offset, 'start_offset', {
          fallback: StartOffset.OLDEST,
          min: -4,
        }),
        startTimestampMs:
          a.start_timestamp_ms === undefined || a.start_timestamp_ms === null
            ? null
            : intArg(a.start_timestamp_ms, 'start_timestamp_ms', { min: 0 }),
        partitionId: partition(a),
        caseSensitive: boolArg(a.case_sensitive, 'case_sensitive', true),
      }),
  };
}
