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
  const n = typeof value === 'string' && value.trim() !== '' ? Number(value) : value;
  if (typeof n !== 'number' || !Number.isFinite(n) || !Number.isInteger(n)) {
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

export const TOOLS = [
  {
    name: 'list_topics',
    description: 'List Kafka topics with partition count and replication factor.',
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
      },
      additionalProperties: false,
    },
  },
  {
    name: 'cluster_info',
    description: 'Get cluster health: status, version, broker and partition counts.',
    inputSchema: { type: 'object', properties: {}, additionalProperties: false },
  },
  {
    name: 'describe_topic',
    description: 'Get the effective configuration of a topic.',
    inputSchema: {
      type: 'object',
      properties: { topic: topicSchema },
      required: ['topic'],
      additionalProperties: false,
    },
  },
  {
    name: 'fetch_latest',
    description: 'Fetch the most recent messages from a topic.',
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
  },
  {
    name: 'fetch_by_time',
    description: 'Fetch messages from the first offset at or after a timestamp.',
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
  },
  {
    name: 'search_messages',
    description:
      'Find messages whose key or value contains the given text. Matching runs ' +
      "server-side in the Console's sandboxed JavaScript interpreter, so only " +
      'matching messages are transferred. Scanning starts from the oldest message ' +
      'by default; narrow the range with start_offset or start_timestamp_ms on ' +
      'high-volume topics.',
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
            'or an explicit offset.',
        },
        start_timestamp_ms: {
          type: 'integer',
          minimum: 0,
          description: 'Scan from this time instead of an offset.',
        },
        case_sensitive: { type: 'boolean', default: true },
        partition_id: partitionSchema,
      },
      required: ['topic', 'text'],
      additionalProperties: false,
    },
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
  // failure mode instead of two.
  return {
    list_topics: async (a) =>
      service.listTopics({
        nameContains: stringArg(a.name_contains, 'name_contains', { fallback: '' }),
        pageSize: intArg(a.page_size, 'page_size', {
          fallback: LIMITS.DEFAULT_PAGE_SIZE,
          min: 1,
          max: LIMITS.MAX_PAGE_SIZE,
        }),
      }),

    cluster_info: async () => service.clusterInfo(),

    describe_topic: async (a) => service.describeTopic(topic(a)),

    fetch_latest: async (a) =>
      service.fetchLatest({
        topic: topic(a),
        maxResults: maxResults(a, LIMITS.DEFAULT_MAX_RESULTS),
        partitionId: partition(a),
      }),

    fetch_by_offset: async (a) =>
      service.fetchByOffset({
        topic: topic(a),
        offset: intArg(a.offset, 'offset', { min: 0 }),
        maxResults: maxResults(a, LIMITS.DEFAULT_MAX_RESULTS),
        partitionId: partition(a),
      }),

    fetch_by_time: async (a) =>
      service.fetchByTime({
        topic: topic(a),
        timestampMs: intArg(a.timestamp_ms, 'timestamp_ms', { min: 0 }),
        maxResults: maxResults(a, LIMITS.DEFAULT_SEARCH_RESULTS),
        partitionId: partition(a),
      }),

    search_messages: async (a) =>
      service.searchMessages({
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
