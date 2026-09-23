// Business logic layered over `console.mjs`.
//
// This module knows the Console's API shapes and constraints -- endpoint
// paths, the `startOffset` sentinel values, the 500-message server cap -- and
// exposes them as ordinary async functions returning plain objects and arrays.
// Keeping this separate from the MCP tool definitions makes the behaviour
// directly testable without an MCP client in the loop.

import { MAX_RESULTS_LIMIT, LIMITS } from './config.mjs';
import { buildContainsFilter, encodeFilter } from './filters.mjs';
import { normalizeMessage } from './models.mjs';

const LIST_MESSAGES = '/redpanda.api.console.v1alpha1.ConsoleService/ListMessages';
const KAFKA_INFO = '/redpanda.api.console.v1alpha1.ClusterStatusService/GetKafkaInfo';
const TOPICS = '/v1/topics';

export const ALL_PARTITIONS = -1;

/** Sentinel values accepted by the Console's `startOffset` field. */
export const StartOffset = Object.freeze({
  RECENT: -1, // Newest N messages, walking backwards from the high watermark.
  OLDEST: -2, // From the beginning of the topic.
  NEWEST: -3, // Tail live messages produced from now on.
  TIMESTAMP: -4, // From the first offset at or after `startTimestamp`.
});

/** Raised for caller mistakes: surfaced to the model as a tool error. */
export class ValidationError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ValidationError';
  }
}

function requireInteger(value, name) {
  if (typeof value !== 'number' || !Number.isFinite(value) || !Number.isInteger(value)) {
    throw new ValidationError(`${name} must be an integer (got ${JSON.stringify(value)}).`);
  }
  return value;
}

function validateMaxResults(maxResults) {
  requireInteger(maxResults, 'max_results');
  if (maxResults < 1) {
    throw new ValidationError(`max_results must be at least 1 (got ${maxResults}).`);
  }
  if (maxResults > MAX_RESULTS_LIMIT) {
    throw new ValidationError(
      `max_results must not exceed ${MAX_RESULTS_LIMIT} (got ${maxResults}). ` +
        'The Console aborts the response stream above this limit; page ' +
        'through the topic with fetch_by_offset instead.',
    );
  }
  return maxResults;
}

function validatePartition(partitionId) {
  requireInteger(partitionId, 'partition_id');
  if (partitionId < ALL_PARTITIONS) {
    throw new ValidationError(
      `partition_id must be -1 (all partitions) or a non-negative ` +
        `partition number (got ${partitionId}).`,
    );
  }
  return partitionId;
}

function requireTopic(topic) {
  if (typeof topic !== 'string' || !topic.trim()) {
    throw new ValidationError('topic must be a non-empty topic name.');
  }
  return topic;
}

/** High-level operations against a Redpanda Console instance. */
export class ConsoleService {
  /** @param {import('./console.mjs').ConsoleClient} client */
  constructor(client) {
    this.client = client;
  }

  /** List topics, optionally filtered by a substring of the name. */
  async listTopics({ nameContains = '', pageSize = LIMITS.DEFAULT_PAGE_SIZE } = {}) {
    requireInteger(pageSize, 'page_size');
    if (pageSize < 1) {
      throw new ValidationError(`page_size must be at least 1 (got ${pageSize}).`);
    }
    if (pageSize > LIMITS.MAX_PAGE_SIZE) {
      throw new ValidationError(
        `page_size must not exceed ${LIMITS.MAX_PAGE_SIZE} (got ${pageSize}).`,
      );
    }

    const params = { page_size: pageSize };
    if (nameContains) params['filter.name_contains'] = nameContains;

    const payload = await this.client.getJson(TOPICS, params);
    const isObject = payload !== null && typeof payload === 'object' && !Array.isArray(payload);
    const topics = isObject && Array.isArray(payload.topics) ? payload.topics : [];
    return {
      topics,
      count: topics.length,
      next_page_token: isObject ? (payload.next_page_token ?? null) : null,
    };
  }

  /** Return Kafka cluster health, broker counts and version. */
  async clusterInfo() {
    return this.client.unaryRpc(KAFKA_INFO, {});
  }

  /** Return the effective configuration of a single topic. */
  async describeTopic(topic) {
    requireTopic(topic);
    // Percent-encode the path segment so a topic name containing '/' or '%'
    // cannot break out of the URL path.
    return this.client.getJson(`/v1/topics/${encodeURIComponent(topic)}/configurations`);
  }

  /**
   * Consume messages and return them normalised.
   *
   * Streaming frames are consumed lazily and the loop exits as soon as
   * `maxResults` messages have been collected, so an over-eager server cannot
   * force us to buffer more than the caller asked for.
   */
  async fetchMessages({
    topic,
    startOffset,
    maxResults,
    partitionId = ALL_PARTITIONS,
    startTimestampMs = null,
    filterCode = null,
  }) {
    requireTopic(topic);
    validateMaxResults(maxResults);
    validatePartition(partitionId);
    requireInteger(startOffset, 'start_offset');

    const request = {
      topic,
      startOffset,
      partitionId,
      maxResults,
    };
    if (startTimestampMs !== null && startTimestampMs !== undefined) {
      request.startTimestamp = startTimestampMs;
    }
    if (filterCode) {
      request.filterInterpreterCode = encodeFilter(filterCode);
    }

    const messages = [];
    const stream = this.client.streamRpc(LIST_MESSAGES, request);
    for await (const frame of stream) {
      const data = frame.data;
      if (data !== null && typeof data === 'object' && !Array.isArray(data)) {
        messages.push(normalizeMessage(data));
        if (messages.length >= maxResults) {
          // Tell the generator to run its cleanup so the socket is released.
          await stream.return?.();
          break;
        }
      }
    }
    return messages;
  }

  async fetchLatest({ topic, maxResults = LIMITS.DEFAULT_MAX_RESULTS, partitionId = ALL_PARTITIONS }) {
    return this.fetchMessages({
      topic,
      startOffset: StartOffset.RECENT,
      maxResults,
      partitionId,
    });
  }

  async fetchByOffset({
    topic,
    offset,
    maxResults = LIMITS.DEFAULT_MAX_RESULTS,
    partitionId = ALL_PARTITIONS,
  }) {
    requireInteger(offset, 'offset');
    if (offset < 0) {
      throw new ValidationError(
        `offset must be non-negative (got ${offset}). To start from the ` +
          'beginning or the end of a topic use fetch_latest, or search_messages ' +
          "which exposes the 'oldest', 'newest' and 'recent' start positions.",
      );
    }
    return this.fetchMessages({ topic, startOffset: offset, maxResults, partitionId });
  }

  async fetchByTime({
    topic,
    timestampMs,
    maxResults = LIMITS.DEFAULT_SEARCH_RESULTS,
    partitionId = ALL_PARTITIONS,
  }) {
    requireInteger(timestampMs, 'timestamp_ms');
    if (timestampMs < 0) {
      throw new ValidationError(
        `timestamp_ms must be a non-negative epoch value in milliseconds (got ${timestampMs}).`,
      );
    }
    return this.fetchMessages({
      topic,
      startOffset: StartOffset.TIMESTAMP,
      startTimestampMs: timestampMs,
      maxResults,
      partitionId,
    });
  }

  /**
   * Find messages whose key or value contains `text`.
   *
   * Matching runs inside the Console's sandboxed JS interpreter, so only
   * matching messages cross the network.
   */
  async searchMessages({
    topic,
    text,
    maxResults = LIMITS.DEFAULT_SEARCH_RESULTS,
    startOffset = StartOffset.OLDEST,
    startTimestampMs = null,
    partitionId = ALL_PARTITIONS,
    caseSensitive = true,
  }) {
    let effectiveOffset = startOffset;
    if (startTimestampMs !== null && startTimestampMs !== undefined) {
      requireInteger(startTimestampMs, 'start_timestamp_ms');
      if (startTimestampMs < 0) {
        throw new ValidationError(
          `start_timestamp_ms must be a non-negative epoch value in milliseconds (got ${startTimestampMs}).`,
        );
      }
      effectiveOffset = StartOffset.TIMESTAMP;
    }

    let filterCode;
    try {
      filterCode = buildContainsFilter(text, { caseSensitive });
    } catch (e) {
      // Filter construction rejects empty/oversized needles; report as validation.
      throw new ValidationError(e.message);
    }

    return this.fetchMessages({
      topic,
      startOffset: effectiveOffset,
      startTimestampMs,
      maxResults,
      partitionId,
      filterCode,
    });
  }
}
