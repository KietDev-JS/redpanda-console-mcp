# redpanda-console-mcp

[![CI](https://github.com/KietDev-JS/redpanda-console-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/KietDev-JS/redpanda-console-mcp/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/redpanda-console-mcp)](https://www.npmjs.com/package/redpanda-console-mcp)
[![node](https://img.shields.io/node/v/redpanda-console-mcp)](https://nodejs.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

An [MCP](https://modelcontextprotocol.io) server that lets an AI assistant read and
search Kafka messages through [Redpanda Console](https://www.redpanda.com/redpanda-console-kafka-ui).

Zero runtime dependencies. Node 18.17+.

## What it does

Point it at a Redpanda Console instance and your assistant can answer questions
like *"what did the last 20 messages on `orders` look like?"* or *"find the message
mentioning order-4821"* without you opening the UI.

Message search runs **server-side**, inside the Console's sandboxed JavaScript
interpreter, so only matching messages cross the network.

## Tools

| Tool | Purpose |
|---|---|
| `list_topics` | List topics with partition count and replication factor |
| `cluster_info` | Cluster health: status, version, broker and partition counts |
| `describe_topic` | Effective configuration of one topic |
| `fetch_latest` | Most recent messages from a topic |
| `fetch_by_offset` | Messages from an offset, reading forwards |
| `fetch_by_time` | Messages from the first offset at or after a timestamp |
| `search_messages` | Messages whose key or value contains a substring |

## Install

```bash
npx redpanda-console-mcp        # no install
npm install -g redpanda-console-mcp
```

## Configuration

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `CONSOLE_BASE_URL` | yes | — | Console address, e.g. `https://console.example.com` |
| `CONSOLE_API_KEY` | no | *(none)* | Sent as `Authorization: Bearer <key>` |
| `CONSOLE_TIMEOUT` | no | `60` | Request timeout in seconds |
| `CONSOLE_VERIFY_TLS` | no | `true` | Set `false` to skip TLS verification |

There is deliberately no default base URL: pointing a Kafka client at a guessed
host is worse than refusing to start. Without `CONSOLE_BASE_URL` the server exits
with status 2 and says so.

## Client setup

Claude Desktop (`claude_desktop_config.json`) or Claude Code (`~/.claude.json`):

```json
{
  "mcpServers": {
    "redpanda-console": {
      "command": "npx",
      "args": ["-y", "redpanda-console-mcp"],
      "env": { "CONSOLE_BASE_URL": "https://console.example.com" }
    }
  }
}
```

opencode (`opencode.json`):

```json
{
  "mcp": {
    "redpanda-console": {
      "type": "local",
      "command": ["npx", "-y", "redpanda-console-mcp"],
      "enabled": true,
      "environment": { "CONSOLE_BASE_URL": "https://console.example.com" }
    }
  }
}
```

See [docs/SETUP.md](docs/SETUP.md) for a connectivity check to run *before* wiring
up a client, plus troubleshooting.

## Limits

- `max_results` is capped at **500**. The Console aborts its response stream above
  that; page through a topic with `fetch_by_offset` instead.
- `page_size` for `list_topics` is capped at 1000.
- Search text is capped at 4096 characters.
- A single tool result is capped at ~200 KB. Oversized results shed whole
  messages and report the truncation rather than emitting broken JSON.

## Notes on behaviour

A few Console details this server absorbs so the model does not have to:

- **proto3 omits zero values.** A missing `partitionId` means partition 0, not
  "unknown". Defaulting it to null would mislabel every message on partition 0.
- **64-bit integers arrive as strings.** `offset` and `timestamp` come back as
  `"105825"`; they are parsed to numbers.
- **Unary and streaming Connect RPC differ.** Unary methods need plain
  `application/json` — sending the streaming content type yields HTTP 415.
  Streaming methods use 5-byte length-prefixed envelopes, which routinely
  straddle TCP chunk boundaries.
- **Binary payloads are not mangled.** Values that are not valid UTF-8 are
  returned as base64 and flagged with `value_is_binary`.
- **`partition_id: -1` reads every partition**, so an offset is applied per
  partition. Pass an explicit partition to page deterministically.

## Security

This server only reads. It never produces, deletes, or alters configuration.

It cannot, however, reduce the privileges of the credential you give it — use a
read-only Console user. Search terms are embedded as JSON string literals, so
they cannot escape into executable code in the Console's interpreter.

`CONSOLE_VERIFY_TLS=false` disables certificate verification for the whole
process and prints a warning. Use it only against a host you control.

## Development

```bash
npm test                 # 212 tests, no network access
npm run test:coverage
```

## License

MIT
