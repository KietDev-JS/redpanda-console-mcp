# Setup

## 1. Check connectivity first

Wire up an MCP client only after you know the Console is reachable. A failed MCP
server is much harder to diagnose from inside a chat window than from a shell.

```bash
curl -s "https://console.example.com/v1/topics?page_size=1"
```

You want JSON back. Common answers and what they mean:

| Response | Meaning |
|---|---|
| `{"topics":[...]}` | Good. |
| HTML (a login page) | The URL is behind an SSO proxy; the MCP server cannot authenticate through it. |
| `404` | The base URL is probably missing a subpath, or is not a Console. |
| `401` / `403` | Credentials required — set `CONSOLE_API_KEY`. |
| Connection refused / DNS failure | Wrong host, or you need to be on a VPN. |

If the Console sits behind a reverse proxy on a subpath, include it:
`CONSOLE_BASE_URL=https://proxy.example.com/redpanda`.

## 2. Check the server starts

```bash
CONSOLE_BASE_URL=https://console.example.com npx -y redpanda-console-mcp
```

It should sit there silently waiting for JSON-RPC on stdin. That is correct — it
speaks stdio, not HTTP. `Ctrl+C` to exit.

Without configuration it exits immediately with status 2 and names the missing
variable.

On Windows PowerShell:

```powershell
$env:CONSOLE_BASE_URL="https://console.example.com"; npx -y redpanda-console-mcp
```

To confirm it answers, pipe one request in:

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' \
  | CONSOLE_BASE_URL=https://console.example.com npx -y redpanda-console-mcp
```

You should get a JSON line listing seven tools.

## 3. Configure your client

### Claude Desktop

`%APPDATA%\Claude\claude_desktop_config.json` (Windows) or
`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS):

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

Restart Claude Desktop completely — it does not reload config on the fly.

### Claude Code

`~/.claude.json`, same `mcpServers` shape as above.

### opencode

`~/.config/opencode/opencode.json`:

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

Note the key differences from the Claude format: `command` is an array, and the
environment key is `environment`, not `env`.

### Cursor

`.cursor/mcp.json` in the project, or `~/.cursor/mcp.json` globally — same shape
as the Claude Desktop config.

### Pinning a version

`npx -y redpanda-console-mcp` resolves the latest release. To pin:

```json
{ "args": ["-y", "redpanda-console-mcp@1.0.0"] }
```

## Troubleshooting

**The server does not appear in the client.**
Check the client's MCP logs. For Claude Desktop, `%APPDATA%\Claude\logs\`. The
most common cause is `npx` not being on the PATH the client sees — use an
absolute path to `node` and the installed script if so.

**"CONSOLE_BASE_URL is not set" although you set it.**
Environment variables exported in your shell are not inherited by a GUI app
launched from the desktop. Put the variable in the client's config file.

**Every call returns 401 or 403.**
Set `CONSOLE_API_KEY`. If your Console uses SSO rather than API keys, this server
cannot authenticate to it.

**Calls time out on a large topic.**
Lower `max_results`, or narrow the scan with `start_offset` / `start_timestamp_ms`.
Searching from the oldest offset on a high-volume topic means the Console scans
the whole topic. Raise `CONSOLE_TIMEOUT` if you genuinely need long scans.

**"max_results must not exceed 500".**
That is the Console's own streaming limit, not an arbitrary one. Page with
`fetch_by_offset`, passing an explicit `partition_id` so paging is deterministic.

**Self-signed certificate errors.**
`CONSOLE_VERIFY_TLS=false` works but disables verification for the whole process.
Prefer adding your CA to the system trust store, or point `NODE_EXTRA_CA_CERTS` at
the CA bundle.

**Messages come back as base64 with `value_is_binary: true`.**
The payload is not valid UTF-8 — typically Protobuf or Avro without a schema
registered in the Console. Register the schema and the Console will return a
decoded `normalizedPayload`.
