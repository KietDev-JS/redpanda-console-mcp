# Improvements

> Snapshot from the review before the 1.2.0 release. Items marked as open may since have been resolved.

Review of `redpanda-console-mcp` 1.1.0, carried out on 2026-11-18. Nothing here has
been committed, pushed or published, and the package version is unchanged.

## What could not be verified

- **No real Redpanda Console was contacted**, as required. All upstream behaviour is
  mocked, so these assumptions about the Console API still need a live check:
  - `GET /v1/topics` accepts a `page_token` query parameter.
  - `ListMessages` reports failures in a frame shaped `{ "error": { "message": ... } }`.
  - Streaming responses use a `application/connect+json` content type and always end
    with an end-of-stream frame.
- **Only Node 26.7.0 on Windows was tested.** The package declares Node 18.17+ and CI
  covers Node 18, 20, 22 and 24 on three operating systems, but no other Node version
  was installed here and the GitHub workflow was not run.
- **`npx` from the registry was not tested**, because the changes are unpublished.
  `npm pack --dry-run` was run instead.

## Ranked ideas

Value and effort are High, Medium or Low. Risk is the chance of breaking existing users.

### Implemented

| # | Improvement | Area | Value | Effort | Risk |
|---|---|---|---|---|---|
| 1 | `list_topics` accepts `page_token`. It returned a token before, but no argument could use it, so topics past page one were unreachable | Tool design | High | Low | Low |
| 2 | `ListMessages` error frames raise an error. They were ignored, so a failed search looked like "no matches" | Reliability | High | Low | Low |
| 3 | Topic names `.` and `..` are rejected. `describe_topic("..")` was requesting `/v1/configurations` | Security | High | Low | Low |
| 4 | Error text is scrubbed of the API key, tokens and URL credentials, including `incomplete.reason` and startup errors | Security | High | Low | Low |
| 5 | `notifications/cancelled` aborts the upstream request and sends no response | Reliability | High | Medium | Low |
| 6 | Network failures and HTTP 429, 502, 503 and 504 are retried with backoff and `Retry-After`, inside one overall deadline | Reliability | High | Medium | Low |
| 7 | Oversized results give `truncated.resume_from` offsets, and a single huge message is clipped and flagged instead of returning nothing | Output size | High | Medium | Low |
| 8 | Result shedding is linear. It re-serialized the whole page after every removed message | Reliability | Medium | Low | Low |
| 9 | The size cap counts bytes rather than characters, and a shed topic list reports its real `count` | Output size | Medium | Low | Low |
| 10 | A stream that closes without its end-of-stream frame is reported as `incomplete` instead of passing as complete | Reliability | Medium | Low | Low |
| 11 | HTML responses and impossible frame lengths on a stream fail at once with advice, with frames capped at 64 MiB | Reliability | Medium | Low | Low |
| 12 | HTTP, Connect, timeout and network errors say what to do next, and HTML error pages are summarised | Tool design | Medium | Low | Low |
| 13 | JSON-RPC notifications are never answered or executed, and non-object messages get `-32600` | Protocol | Medium | Low | Low |
| 14 | `search_messages` requires `start_timestamp_ms` with `start_offset: -4` | Validation | Medium | Low | Low |
| 15 | Integer arguments reject unsafe values and forms such as `"1e3"`, and topic names over 249 characters are rejected | Validation | Low | Low | Low |
| 16 | `CONSOLE_TIMEOUT` rejects values that overflow Node's timer | Validation | Low | Low | Low |
| 17 | Credentials in `CONSOLE_BASE_URL` trigger a warning, because they were silently ignored | Security | Low | Low | Low |
| 18 | Binary keys are flagged with `key_is_binary: true` | Tool design | Low | Low | Low |
| 19 | Tool descriptions explain result shapes, paging and the `-4` start position | Tool design | Medium | Low | Low |
| 20 | README, setup guide, `.env.example` and `--help` cover the new behaviour | Documentation | Medium | Low | Low |
| 21 | End-to-end stdio tests run the real binary against a loopback mock Console | Test coverage | Medium | Medium | Low |

### Not implemented

| # | Idea | Area | Value | Effort | Risk | Why it is deferred |
|---|---|---|---|---|---|---|
| A | Check the assumptions listed above against a real Console | Reliability | High | Low | Low | Needs a live system |
| B | Report scan statistics from the `done` frame, so "no matches" says how much was scanned | Tool design | High | Medium | Medium | Needs a live check, and array results have no room for metadata |
| C | Add `structuredContent` and `outputSchema` to tool results | Tool design | Medium | Medium | Low | Should follow a decision on a stable result shape |
| D | Flag values the Console withheld as too large (`isPayloadTooLarge`) | Tool design | Medium | Low | Low | Field name needs a live check |
| E | Add optional redaction of message content by field name | Security | Medium | Medium | Medium | Changes the data returned, so it needs an owner decision |
| F | Add a topic allowlist or denylist through configuration | Security | Medium | Medium | Low | New configuration surface |
| G | Add a `fields` or `max_value_chars` argument to reduce output | Output size | Medium | Medium | Low | New API that deserves its own design |
| H | Export `./tools` and `./package.json` from the package | Packaging | Low | Low | Low | No current consumer needs it |
| I | Add an npm provenance publishing workflow | Packaging | Low | Medium | Low | Touches release permissions |

## Behaviour changes to review

Tool names and existing arguments are unchanged, but these results differ from 1.1.0:

- Transient failures are retried twice by default. Set `CONSOLE_MAX_RETRIES=0` for the old behaviour.
- A stream cut off before its end-of-stream frame returns `{ messages, incomplete }` rather than a plain array.
- A `ListMessages` error frame is now a tool error rather than an empty or short result.
- Integer strings such as `"1e3"` and `"0x10"` are now rejected.
- The `truncated` object has extra fields, and a clipped message has `value_truncated` and `value_total_chars`.

Closing stdin deliberately does not cancel running calls, because the setup guide and CI
pipe a single request into the server and read the answer.

A stream that closes cleanly with no frames at all still counts as an empty result,
because an existing test requires that behaviour.

## Verification results

All commands ran locally on Node 26.7.0 and Windows 11.

| Check | Command | Result |
|---|---|---|
| Baseline before changes | `npm test` | 229 passed, 0 failed |
| Full suite after changes | `node --test` | **331 passed, 0 failed**, repeated 3 times |
| Coverage | `node --test --experimental-test-coverage` | 97.36% lines, 95.03% branches, 95.93% functions (all files) |
| Mutation check | Throwaway script, 28 deliberate source faults | 28 caught. The first run caught 27, and the weak test was fixed |
| Zero dependencies | `npm run check:imports` | Passed |
| Package contents | `npm pack --dry-run` | 12 files at version 1.1.0, with no tests or dotfiles |
| Stdio handshake | `initialize`, `notifications/initialized`, `tools/list` | Passed: protocol `2025-06-18`, seven tools, all read-only |
| Startup without configuration | `node bin/redpanda-console-mcp.mjs` | Exit code 2, naming `CONSOLE_BASE_URL` |
| Version output | `--version` | `1.1.0` |

No test failed in the final run. The new tests use only a fake `fetch` or a mock server on
`127.0.0.1`.

## Secrets and private details

A scan of the tracked and new files found no secrets, personal details or company-specific
hostnames. Credential-like strings in the tests are made-up fixtures. The public GitHub
handle `KietDev-JS` appears in the package metadata and README links, as expected.
