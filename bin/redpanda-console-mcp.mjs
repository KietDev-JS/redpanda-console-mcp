#!/usr/bin/env node
// CLI entry point: read configuration and serve MCP over stdio.

import { loadConfig, ConfigError, redact } from '../src/config.mjs';
import { ConsoleClient } from '../src/console.mjs';
import { ConsoleService } from '../src/service.mjs';
import { createServer, listen, SERVER_INFO } from '../src/server.mjs';

function main(argv = process.argv.slice(2)) {
  if (argv.includes('--version') || argv.includes('-v')) {
    process.stdout.write(`${SERVER_INFO.version}\n`);
    return;
  }
  if (argv.includes('--help') || argv.includes('-h')) {
    process.stdout.write(
      `${SERVER_INFO.name} ${SERVER_INFO.version}\n\n` +
        'An MCP stdio server for Redpanda Console. Configure with environment variables:\n\n' +
        '  CONSOLE_BASE_URL    required, e.g. https://console.example.com\n' +
        '  CONSOLE_API_KEY     optional bearer token\n' +
        '  CONSOLE_TIMEOUT     request timeout in seconds (default 60)\n' +
        '  CONSOLE_VERIFY_TLS  set false to skip TLS verification (insecure)\n',
    );
    return;
  }

  let config;
  try {
    config = loadConfig(process.env);
  } catch (e) {
    if (e instanceof ConfigError) {
      process.stderr.write(`${SERVER_INFO.name}: ${e.message}\n`);
      process.exitCode = 2;
      return;
    }
    throw e;
  }

  if (!config.verifyTls) {
    // fetch() has no per-request TLS switch without a third-party dispatcher,
    // and this package ships zero dependencies, so this is process-wide.
    // It is opt-in, and loud, because it disables TLS verification for
    // everything in this process.
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
    process.stderr.write(
      `${SERVER_INFO.name}: WARNING - CONSOLE_VERIFY_TLS is false; ` +
        'TLS certificate verification is disabled for this process.\n',
    );
  }

  if (process.env.CONSOLE_DEBUG_CONFIG === '1') {
    process.stderr.write(`${SERVER_INFO.name}: ${JSON.stringify(redact(config))}\n`);
  }

  const service = new ConsoleService(new ConsoleClient(config));
  // `listen` owns the JSON-RPC framing and `createServer` needs its `send`,
  // so a ref breaks the circular construction.
  const handlerRef = { current: null };
  const { send } = listen((msg) => handlerRef.current(msg));
  handlerRef.current = createServer(service, send);
}

main();
