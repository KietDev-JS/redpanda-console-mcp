// Guard the zero-dependency promise: shipped code may import only relative
// paths and node: builtins. Anything else means a runtime dependency crept in.
//
// Run from CI and locally via `node scripts/check-imports.mjs`.

import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

const ROOTS = ['src', 'bin'];
const IMPORT_RE = /\bfrom\s+['"]([^'"]+)['"]|\bimport\s*\(\s*['"]([^'"]+)['"]\s*\)/g;

function walk(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) out.push(...walk(full));
    else if (entry.endsWith('.mjs') || entry.endsWith('.js')) out.push(full);
  }
  return out;
}

const offenders = [];
for (const root of ROOTS) {
  for (const file of walk(root)) {
    const source = readFileSync(file, 'utf8');
    for (const m of source.matchAll(IMPORT_RE)) {
      const spec = m[1] ?? m[2];
      const ok = spec.startsWith('.') || spec.startsWith('node:');
      if (!ok) offenders.push(`${file}: ${spec}`);
    }
  }
}

if (offenders.length > 0) {
  console.error('Non-builtin imports found in shipped code:');
  for (const o of offenders) console.error(`  ${o}`);
  process.exit(1);
}
console.log('OK: shipped code imports only relative paths and node: builtins');
