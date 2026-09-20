// ---------------------------------------------------------------------------
// Entry point hygiene: the core has no dependencies. Only the optional entry
// points may import their peers: kilde/valsem → valsem, kilde/testing →
// stifinder. This test walks the import graph from each entry point and
// keeps a convenience import from creeping back in, directly or through a
// re-export.
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';

const SRC = new URL('.', import.meta.url).pathname;

// Every static or dynamic import / require / side-effect import specifier.
const SPECIFIER =
  /(?:\bfrom\s*|\bimport\s*\(?\s*|\brequire\s*\(\s*|^\s*import\s+)['"]([^'"]+)['"]/gm;

/** Bare (package) specifiers reachable from `entry`, with the file that names each. */
function bareImports(entry: string): Map<string, string> {
  const seen = new Set<string>();
  const found = new Map<string, string>();
  const queue = [entry];
  while (queue.length > 0) {
    const file = queue.pop()!;
    if (seen.has(file)) continue;
    seen.add(file);
    const text = readFileSync(file, 'utf8');
    for (const m of text.matchAll(SPECIFIER)) {
      const spec = m[1];
      if (spec.startsWith('.')) {
        queue.push(join(dirname(file), spec.replace(/\.js$/, '.ts')));
      } else if (!found.has(spec)) {
        found.set(spec, relative(SRC, file));
      }
    }
  }
  return found;
}

const PEERS = ['valsem', 'stifinder'];
const isPeer = (spec: string) => PEERS.some((p) => spec === p || spec.startsWith(`${p}/`));

describe('entry points', () => {
  it('the core entry points reach no peer dependency', () => {
    for (const entry of ['index.ts', 'node.ts']) {
      const offenders = [...bareImports(join(SRC, entry))]
        .filter(([spec]) => isPeer(spec))
        .map(([spec, file]) => `${entry} → ${file} imports ${spec}`);
      expect(offenders).toEqual([]);
    }
  });

  it('each optional entry point reaches only its own peer', () => {
    const own: Record<string, string> = { 'valsem.ts': 'valsem', 'testing.ts': 'stifinder' };
    for (const [entry, peer] of Object.entries(own)) {
      const peers = [...bareImports(join(SRC, entry)).keys()].filter(isPeer);
      expect(peers.every((spec) => spec === peer || spec.startsWith(`${peer}/`))).toBe(true);
    }
  });

  it('the walk sees every import form', () => {
    const forms = [
      "import x from 'valsem';",
      'import "valsem";',
      "import 'valsem/temporal';",
      "export { a } from 'valsem';",
      "const m = await import('valsem');",
      "const r = require('valsem');",
    ];
    for (const form of forms) {
      const specs = [...form.matchAll(SPECIFIER)].map((m) => m[1]);
      expect(specs.some(isPeer), form).toBe(true);
    }
  });
});
