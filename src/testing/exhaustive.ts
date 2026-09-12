// ---------------------------------------------------------------------------
// exhaustiveTest — run all permutations via DecisionOracle
// ---------------------------------------------------------------------------

import { ExhaustiveOracle, incrementSequence } from './oracle.js';
import type { Decision } from './oracle.js';

/**
 * Run `fn` exhaustively, exploring all decision permutations.
 *
 * The provided `oracle` controls non-deterministic choices. After each
 * run, the decision sequence is incremented (mixed-radix counter) to
 * explore the next permutation. Stops when all permutations are explored.
 *
 * Throws on the first failure, including the permutation index and
 * decision sequence for debugging.
 *
 * @returns The number of permutations explored.
 *
 * @example
 * ```ts
 * const runs = exhaustiveTest((oracle) => {
 *   const src = testSource([1, 2, 3], { oracle });
 *   const sink = testSink<number>({ oracle });
 *   const s = src.connect(sink);
 *   s.resume();
 *   while (sink.completeCount === 0) s.resume();
 *   expect(sink.values).toEqual([1, 2, 3]);
 * });
 * ```
 */
export function exhaustiveTest(fn: (oracle: ExhaustiveOracle) => void): number {
  let runs = 0;
  let sequence: Decision[] = [];

  do {
    runs++;
    const oracle = new ExhaustiveOracle([...sequence.map((d) => ({ ...d }))]);

    try {
      fn(oracle);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      throw new Error(
        `exhaustiveTest failed on permutation #${runs}:\n` +
          `  decisions: [${oracle
            .getSequence()
            .map((d) => `${d.picked}/${d.range}`)
            .join(', ')}]\n` +
          `  error: ${msg}`,
        { cause: err },
      );
    }

    sequence = incrementSequence(oracle.getSequence());
  } while (sequence.length > 0);

  return runs;
}
