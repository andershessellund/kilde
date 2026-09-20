// ---------------------------------------------------------------------------
// exhaustiveTest — explore every decision sequence of a test body
//
// A thin name over exploreTest (explore.ts), which runs the body under
// stifinder: breadth-first over decision prefixes, fewest deviations from
// the all-zero schedule first, with the smallest counterexample reported.
// ---------------------------------------------------------------------------

import { exploreTest } from './explore.js';
import type { ExploreTestOptions, ExploreTestStats } from './explore.js';
import type { DecisionOracle } from './oracle.js';

/**
 * Run `fn` for every decision sequence its oracle can produce.
 *
 * Throws on the violation that needs the fewest departures from the
 * baseline schedule, with its decision sequence attached. Resolves with
 * exploration statistics otherwise. The space is explored to exhaustion
 * unless `options` bound it.
 *
 * @example
 * ```ts
 * await exhaustiveTest((oracle) => {
 *   const src = testSource([1, 2, 3], { oracle });
 *   const sink = testSink<number>({ oracle });
 *   const s = pipe(src, myOperator(), assertProtocol()).connect(sink);
 *   s.resume();
 *   while (sink.completeCount === 0) s.resume();
 *   expect(sink.values).toEqual([1, 2, 3]);
 * });
 * ```
 */
export function exhaustiveTest(
  fn: (oracle: DecisionOracle) => void,
  options?: ExploreTestOptions,
): Promise<ExploreTestStats> {
  return exploreTest(fn, options);
}
