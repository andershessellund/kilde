// ---------------------------------------------------------------------------
// exploreTest — run a test body under stifinder, through its decisions
//
// The body is a function of stifinder's `Decisions`, whose `integer` is the
// DecisionOracle that testSource and testSink consult. stifinder's
// decisionModel is the model:
//
//   state  = the picks made so far
//   event  = the next pick, offered in preference order: 0 first, so any
//            non-zero pick is a deviation from the plain schedule
//   a run  = the body with a state's picks replayed and 0 answered to every
//            later decision; it records every state along that default
//            continuation, so the body runs once per leaf of its decision
//            tree, not once per edge
//
// check() runs the search as a test. It rejects with a ViolationError for
// the failure that needs the fewest deviations, a body's throw before its
// first decision and an async body's rejection included; with an
// IncompleteError when maxEdges or timeoutMs cut the search short; and with
// a DecisionsError when the body is not deterministic.
// ---------------------------------------------------------------------------

import { check, decisionModel } from 'stifinder';
import type { Decisions } from 'stifinder';

export interface ExploreTestOptions {
  /** Deepest deviation budget tried. Default: unbounded. */
  maxDeviations?: number;
  /** Cap on edges explored. Default: unbounded. */
  maxEdges?: number;
  /** Wall-clock cap for the whole exploration. */
  timeoutMs?: number;
  /**
   * The most decisions one run of the body may make. A run past it is cut
   * off, and that is a failure: the body does not end under that schedule.
   * Default: stifinder's, 10,000.
   */
  maxDecisions?: number;
}

export interface ExploreTestStats {
  /** Number of times the body was run. */
  runs: number;
  /** Distinct states (decision prefixes) reached. */
  states: number;
  /** Edges computed by stifinder. */
  edges: number;
  /** Highest deviation budget that completed. */
  maxDeviationsReached: number;
  /**
   * Whether exploration finished within `maxEdges` and `timeoutMs`. It is
   * always true here, since exploreTest rejects otherwise, and it clears only
   * the budget explored: see `exhaustive`.
   */
  completed: boolean;
  /**
   * Whether every decision sequence was explored. False only when
   * `maxDeviations` stopped the search: the body then passed on every
   * sequence with up to `maxDeviationsReached` deviations, and nothing is
   * known about the rest.
   */
  exhaustive: boolean;
  /** Wall-clock milliseconds. */
  ms: number;
}

/**
 * Explore every decision sequence of `fn` with stifinder, deviations first.
 * Rejects with stifinder's `ViolationError` for the violation that needs the
 * fewest departures from the all-zero schedule; `runOnce(fn, error)` runs
 * that sequence again. Resolves with exploration statistics otherwise.
 */
export async function exploreTest(
  fn: (decide: Decisions) => unknown,
  options: ExploreTestOptions = {},
): Promise<ExploreTestStats> {
  const started = performance.now();
  const model = decisionModel(fn, { maxDecisions: options.maxDecisions });
  const space = await check(model, {
    maxDeviations: options.maxDeviations ?? Infinity,
    maxEdges: options.maxEdges ?? Infinity,
    timeoutMs: options.timeoutMs,
  });
  return {
    runs: model.runs,
    states: space.costs.size,
    edges: space.edgesComputed,
    maxDeviationsReached: space.maxDeviationsReached,
    completed: space.completed,
    exhaustive: space.exhaustive,
    ms: performance.now() - started,
  };
}
