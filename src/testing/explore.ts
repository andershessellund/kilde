// ---------------------------------------------------------------------------
// exploreTest — run a DecisionOracle test body under stifinder
//
// The test body is a synchronous function of a DecisionOracle. Its state
// space is the tree of decision sequences. Under stifinder:
//
//   state  = the sequence of picks made so far (a plain number[])
//   event  = the next pick, offered in preference order: 0 first, so any
//            non-zero pick is a deviation from the "boring" schedule
//   applyEvent(prefix, k) = run the body with the oracle replaying
//            prefix + [k] and answering 0 to everything after
//
// Each run is allowed to continue to completion after the new decision,
// answering 0 to every later decision point. That harvests the ranges of
// every state along the default continuation in one go, so the number of
// runs is the number of deviating edges plus one, not the number of edges.
// A run that throws attributes the error to the edge into the deepest
// state it reached.
// ---------------------------------------------------------------------------

import { StateSpaceCache, exploreIteratively } from 'stifinder';
import type { DecisionLabel, DecisionOracle } from './oracle.js';

/** Outcome of the body for one prefix, recorded per state along the default chain. */
type Entry =
  | { kind: 'branch'; range: number; label: DecisionLabel | undefined }
  | { kind: 'terminal' }
  | { kind: 'error'; error: unknown };

class HarvestingOracle implements DecisionOracle {
  #index = 0;
  readonly ranges: number[] = [];
  readonly labels: (DecisionLabel | undefined)[] = [];

  /**
   * @param picks          decisions to replay
   * @param expectedRanges the range each replayed decision had when first seen
   */
  constructor(
    private readonly picks: readonly number[],
    private readonly expectedRanges: readonly number[],
  ) {}

  integer(range: number, label?: DecisionLabel): number {
    if (range <= 1) return 0;
    const i = this.#index++;
    this.ranges.push(range);
    this.labels.push(label);
    if (i < this.picks.length) {
      if (range !== this.expectedRanges[i]) {
        throw new Error(
          `exploreTest: decision ${i} had range ${this.expectedRanges[i]} before and ${range} now — the body is not deterministic`,
        );
      }
      return this.picks[i];
    }
    return 0;
  }

  /** Number of decision points consulted so far. */
  get consulted(): number {
    return this.#index;
  }
}

export interface ExploreTestOptions {
  /** Deepest deviation budget tried. Default: unbounded. */
  maxDeviations?: number;
  /** Cap on edges explored. Default: unbounded. */
  maxEdges?: number;
  /** Wall-clock cap for the whole exploration. */
  timeoutMs?: number;
}

export interface ExploreTestStats {
  /** Number of times the body was run. */
  runs: number;
  /** Distinct states (decision prefixes) discovered. */
  states: number;
  /** Edges computed by stifinder. */
  edges: number;
  /** Highest deviation budget that completed. */
  maxDeviationsReached: number;
  /** Whether the space was exhausted. */
  completed: boolean;
  /** Wall-clock milliseconds. */
  ms: number;
}

const keyOf = (picks: readonly number[]) => picks.join(',');

/**
 * The deviations in `picks`, one line each, in plain words where the
 * decision point supplied a label.
 */
function describeDeviations(picks: readonly number[], table: Map<string, Entry>): string {
  const lines: string[] = [];
  for (let i = 0; i < picks.length; i++) {
    const pick = picks[i];
    if (pick === 0) continue;
    const entry = table.get(keyOf(picks.slice(0, i)));
    const branch = entry?.kind === 'branch' ? entry : undefined;
    const label = branch?.label;
    let text: string;
    if (typeof label === 'function') text = label(pick);
    else if (typeof label === 'string') text = branch && branch.range > 2 ? `${label} (pick ${pick})` : label;
    else text = `decision #${i + 1} picked ${pick}`;
    lines.push(`  ${lines.length + 1}. ${text}`);
  }
  if (lines.length === 0) return '0 deviations (the plain schedule):';
  return `${lines.length} deviation${lines.length === 1 ? '' : 's'}:\n${lines.join('\n')}`;
}

/**
 * Explore every decision sequence of `fn` with stifinder, deviations first.
 * Throws on the violation that needs the fewest departures from the
 * all-zero schedule, with its decision sequence attached. Resolves with
 * exploration statistics otherwise.
 */
export async function exploreTest(
  fn: (oracle: DecisionOracle) => void,
  options: ExploreTestOptions = {},
): Promise<ExploreTestStats> {
  const started = performance.now();
  const table = new Map<string, Entry>();
  let runs = 0;

  /** Run the body for `picks` and record every state along its default continuation. */
  function run(picks: readonly number[]): void {
    runs++;
    const expected: number[] = [];
    for (let i = 0; i < picks.length; i++) {
      const entry = table.get(keyOf(picks.slice(0, i)));
      expected.push(entry?.kind === 'branch' ? entry.range : -1);
    }
    const oracle = new HarvestingOracle(picks, expected);
    let error: { error: unknown } | null = null;
    try {
      fn(oracle);
    } catch (err) {
      error = { error: err };
    }
    if (oracle.consulted < picks.length) {
      throw new Error(
        `exploreTest: the body consulted ${oracle.consulted} decisions but the prefix has ${picks.length} — the body is not deterministic`,
      );
    }
    // States along the default chain: picks, picks+[0], picks+[0,0], ...
    const chain: number[] = [...picks];
    for (let i = picks.length; i < oracle.ranges.length; i++) {
      table.set(keyOf(chain), { kind: 'branch', range: oracle.ranges[i], label: oracle.labels[i] });
      chain.push(0);
    }
    table.set(keyOf(chain), error ? { kind: 'error', error: error.error } : { kind: 'terminal' });
  }

  function entryFor(picks: readonly number[]): Entry {
    let entry = table.get(keyOf(picks));
    if (!entry) {
      run(picks);
      entry = table.get(keyOf(picks))!;
    }
    return entry;
  }

  const cache = new StateSpaceCache<number[], number>({
    initialState: [],
    async getEvents(state) {
      const entry = entryFor(state);
      if (entry.kind !== 'branch') return [];
      const events = [];
      for (let k = 0; k < entry.range; k++) events.push({ event: k, cost: [] });
      return events;
    },
    async applyEvent(state, event) {
      const next = [...state, event];
      const entry = entryFor(next);
      if (entry.kind === 'error') return { error: entry.error };
      return { to: next };
    },
  });

  const space = await exploreIteratively(cache, {
    maxDeviations: options.maxDeviations ?? Number.MAX_SAFE_INTEGER,
    maxEdges: options.maxEdges ?? Number.MAX_SAFE_INTEGER,
    timeoutMs: options.timeoutMs,
  });

  const stats: ExploreTestStats = {
    runs,
    states: table.size,
    edges: space.edgesComputed,
    maxDeviationsReached: space.maxDeviationsReached,
    completed: space.completed,
    ms: performance.now() - started,
  };

  if (space.violation) {
    const picks = space.violation.steps.map((s) => s.event);
    const err = space.violation.error;
    const msg = err instanceof Error ? err.message : String(err);
    throw new Error(
      `exhaustiveTest failed with ${describeDeviations(picks, table)}\n` +
        `  (${picks.length} decisions, ${runs} runs, ${table.size} states explored)\n` +
        `  decisions: [${picks.join(', ')}]\n` +
        `  error: ${msg}`,
      { cause: err },
    );
  }

  if (!space.completed) {
    throw new Error(
      `exploreTest: exploration did not complete (${stats.edges} edges, ` +
        `${stats.maxDeviationsReached} deviations reached, ${stats.ms.toFixed(0)} ms)`,
    );
  }

  return stats;
}
