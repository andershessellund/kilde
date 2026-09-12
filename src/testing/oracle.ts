// ---------------------------------------------------------------------------
// DecisionOracle — abstract decision point for exhaustive testing
//
// ExhaustiveOracle — records decisions as a mixed-radix sequence and
// supports increment to explore all permutations.
// ---------------------------------------------------------------------------

/**
 * Abstract decision point. Picks a number from [0, range).
 *
 * Implementations control which value is returned, enabling exhaustive
 * exploration of all possible scheduling interleavings.
 */
export interface DecisionOracle {
  /** Pick an integer from [0, range). */
  integer(range: number): number;
}

/**
 * A recorded decision: the range of choices and the one that was picked.
 */
export interface Decision {
  range: number;
  picked: number;
}

/**
 * Exhaustive oracle — records every decision as a mixed-radix digit.
 *
 * The orchestrator creates a new oracle for each run, passing in the
 * decision sequence. The oracle replays recorded decisions and extends
 * the sequence when it encounters new decision points (always picking 0).
 *
 * After a run, the sequence can be incremented to explore the next
 * permutation. When increment overflows (all permutations explored),
 * the sequence becomes empty.
 */
export class ExhaustiveOracle implements DecisionOracle {
  #index = 0;
  readonly #sequence: Decision[];

  constructor(sequence: Decision[]) {
    this.#sequence = sequence;
  }

  integer(range: number): number {
    if (range <= 1) return 0;

    if (this.#index < this.#sequence.length) {
      const decision = this.#sequence[this.#index++];
      if (decision.range !== range) {
        throw new Error(
          `ExhaustiveOracle: range mismatch at index ${this.#index - 1} — ` +
            `expected ${decision.range}, got ${range}`,
        );
      }
      return decision.picked;
    }

    // New decision point — pick 0 (first option)
    this.#index++;
    this.#sequence.push({ range, picked: 0 });
    return 0;
  }

  /** Get the recorded decision sequence (for incrementing). */
  getSequence(): Decision[] {
    return this.#sequence;
  }
}

/**
 * Increment a decision sequence (mixed-radix counter).
 *
 * Returns a new sequence representing the next permutation.
 * Returns an empty array when all permutations have been explored.
 */
export function incrementSequence(sequence: Decision[]): Decision[] {
  const result = sequence.map((d) => ({ ...d }));

  for (let i = result.length - 1; i >= 0; i--) {
    result[i].picked++;
    if (result[i].picked < result[i].range) {
      return result;
    }
    // Overflow — drop this digit and carry
    result.pop();
  }

  // All permutations explored
  return [];
}
