// ---------------------------------------------------------------------------
// DecisionOracle — abstract decision point for state-space exploration
//
// testSource and testSink ask the oracle at every point where they could
// pause, self-pause, or deliver a terminal event. The explorer behind
// exhaustiveTest (see explore.ts) supplies the oracle and enumerates the
// decision sequences.
// ---------------------------------------------------------------------------

/**
 * Abstract decision point. Picks a number from [0, range).
 *
 * Index 0 is the baseline, the thing that "should" happen: no pause, no
 * self-pause, completion on resume. Every other index is a deviation from
 * that schedule. Implementations control which value is returned, enabling
 * exhaustive exploration of all possible scheduling interleavings.
 */
export interface DecisionOracle {
  /**
   * Pick an integer from [0, range).
   *
   * `label` says what a non-zero pick means, in plain words, so a failure
   * report can list the deviations that led to it ("sink pauses after value
   * #2"). A function receives the pick, for ranges above 2.
   */
  integer(range: number, label?: DecisionLabel): number;
}

/** Plain-words description of a deviating pick. */
export type DecisionLabel = string | ((pick: number) => string);
