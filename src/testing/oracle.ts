// ---------------------------------------------------------------------------
// DecisionOracle — abstract decision point for state-space exploration
//
// testSource and testSink ask the oracle at every point where they could
// pause, self-pause, or deliver a terminal event. exhaustiveTest supplies
// stifinder's `Decisions`, of which this is the `integer` method, and
// stifinder enumerates the decision sequences.
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
   * #2"). A function is given every pick, 0 included, and says how it
   * reads.
   */
  integer(range: number, label?: DecisionLabel): number;
}

/** Plain-words description of a deviating pick. */
export type DecisionLabel = string | ((pick: number) => string);
