// ---------------------------------------------------------------------------
// testSource — parameterized source for exhaustive testing
//
// The oracle controls two things:
//   - whether the source self-pauses after delivering a value (modelling a
//     source that has nothing more to say right now and will be resumed
//     later), and
//   - when the last value is answered with PAUSE, whether completion is
//     delivered immediately (terminal events may arrive while paused) or
//     on the next resume().
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';
import type { DecisionOracle } from './oracle.js';

class TestSourceStream<T> implements Stream {
  #index = 0;
  #disposed = false;
  #completed = false;

  constructor(
    private readonly sink: Sink<T>,
    private readonly values: T[],
    private readonly oracle: DecisionOracle | undefined,
  ) {}

  resume(): void {
    if (this.#disposed || this.#completed) return;
    while (this.#index < this.values.length) {
      const value = this.values[this.#index++];
      const result = this.sink.next(value);
      if (this.#disposed) return;
      if (result === PAUSE) {
        // Nothing left: the oracle decides whether completion arrives now
        // (while the sink is paused) or on the next resume().
        if (this.#index >= this.values.length && this.oracle && this.oracle.integer(2) === 1) {
          this.#complete();
        }
        return;
      }
      // The oracle may decide the source self-pauses here.
      if (this.oracle && this.#index < this.values.length && this.oracle.integer(2) === 1) {
        return;
      }
    }
    this.#complete();
  }

  #complete(): void {
    if (this.#disposed || this.#completed) return;
    this.#completed = true;
    this.sink.complete();
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
  }
}

class TestSourceImpl<T> extends AbstractSource<T> {
  constructor(
    private readonly values: T[],
    private readonly oracle: DecisionOracle | undefined,
  ) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    return new TestSourceStream(sink, this.values, this.oracle);
  }
}

export interface TestSourceOptions {
  /** Oracle to parameterize delivery behavior. */
  oracle?: DecisionOracle;
}

/**
 * Create a test source that emits the given values.
 *
 * When an `oracle` is provided, it parameterizes whether the source
 * self-pauses between values and whether completion is delivered while the
 * sink is paused or on the next `resume()` (enabling exhaustive testing of
 * different delivery patterns).
 *
 * Without an oracle, behaves identically to `fromArray`.
 */
export function testSource<T>(values: T[], opts?: TestSourceOptions): Source<T> {
  return new TestSourceImpl(values, opts?.oracle);
}
