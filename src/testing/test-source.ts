// ---------------------------------------------------------------------------
// testSource — parameterized source for exhaustive testing
//
// The oracle controls whether each value delivery causes a pause and
// whether delivery is immediate or deferred.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';
import type { DecisionOracle } from './oracle.js';

class TestSourceStream<T> implements Stream {
  #index = 0;
  #disposed = false;

  constructor(
    private readonly sink: Sink<T>,
    private readonly values: T[],
    private readonly oracle: DecisionOracle | undefined,
  ) {}

  resume(): void {
    while (this.#index < this.values.length && !this.#disposed) {
      const value = this.values[this.#index++];
      const result = this.sink.next(value);
      if (result === PAUSE) {
        return;
      }
      // If oracle present, it may decide to pause after delivery
      if (this.oracle && this.#index < this.values.length) {
        const shouldPause = this.oracle.integer(2) === 1;
        if (shouldPause) {
          return;
        }
      }
    }
    if (!this.#disposed && this.#index >= this.values.length) {
      this.sink.complete();
    }
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
 * self-pauses between values (enabling exhaustive testing of different
 * delivery patterns).
 *
 * Without an oracle, behaves identically to `fromArray`.
 */
export function testSource<T>(values: T[], opts?: TestSourceOptions): Source<T> {
  return new TestSourceImpl(values, opts?.oracle);
}
