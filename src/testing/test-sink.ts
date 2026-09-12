// ---------------------------------------------------------------------------
// testSink — recording sink with parameterized pause decisions
// ---------------------------------------------------------------------------

import type { Sink } from '../types.js';
import { PAUSE as PAUSE_SYM } from '../types.js';
import type { DecisionOracle } from './oracle.js';

/**
 * A test sink that records all received values, errors, and completions.
 *
 * When an `oracle` is provided, it parameterizes whether the sink returns
 * PAUSE after each `next()` call.
 */
export interface TestSink<T> extends Sink<T> {
  /** All values received via `next()`. */
  readonly values: T[];
  /** All errors received via `error()`. */
  readonly errors: unknown[];
  /** Number of times `complete()` was called. */
  readonly completeCount: number;
}

export interface TestSinkOptions {
  /** Oracle to parameterize pause behavior. */
  oracle?: DecisionOracle;
}

class TestSinkImpl<T> implements TestSink<T> {
  readonly values: T[] = [];
  readonly errors: unknown[] = [];
  completeCount = 0;

  constructor(private readonly oracle: DecisionOracle | undefined) {}

  next(value: T): undefined | typeof PAUSE_SYM {
    this.values.push(value);
    if (this.oracle) {
      const shouldPause = this.oracle.integer(2) === 1;
      if (shouldPause) return PAUSE_SYM;
    }
    return undefined;
  }

  complete(): void {
    this.completeCount++;
  }

  error(error: unknown): void {
    this.errors.push(error);
  }
}

/**
 * Create a test sink that records values, errors, and completions.
 *
 * When an `oracle` is provided, it parameterizes whether the sink
 * returns PAUSE after each value (enabling exhaustive testing of
 * backpressure scenarios).
 */
export function testSink<T>(opts?: TestSinkOptions): TestSink<T> {
  return new TestSinkImpl<T>(opts?.oracle);
}
