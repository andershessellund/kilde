// ---------------------------------------------------------------------------
// testSink — recording sink with parameterized pause decisions
//
// Also asserts the part of the protocol a sink can see on its own: nothing
// may arrive after complete() or error().
// ---------------------------------------------------------------------------

import type { Sink } from '../types.js';
import { PAUSE as PAUSE_SYM } from '../types.js';
import type { DecisionOracle } from './oracle.js';
import { ProtocolViolationError } from './protocol.js';

/**
 * A test sink that records all received values, errors, and completions.
 *
 * When an `oracle` is provided, it parameterizes whether the sink returns
 * PAUSE after each `next()` call.
 *
 * Throws {@link ProtocolViolationError} if `next()`, `complete()` or
 * `error()` is called after a terminal event.
 */
export interface TestSink<T> extends Sink<T> {
  /** All values received via `next()`. */
  readonly values: T[];
  /** All errors received via `error()`. */
  readonly errors: unknown[];
  /** Number of times `complete()` was called. */
  readonly completeCount: number;
  /** Whether `complete()` or `error()` has been received. */
  readonly terminated: boolean;
  /** Whether the last `next()` returned `PAUSE`. */
  readonly paused: boolean;
}

export interface TestSinkOptions {
  /** Oracle to parameterize pause behavior. */
  oracle?: DecisionOracle;
}

class TestSinkImpl<T> implements TestSink<T> {
  readonly values: T[] = [];
  readonly errors: unknown[] = [];
  completeCount = 0;
  terminated = false;
  paused = false;

  constructor(private readonly oracle: DecisionOracle | undefined) {}

  next(value: T): undefined | typeof PAUSE_SYM {
    if (this.terminated) throw new ProtocolViolationError('next() after a terminal event');
    this.values.push(value);
    if (this.oracle && this.oracle.integer(2) === 1) {
      this.paused = true;
      return PAUSE_SYM;
    }
    this.paused = false;
    return undefined;
  }

  complete(): void {
    if (this.terminated) throw new ProtocolViolationError('complete() after a terminal event');
    this.terminated = true;
    this.completeCount++;
  }

  error(error: unknown): void {
    if (this.terminated) throw new ProtocolViolationError('error() after a terminal event');
    this.terminated = true;
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
