// ---------------------------------------------------------------------------
// reduce — accumulate all values, emit one result on complete
//
// Works as a collector when used as the last argument to stream():
//   stream(fromArray([1,2,3]), reduce((a,b) => a+b, 0)) → 6
// ---------------------------------------------------------------------------

import type { Sink, Operator, PAUSE } from '../types.js';
import { OperatorStream, OperatorSource } from '../internal/operator-stream.js';

class ReduceStream<T, R> extends OperatorStream<T, R> {
  #acc: R;

  constructor(
    sink: Sink<R>,
    private readonly fn: (acc: R, value: T) => R,
    initial: R,
  ) {
    super(sink);
    this.#acc = initial;
  }

  protected onValue(value: T): undefined | PAUSE {
    this.#acc = this.fn(this.#acc, value);
    return undefined; // Never pause — consume everything
  }

  protected onComplete(): void {
    // The result is the last thing we say; completing right after a PAUSE
    // is allowed (terminal events are not governed by PAUSE).
    this.emit(this.#acc);
    this.emitComplete();
  }
}

/**
 * Accumulate all values, emitting a single result on complete.
 *
 * If `fn` throws, the error is sent downstream and the upstream is disposed.
 *
 * As the last argument to `stream()`, acts as a synchronous collector:
 * ```ts
 * stream(fromArray([1, 2, 3]), reduce((a, b) => a + b, 0)) // 6
 * ```
 *
 * Can also be used mid-pipeline:
 * ```ts
 * pipe(source, reduce(fn, init)) // Source<R> that emits one value
 * ```
 */
export function reduce<T, R>(fn: (acc: R, value: T) => R, initial: R): Operator<T, R> {
  return (source) => new OperatorSource(source, (sink) => new ReduceStream(sink, fn, initial));
}
