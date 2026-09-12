// ---------------------------------------------------------------------------
// scan — emit each intermediate accumulation
// ---------------------------------------------------------------------------

import type { Sink, Operator, PAUSE } from '../types.js';
import { OperatorStream, OperatorSource } from '../internal/operator-stream.js';

class ScanStream<T, R> extends OperatorStream<T, R> {
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
    return this.emit(this.#acc);
  }
}

/**
 * Accumulate values and emit each intermediate result.
 *
 * Like `reduce` but emits after every value, not just on complete.
 * If `fn` throws, the error is sent downstream and the upstream is disposed.
 *
 * @example
 * ```ts
 * stream(fromArray([1, 2, 3]), scan((acc, x) => acc + x, 0), toArray())
 * // [1, 3, 6]
 * ```
 */
export function scan<T, R>(fn: (acc: R, value: T) => R, initial: R): Operator<T, R> {
  return (source) => new OperatorSource(source, (sink) => new ScanStream(sink, fn, initial));
}
