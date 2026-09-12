// ---------------------------------------------------------------------------
// filter — drop values that don't match a predicate
// ---------------------------------------------------------------------------

import type { Sink, Operator, PAUSE } from '../types.js';
import { OperatorStream, OperatorSource } from '../internal/operator-stream.js';

class FilterStream<T> extends OperatorStream<T, T> {
  constructor(
    sink: Sink<T>,
    private readonly predicate: (value: T) => boolean,
  ) {
    super(sink);
  }

  protected onValue(value: T): undefined | PAUSE {
    if (!this.predicate(value)) return undefined;
    return this.emit(value);
  }
}

/**
 * Filter values from the source, keeping only those that match `predicate`.
 *
 * If `predicate` throws, the error is sent downstream and the upstream
 * is disposed.
 *
 * @example
 * ```ts
 * stream(fromArray([1, 2, 3, 4]), filter(x => x % 2 === 0), toArray()) // [2, 4]
 * ```
 */
export function filter<T>(predicate: (value: T) => boolean): Operator<T, T> {
  return (source) => new OperatorSource(source, (sink) => new FilterStream(sink, predicate));
}
