// ---------------------------------------------------------------------------
// map — transform each value
// ---------------------------------------------------------------------------

import type { Sink, Operator, PAUSE } from '../types.js';
import { OperatorStream, OperatorSource } from '../internal/operator-stream.js';

class MapStream<T, R> extends OperatorStream<T, R> {
  constructor(
    sink: Sink<R>,
    private readonly fn: (value: T) => R,
  ) {
    super(sink);
  }

  protected onValue(value: T): undefined | PAUSE {
    // Only `fn` is operator logic; a throwing downstream sink propagates
    // to the producer (see OperatorStream).
    const mapped = this.fn(value);
    return this.emit(mapped);
  }
}

/**
 * Transform each value from the source using `fn`.
 *
 * If `fn` throws, the error is sent to the downstream sink and the
 * upstream is disposed.
 *
 * @example
 * ```ts
 * stream(fromArray([1, 2, 3]), map(x => x * 2), toArray()) // [2, 4, 6]
 * ```
 */
export function map<T, R>(fn: (value: T) => R): Operator<T, R> {
  return (source) => new OperatorSource(source, (sink) => new MapStream(sink, fn));
}
