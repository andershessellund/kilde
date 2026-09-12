// ---------------------------------------------------------------------------
// toSource — wrap the upstream pipeline as a single emitted value
//
// Useful for creating streams of sources (input to flatten):
//   stream(source, map(x => pipe(from(x), map(...))), toSource())
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { AbstractSource } from '../abstract-source.js';
import { SingleValueStream } from '../internal/single-value-stream.js';

class ToSourceSource<T> extends AbstractSource<Source<T>> {
  constructor(private readonly source: Source<T>) {
    super();
  }

  connect(sink: Sink<Source<T>>): Stream {
    return new SingleValueStream(sink, () => this.source);
  }
}

/**
 * Wrap the upstream pipeline as a single emitted `Source<T>` value.
 *
 * The upstream is NOT connected or consumed — it's wrapped as-is.
 * Useful for creating streams of sources (e.g., input to `flatten()`).
 * The value is emitted once, on the first `resume()`.
 *
 * @example
 * ```ts
 * // Get a lazy source back from stream()
 * const src = stream(fromArray([1,2,3]), map(x => x*2), toSource());
 * // src is Source<number> — not yet consumed
 * ```
 */
export function toSource<T>(): Operator<T, Source<T>> {
  return (source) => new ToSourceSource(source);
}
