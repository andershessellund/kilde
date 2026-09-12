// ---------------------------------------------------------------------------
// filter — drop values that don't match a predicate
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class FilterStream<T> implements Stream, Sink<T> {
  #disposed = false;
  #upstream!: Stream;

  constructor(
    private readonly sink: Sink<T>,
    private readonly predicate: (value: T) => boolean,
  ) {}

  _setUpstream(upstream: Stream): void {
    this.#upstream = upstream;
  }

  // --- Sink<T> ---

  next(value: T): undefined | typeof PAUSE {
    if (this.#disposed) return PAUSE;
    try {
      if (this.predicate(value)) {
        return this.sink.next(value);
      }
      return undefined;
    } catch (err) {
      this.sink.error(err);
      this.#upstream[Symbol.dispose]();
      return PAUSE;
    }
  }

  complete(): void {
    if (!this.#disposed) {
      this.sink.complete();
    }
  }

  error(error: unknown): void {
    if (!this.#disposed) {
      this.sink.error(error);
    }
  }

  // --- Stream ---

  resume(): void {
    this.#upstream.resume();
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
    this.#upstream[Symbol.dispose]();
  }
}

class FilterSource<T> extends AbstractSource<T> {
  constructor(
    private readonly source: Source<T>,
    private readonly predicate: (value: T) => boolean,
  ) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    const filterStream = new FilterStream(sink, this.predicate);
    const upstream = this.source.connect(filterStream);
    filterStream._setUpstream(upstream);
    return filterStream;
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
  return (source) => new FilterSource(source, predicate);
}
