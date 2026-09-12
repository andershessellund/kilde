// ---------------------------------------------------------------------------
// reduce — accumulate all values, emit one result on complete
//
// Works as a collector when used as the last argument to stream():
//   stream(fromArray([1,2,3]), reduce((a,b) => a+b, 0)) → 6
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class ReduceStream<T, R> implements Stream, Sink<T> {
  #acc: R;
  #disposed = false;
  #upstream!: Stream;

  constructor(
    private readonly sink: Sink<R>,
    private readonly fn: (acc: R, value: T) => R,
    initial: R,
  ) {
    this.#acc = initial;
  }

  _setUpstream(upstream: Stream): void {
    this.#upstream = upstream;
  }

  // --- Sink<T> ---

  next(value: T): undefined | typeof PAUSE {
    if (this.#disposed) return PAUSE;
    try {
      this.#acc = this.fn(this.#acc, value);
      return undefined; // Never pause — consume everything
    } catch (err) {
      this.sink.error(err);
      this.#upstream[Symbol.dispose]();
      return PAUSE;
    }
  }

  complete(): void {
    if (!this.#disposed) {
      this.sink.next(this.#acc);
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

class ReduceSource<T, R> extends AbstractSource<R> {
  constructor(
    private readonly source: Source<T>,
    private readonly fn: (acc: R, value: T) => R,
    private readonly initial: R,
  ) {
    super();
  }

  connect(sink: Sink<R>): Stream {
    const reduceStream = new ReduceStream(sink, this.fn, this.initial);
    const upstream = this.source.connect(reduceStream);
    reduceStream._setUpstream(upstream);
    return reduceStream;
  }
}

/**
 * Accumulate all values, emitting a single result on complete.
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
  return (source) => new ReduceSource(source, fn, initial);
}
