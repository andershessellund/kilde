// ---------------------------------------------------------------------------
// take — emit the first N values, then complete
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class TakeStream<T> implements Stream, Sink<T> {
  #remaining: number;
  #disposed = false;
  #upstream!: Stream;

  constructor(
    private readonly sink: Sink<T>,
    count: number,
  ) {
    this.#remaining = count;
  }

  _setUpstream(upstream: Stream): void {
    this.#upstream = upstream;
  }

  // --- Sink<T> ---

  next(value: T): undefined | typeof PAUSE {
    if (this.#disposed || this.#remaining <= 0) return PAUSE;

    this.#remaining--;
    const result = this.sink.next(value);

    if (this.#remaining <= 0) {
      this.#upstream[Symbol.dispose]();
      this.sink.complete();
      return PAUSE;
    }

    return result;
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
    if (this.#remaining <= 0) {
      this.sink.complete();
      return;
    }
    this.#upstream.resume();
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
    this.#upstream[Symbol.dispose]();
  }
}

class TakeSource<T> extends AbstractSource<T> {
  constructor(
    private readonly source: Source<T>,
    private readonly count: number,
  ) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    const takeStream = new TakeStream(sink, this.count);
    const upstream = this.source.connect(takeStream);
    takeStream._setUpstream(upstream);
    return takeStream;
  }
}

/**
 * Emit the first `n` values from the source, then dispose upstream
 * and complete.
 *
 * `take(0)` completes immediately on resume without connecting upstream.
 *
 * @example
 * ```ts
 * stream(fromArray([1, 2, 3, 4, 5]), take(3), toArray()) // [1, 2, 3]
 * ```
 */
export function take<T>(n: number): Operator<T, T> {
  return (source) => new TakeSource(source, n);
}
