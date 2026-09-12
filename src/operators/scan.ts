// ---------------------------------------------------------------------------
// scan — emit each intermediate accumulation
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class ScanStream<T, R> implements Stream, Sink<T> {
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
      return this.sink.next(this.#acc);
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

class ScanSource<T, R> extends AbstractSource<R> {
  constructor(
    private readonly source: Source<T>,
    private readonly fn: (acc: R, value: T) => R,
    private readonly initial: R,
  ) {
    super();
  }

  connect(sink: Sink<R>): Stream {
    const scanStream = new ScanStream(sink, this.fn, this.initial);
    const upstream = this.source.connect(scanStream);
    scanStream._setUpstream(upstream);
    return scanStream;
  }
}

/**
 * Accumulate values and emit each intermediate result.
 *
 * Like `reduce` but emits after every value, not just on complete.
 *
 * @example
 * ```ts
 * stream(fromArray([1, 2, 3]), scan((acc, x) => acc + x, 0), toArray())
 * // [1, 3, 6]
 * ```
 */
export function scan<T, R>(fn: (acc: R, value: T) => R, initial: R): Operator<T, R> {
  return (source) => new ScanSource(source, fn, initial);
}
