// ---------------------------------------------------------------------------
// toArray — collect all values into an array, emit on complete
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class ToArrayStream<T> implements Stream, Sink<T> {
  #buffer: T[] = [];
  #disposed = false;
  #upstream!: Stream;

  constructor(private readonly sink: Sink<T[]>) {}

  _setUpstream(upstream: Stream): void {
    this.#upstream = upstream;
  }

  // --- Sink<T> ---

  next(value: T): undefined | typeof PAUSE {
    if (this.#disposed) return PAUSE;
    this.#buffer.push(value);
    return undefined; // Never pause — collect everything
  }

  complete(): void {
    if (!this.#disposed) {
      this.sink.next(this.#buffer);
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

class ToArraySource<T> extends AbstractSource<T[]> {
  constructor(private readonly source: Source<T>) {
    super();
  }

  connect(sink: Sink<T[]>): Stream {
    const toArrayStream = new ToArrayStream<T>(sink);
    const upstream = this.source.connect(toArrayStream);
    toArrayStream._setUpstream(upstream);
    return toArrayStream;
  }
}

/**
 * Collect all upstream values into an array, emit the array on complete.
 *
 * As a collector with `stream()`:
 * ```ts
 * stream(fromArray([1, 2, 3]), toArray()) // [1, 2, 3]
 * ```
 *
 * Mid-pipeline:
 * ```ts
 * pipe(source, toArray()) // Source<T[]>
 * ```
 */
export function toArray<T>(): Operator<T, T[]> {
  return (source) => new ToArraySource(source);
}
