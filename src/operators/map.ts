// ---------------------------------------------------------------------------
// map — transform each value
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class MapStream<T, R> implements Stream, Sink<T> {
  #disposed = false;
  #upstream!: Stream;

  constructor(
    private readonly sink: Sink<R>,
    private readonly fn: (value: T) => R,
  ) {}

  _setUpstream(upstream: Stream): void {
    this.#upstream = upstream;
  }

  // --- Sink<T> (receives from upstream) ---

  next(value: T): undefined | typeof PAUSE {
    if (this.#disposed) return PAUSE;
    try {
      const mapped = this.fn(value);
      return this.sink.next(mapped);
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

  // --- Stream (exposed to downstream) ---

  resume(): void {
    this.#upstream.resume();
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
    this.#upstream[Symbol.dispose]();
  }
}

class MapSource<T, R> extends AbstractSource<R> {
  constructor(
    private readonly source: Source<T>,
    private readonly fn: (value: T) => R,
  ) {
    super();
  }

  connect(sink: Sink<R>): Stream {
    const mapStream = new MapStream(sink, this.fn);
    const upstream = this.source.connect(mapStream);
    mapStream._setUpstream(upstream);
    return mapStream;
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
  return (source) => new MapSource(source, fn);
}
