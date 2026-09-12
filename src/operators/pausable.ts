// ---------------------------------------------------------------------------
// pausable — insert a buffer between source and sink for backpressure
//
// Accumulates values when downstream is paused. Drains buffer on resume.
// Key infrastructure primitive used internally by flatten and relay.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class PausableStream<T> implements Stream, Sink<T> {
  #upstream!: Stream;
  #buffer: T[] = [];
  #paused = true;
  #disposed = false;
  #completed = false;
  #error: unknown;
  #hasError = false;

  constructor(private readonly sink: Sink<T>) {}

  _setUpstream(upstream: Stream): void {
    this.#upstream = upstream;
  }

  // --- Sink<T> (receives from upstream) ---

  next(value: T): undefined | typeof PAUSE {
    if (this.#disposed) return PAUSE;

    if (this.#paused) {
      this.#buffer.push(value);
      return PAUSE;
    }

    const result = this.sink.next(value);
    if (result === PAUSE) {
      this.#paused = true;
    }
    return result;
  }

  complete(): void {
    if (this.#disposed) return;
    if (this.#paused && this.#buffer.length > 0) {
      // Defer complete until buffer is drained
      this.#completed = true;
    } else {
      this.sink.complete();
    }
  }

  error(error: unknown): void {
    if (this.#disposed) return;
    if (this.#paused && this.#buffer.length > 0) {
      this.#hasError = true;
      this.#error = error;
    } else {
      this.sink.error(error);
    }
  }

  // --- Stream (exposed to downstream) ---

  resume(): void {
    if (this.#disposed) return;

    // Drain buffer first
    const drained = this.#drainBuffer();

    if (this.#disposed) return;

    if (drained === PAUSE) {
      // Sink paused again during drain — stay paused
      return;
    }

    // Buffer drained — check for deferred terminal events
    if (this.#completed) {
      this.sink.complete();
      return;
    }
    if (this.#hasError) {
      this.sink.error(this.#error);
      return;
    }

    // Resume upstream
    this.#paused = false;
    this.#upstream.resume();
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
    this.#buffer.length = 0;
    this.#upstream[Symbol.dispose]();
  }

  // --- Private ---

  #drainBuffer(): undefined | typeof PAUSE {
    while (this.#buffer.length > 0) {
      const value = this.#buffer.shift()!;
      const result = this.sink.next(value);
      if (result === PAUSE) {
        return PAUSE;
      }
    }
    return undefined;
  }
}

class PausableSource<T> extends AbstractSource<T> {
  constructor(private readonly source: Source<T>) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    const pausableStream = new PausableStream(sink);
    const upstream = this.source.connect(pausableStream);
    pausableStream._setUpstream(upstream);
    return pausableStream;
  }
}

/**
 * Insert a buffer between source and sink for backpressure management.
 *
 * Accumulates values in an internal buffer when the downstream sink returns
 * PAUSE. On `resume()`, drains the buffer before resuming upstream.
 *
 * This is a key infrastructure primitive used internally by `flatten()`
 * and `createRelay()`.
 */
export function pausable<T>(): Operator<T, T> {
  return (source) => new PausableSource(source);
}
