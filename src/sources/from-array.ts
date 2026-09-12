// ---------------------------------------------------------------------------
// fromArray — synchronous source from an ArrayLike
//
// Emits all elements in order. Respects PAUSE — stops emitting and resumes
// from where it left off when resume() is called again.
// ---------------------------------------------------------------------------

import type { Sink, Stream, StreamableSource } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class FromArrayStream<T> implements Stream {
  #index = 0;
  #completed = false;
  #disposed = false;
  #delivering = false;

  constructor(
    private readonly sink: Sink<T>,
    private readonly array: ArrayLike<T>,
  ) {}

  resume(): void {
    // A resume() re-entered from inside next() is a no-op: the outer loop
    // is still delivering.
    if (this.#completed || this.#disposed || this.#delivering) return;
    this.#delivering = true;
    try {
      while (this.#index < this.array.length) {
        const result = this.sink.next(this.array[this.#index++]);
        if (this.#disposed || result === PAUSE) return;
      }
    } finally {
      this.#delivering = false;
    }
    this.#completed = true;
    this.sink.complete();
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
  }
}

class FromArraySource<T> extends AbstractSource<T> {
  constructor(private readonly array: ArrayLike<T>) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    return new FromArrayStream(sink, this.array);
  }
}

/**
 * Create a synchronous source from an `ArrayLike<T>`.
 *
 * Emits all elements in order, respecting backpressure (PAUSE).
 *
 * @example
 * ```ts
 * stream(fromArray([1, 2, 3]), toArray()) // [1, 2, 3]
 * stream(fromArray([1, 2, 3]), reduce((a, b) => a + b, 0)) // 6
 * ```
 */
export function fromArray<T>(array: ArrayLike<T>): StreamableSource<T> {
  return new FromArraySource(array);
}

/**
 * Create a synchronous source from variadic arguments.
 *
 * Sugar for `fromArray(values)`.
 *
 * @example
 * ```ts
 * stream(of(1, 2, 3), toArray()) // [1, 2, 3]
 * ```
 */
export function of<T>(...values: T[]): StreamableSource<T> {
  return new FromArraySource(values);
}
