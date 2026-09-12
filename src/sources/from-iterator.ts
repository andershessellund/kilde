// ---------------------------------------------------------------------------
// fromIterator — synchronous source from an Iterator
//
// Pulls values lazily from the iterator. Respects PAUSE — stops pulling
// and resumes from where it left off when resume() is called again.
// ---------------------------------------------------------------------------

import type { Sink, Stream, StreamableSource } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class FromIteratorStream<T> implements Stream {
  #disposed = false;

  constructor(
    private readonly sink: Sink<T>,
    private readonly iterator: Iterator<T>,
  ) {}

  resume(): void {
    while (!this.#disposed) {
      const result = this.iterator.next();
      if (result.done) {
        if (!this.#disposed) {
          this.sink.complete();
        }
        return;
      }
      const pause = this.sink.next(result.value);
      if (pause === PAUSE) {
        return;
      }
    }
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
    // Call iterator.return() if available for cleanup
    this.iterator.return?.();
  }
}

class FromIteratorSource<T> extends AbstractSource<T> {
  constructor(private readonly iterator: Iterator<T>) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    return new FromIteratorStream(sink, this.iterator);
  }
}

/**
 * Create a synchronous source from an `Iterator<T>`.
 *
 * Pulls values lazily. Respects backpressure (PAUSE) — stops pulling
 * and resumes from where it left off.
 *
 * Note: An iterator is stateful and single-use. The returned source
 * should only be connected once.
 *
 * @example
 * ```ts
 * function* naturals() { let i = 0; while (true) yield i++; }
 * stream(fromIterator(naturals()), take(3), toArray()) // [0, 1, 2]
 * ```
 */
export function fromIterator<T>(iterator: Iterator<T>): StreamableSource<T> {
  return new FromIteratorSource(iterator);
}
