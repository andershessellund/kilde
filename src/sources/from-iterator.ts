// ---------------------------------------------------------------------------
// fromIterator — synchronous source from an Iterator
//
// Pulls values lazily from the iterator. Respects PAUSE — stops pulling
// and resumes from where it left off when resume() is called again.
//
// An exception thrown by iterator.next() is routed to sink.error() and ends
// the stream. An exception thrown by iterator.return() during dispose is
// swallowed — dispose must not throw.
// ---------------------------------------------------------------------------

import type { Sink, Stream, StreamableSource } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class FromIteratorStream<T> implements Stream {
  #terminated = false;
  #disposed = false;

  constructor(
    private readonly sink: Sink<T>,
    private readonly iterator: Iterator<T>,
  ) {}

  resume(): void {
    while (!this.#terminated && !this.#disposed) {
      let result: IteratorResult<T>;
      try {
        result = this.iterator.next();
      } catch (err) {
        // The iterator is broken — it is done as far as we are concerned,
        // so return() is not called on it.
        this.#terminated = true;
        this.sink.error(err);
        return;
      }
      if (result.done) {
        this.#terminated = true;
        this.sink.complete();
        return;
      }
      const pause = this.sink.next(result.value);
      if (pause === PAUSE) return;
    }
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    // Give the iterator a chance to clean up — but only if it is still
    // running. A finished or broken iterator has nothing left to release.
    if (this.#terminated) return;
    try {
      this.iterator.return?.();
    } catch {
      // dispose() never throws
    }
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
 * If `iterator.next()` throws, the error is delivered to `sink.error()` and
 * the stream ends. Disposing the stream calls `iterator.return()` (if
 * present) while the iterator is still running; an exception thrown from
 * `return()` is swallowed.
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
