// ---------------------------------------------------------------------------
// merge — concurrent inner source merging
//
// Accepts Source<Source<T>>, connects to all inner sources concurrently.
// All inner values are forwarded to the output. Completes when the outer
// source and all inner sources have completed.
//
// Unlike flatten (which serializes), merge runs all inner sources at the
// same time. The outer source is never paused — new inner sources are
// always accepted. Backpressure from downstream pauses all active inner
// streams; resume() resumes them one at a time.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class MergeStream<T> implements Stream {
  #outerStream: Stream;
  #innerStreams = new Map<number, Stream>();
  #nextId = 0;
  #outerCompleted = false;
  #paused = false;
  #disposed = false;

  constructor(
    source: Source<Source<T>>,
    private readonly sink: Sink<T>,
  ) {
    this.#outerStream = source.connect({
      next: (innerSource: Source<T>): undefined | typeof PAUSE => {
        if (this.#disposed) return PAUSE;
        this.#subscribeInner(innerSource);
        // Never pause outer — always accept new inner sources
        return undefined;
      },
      complete: () => {
        this.#outerCompleted = true;
        if (this.#innerStreams.size === 0 && !this.#disposed) {
          this.sink.complete();
        }
      },
      error: (err: unknown) => {
        if (this.#disposed) return;
        this.#disposeAll();
        this.sink.error(err);
      },
    });
  }

  #subscribeInner(innerSource: Source<T>): void {
    const id = this.#nextId++;

    const innerStream = innerSource.connect({
      next: (value: T): undefined | typeof PAUSE => {
        if (this.#disposed) return PAUSE;
        const result = this.sink.next(value);
        if (result === PAUSE) {
          this.#paused = true;
        }
        return result;
      },
      complete: () => {
        this.#innerStreams.delete(id);
        if (this.#outerCompleted && this.#innerStreams.size === 0 && !this.#disposed) {
          this.sink.complete();
        }
      },
      error: (err: unknown) => {
        if (this.#disposed) return;
        this.#disposeAll();
        this.sink.error(err);
      },
    });

    this.#innerStreams.set(id, innerStream);
  }

  resume(): void {
    if (this.#disposed) return;
    this.#paused = false;

    // Resume outer first — may create new inner sources
    this.#outerStream.resume();
    if (this.#paused || this.#disposed) return;

    // Resume inner streams one at a time, stopping if downstream pauses
    for (const innerStream of this.#innerStreams.values()) {
      if (this.#paused || this.#disposed) break;
      innerStream.resume();
    }
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposeAll();
  }

  #disposeAll(): void {
    this.#disposed = true;
    this.#outerStream[Symbol.dispose]();
    for (const innerStream of this.#innerStreams.values()) {
      innerStream[Symbol.dispose]();
    }
    this.#innerStreams.clear();
  }
}

class MergeSource<T> extends AbstractSource<T> {
  constructor(private readonly source: Source<Source<T>>) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    return new MergeStream(this.source, sink);
  }
}

/**
 * Merge all inner sources concurrently into a single output stream.
 *
 * Accepts `Source<Source<T>>` and subscribes to each inner source as it
 * arrives. Values from all inner sources are forwarded to the output.
 * Completes when the outer source and all inner sources have completed.
 *
 * The outer source is **never paused** — new inner sources are always
 * accepted (infinite concurrency). Backpressure from downstream pauses
 * active inner streams; `resume()` resumes them one at a time.
 *
 * Contrast with `flatten()`, which serializes inner sources (waits for
 * each to complete before subscribing to the next).
 *
 * @example
 * ```ts
 * import { stream, fromArray, merge, toArray } from 'kilde';
 *
 * const sources = fromArray([fromArray([1, 2]), fromArray([3, 4])]);
 * stream(sources, merge(), toArray()) // [1, 2, 3, 4]
 * ```
 */
export function merge<T>(): Operator<Source<T>, T> {
  return (source) => new MergeSource(source);
}
