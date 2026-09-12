// ---------------------------------------------------------------------------
// flatten — serialize inner sources into a single output stream
//
// Accepts Source<Source<T>>, connects to each inner source sequentially.
// Pauses the outer source while an inner source is active. When the inner
// completes, resumes the outer to get the next inner source.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class FlattenStream<T> implements Stream {
  #outerStream: Stream | undefined;
  #innerStream: Stream | undefined;
  #outerPaused = true;
  #disposed = false;
  #outerCompleted = false;

  constructor(
    private readonly source: Source<Source<T>>,
    private readonly sink: Sink<T>,
  ) {
    // Connect to outer source
    this.#outerStream = this.source.connect({
      next: (innerSource: Source<T>): undefined | typeof PAUSE => {
        if (this.#disposed) return PAUSE;

        // Connect to inner source
        this.#innerStream = innerSource.connect({
          next: (value: T): undefined | typeof PAUSE => {
            if (this.#disposed) return PAUSE;
            return this.sink.next(value);
          },
          complete: () => {
            // Inner done — resume outer
            this.#innerStream = undefined;
            if (this.#outerPaused && this.#outerStream) {
              this.#outerPaused = false;
              this.#outerStream.resume();
            } else if (!this.#outerStream || this.#outerCompleted) {
              this.sink.complete();
            }
          },
          error: (error: unknown) => {
            this.sink.error(error);
            this.#outerStream?.[Symbol.dispose]();
          },
        });
        this.#innerStream.resume();

        if (!this.#innerStream) {
          // Inner completed synchronously — continue to next
          return undefined;
        }

        // Inner is active — pause outer
        this.#outerPaused = true;
        return PAUSE;
      },
      complete: () => {
        this.#outerCompleted = true;
        this.#outerStream = undefined;
        if (!this.#innerStream) {
          this.sink.complete();
        }
      },
      error: (error: unknown) => {
        this.sink.error(error);
      },
    });
  }

  resume(): void {
    if (this.#disposed) return;

    if (this.#innerStream) {
      this.#innerStream.resume();
    } else if (this.#outerStream) {
      if (this.#outerPaused) {
        this.#outerPaused = false;
      }
      this.#outerStream.resume();
    }
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#innerStream?.[Symbol.dispose]();
    this.#outerStream?.[Symbol.dispose]();
  }
}

class FlattenSource<T> extends AbstractSource<T> {
  constructor(private readonly source: Source<Source<T>>) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    return new FlattenStream(this.source, sink);
  }
}

/**
 * Flatten a source of sources into a single output stream by processing
 * inner sources sequentially.
 *
 * Pauses the outer source while an inner source is active. When the inner
 * completes, resumes the outer to get the next inner source.
 *
 * @example
 * ```ts
 * const sources = fromArray([fromArray([1,2]), fromArray([3,4])]);
 * stream(sources, flatten(), toArray()) // [1, 2, 3, 4]
 * ```
 */
export function flatten<T>(): Operator<Source<T>, T> {
  return (source) => new FlattenSource(source);
}
