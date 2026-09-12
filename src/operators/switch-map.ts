// ---------------------------------------------------------------------------
// switchMap — map each outer value to an inner source, switching on new
//
// When the outer emits, the current inner source (if any) is disposed and
// a new inner is connected. Only the latest inner source is active.
// The outer source is never paused by switchMap itself.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class SwitchMapStream<T, R> implements Stream {
  #outerStream: Stream | undefined;
  #innerStream: Stream | undefined;
  #disposed = false;
  #outerCompleted = false;
  #downstreamPaused = true; // starts paused per protocol

  constructor(
    source: Source<T>,
    private readonly fn: (value: T) => Source<R>,
    private readonly sink: Sink<R>,
  ) {
    this.#outerStream = source.connect({
      next: (value: T): undefined | typeof PAUSE => {
        if (this.#disposed) return PAUSE;

        // Dispose previous inner
        if (this.#innerStream) {
          this.#innerStream[Symbol.dispose]();
          this.#innerStream = undefined;
        }

        // Connect new inner
        const innerSource = this.fn(value);
        this.#innerStream = innerSource.connect({
          next: (innerValue: R): undefined | typeof PAUSE => {
            if (this.#disposed) return PAUSE;
            const result = this.sink.next(innerValue);
            if (result === PAUSE) {
              this.#downstreamPaused = true;
              return PAUSE;
            }
            return undefined;
          },
          complete: () => {
            this.#innerStream = undefined;
            if (this.#outerCompleted) {
              this.sink.complete();
            }
          },
          error: (error: unknown) => {
            this.#innerStream = undefined;
            this.sink.error(error);
            this.#outerStream?.[Symbol.dispose]();
          },
        });

        // Only resume inner if downstream is not paused
        if (!this.#downstreamPaused) {
          this.#innerStream.resume();
        }

        // Never pause outer
        return undefined;
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
    this.#downstreamPaused = false;

    if (this.#innerStream) {
      this.#innerStream.resume();
    } else if (this.#outerStream) {
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

class SwitchMapSource<T, R> extends AbstractSource<R> {
  constructor(
    private readonly source: Source<T>,
    private readonly fn: (value: T) => Source<R>,
  ) {
    super();
  }

  connect(sink: Sink<R>): Stream {
    return new SwitchMapStream(this.source, this.fn, sink);
  }
}

/**
 * Map each value from the outer source to an inner source, switching to
 * the latest. When a new outer value arrives, the current inner source
 * is disposed and a new one is connected.
 *
 * Only the latest inner source is active at any time. The outer source
 * is never paused by this operator.
 *
 * @example
 * ```ts
 * // Each search term switches to a new request, cancelling the previous
 * const results = pipe(
 *   searchTerms,
 *   switchMap(term => fromAsyncFn(() => search(term))),
 *   toArray(),
 * );
 * ```
 */
export function switchMap<T, R>(fn: (value: T) => Source<R>): Operator<T, R> {
  return (source) => new SwitchMapSource(source, fn);
}
