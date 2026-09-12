// ---------------------------------------------------------------------------
// flatten — serialize inner sources into a single output stream
//
// Accepts Source<Source<T>>, connects to each inner source sequentially.
// Pauses the outer source while an inner source is active. When the inner
// completes, resumes the outer to get the next inner source — unless the
// downstream is paused, in which case the next downstream resume() does.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class FlattenStream<T> implements Stream {
  #outer: Stream | undefined;
  #inner: Stream | undefined;
  #outerPaused = false; // we returned PAUSE to the outer and have not resumed it
  #outerCompleted = false;
  #downstreamPaused = true; // streams start paused
  #terminated = false;
  #disposed = false;

  constructor(
    source: Source<Source<T>>,
    private readonly sink: Sink<T>,
  ) {
    this.#outer = source.connect({
      next: (innerSource: Source<T>): undefined | PAUSE => {
        if (!this.#active) return PAUSE;

        const inner = innerSource.connect({
          next: (value: T): undefined | PAUSE => {
            if (!this.#active) return PAUSE;
            const result = sink.next(value);
            if (result === PAUSE) this.#downstreamPaused = true;
            return result;
          },
          complete: () => {
            if (!this.#active || this.#inner !== inner) return;
            this.#inner = undefined;
            if (this.#outerCompleted) {
              this.#complete();
            } else if (this.#outerPaused && !this.#downstreamPaused) {
              // Inner finished asynchronously — fetch the next one.
              this.#outerPaused = false;
              this.#outer?.resume();
            }
            // Otherwise: either we are inside the outer's own delivery
            // (it continues by itself), or the downstream is paused and
            // its resume() will restart the outer.
          },
          error: (err: unknown) => {
            if (this.#inner !== inner) return;
            this.#fail(err);
          },
        });
        this.#inner = inner;

        if (!this.#downstreamPaused) inner.resume();

        if (this.#inner === undefined) {
          // Inner completed synchronously. Keep the outer going unless the
          // downstream paused meanwhile.
          if (this.#downstreamPaused && this.#active) {
            this.#outerPaused = true;
            return PAUSE;
          }
          return this.#active ? undefined : PAUSE;
        }

        // Inner is active (or waiting for the downstream) — pause the outer.
        this.#outerPaused = true;
        return PAUSE;
      },
      complete: () => {
        if (!this.#active) return;
        this.#outerCompleted = true;
        this.#outer = undefined;
        if (!this.#inner) this.#complete();
      },
      error: (err: unknown) => this.#fail(err),
    });
  }

  get #active(): boolean {
    return !this.#terminated && !this.#disposed;
  }

  #complete(): void {
    if (!this.#active) return;
    this.#terminated = true;
    this.sink.complete();
  }

  #fail(err: unknown): void {
    if (!this.#active) return;
    this.#terminated = true;
    // Release whichever side is still open before reporting.
    this.#inner?.[Symbol.dispose]();
    this.#inner = undefined;
    this.#outer?.[Symbol.dispose]();
    this.#outer = undefined;
    this.sink.error(err);
  }

  resume(): void {
    if (!this.#active) return;
    this.#downstreamPaused = false;

    if (this.#inner) {
      this.#inner.resume();
    } else if (this.#outer) {
      this.#outerPaused = false;
      this.#outer.resume();
    }
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#inner?.[Symbol.dispose]();
    this.#inner = undefined;
    this.#outer?.[Symbol.dispose]();
    this.#outer = undefined;
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
 * completes, resumes the outer to get the next inner source. An error from
 * either side disposes the other and is forwarded downstream.
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
