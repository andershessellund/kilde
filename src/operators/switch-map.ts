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
  #outer: Stream | undefined;
  #inner: Stream | undefined;
  #outerCompleted = false;
  #downstreamPaused = true; // streams start paused
  #terminated = false;
  #disposed = false;

  constructor(
    source: Source<T>,
    private readonly fn: (value: T) => Source<R>,
    private readonly sink: Sink<R>,
  ) {
    this.#outer = source.connect({
      next: (value: T): undefined | PAUSE => {
        if (!this.#active) return PAUSE;

        // Dispose previous inner
        if (this.#inner) {
          this.#inner[Symbol.dispose]();
          this.#inner = undefined;
        }

        // Project — a throwing `fn` fails the stream, it never escapes.
        let innerSource: Source<R>;
        try {
          innerSource = this.fn(value);
        } catch (err) {
          this.#fail(err);
          return PAUSE;
        }

        const inner = innerSource.connect({
          next: (innerValue: R): undefined | PAUSE => {
            if (!this.#active || this.#inner !== inner) return PAUSE;
            const result = this.sink.next(innerValue);
            if (result === PAUSE) this.#downstreamPaused = true;
            return result;
          },
          complete: () => {
            if (!this.#active || this.#inner !== inner) return;
            this.#inner = undefined;
            if (this.#outerCompleted) this.#complete();
          },
          error: (err: unknown) => {
            if (this.#inner !== inner) return;
            this.#fail(err);
          },
        });
        this.#inner = inner;

        // Only resume the inner if the downstream can take values
        if (!this.#downstreamPaused) inner.resume();

        return this.#active ? undefined : PAUSE; // never pause the outer
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
 * is never paused by this operator. An error from the outer, the inner,
 * or a throwing `fn` disposes whatever is still connected and is
 * forwarded downstream.
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
