// ---------------------------------------------------------------------------
// combineLatest — combine latest values from multiple sources
//
// Takes an array of sources. Once every source has emitted at least once,
// emits a tuple of the latest values whenever any source changes.
//
// Backpressure: downstream PAUSE → pause all inputs.
//               downstream resume → resume all inputs.
//
// Completes when ALL inputs complete — but never ahead of a pending tuple:
// if the inputs finish while the downstream is paused with a fresh tuple
// owed, resume() delivers the tuple first. Error from any input → dispose
// all, forward error.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, StreamableSource } from './types.js';
import { PAUSE } from './types.js';
import { AbstractSource } from './abstract-source.js';
import { completeOnResume } from './internal/complete-on-resume.js';

// ---------------------------------------------------------------------------
// Type helpers
// ---------------------------------------------------------------------------

/**
 * Extract value types from a tuple of `Source<T>` into a tuple of `T`.
 *
 * ```ts
 * CombineLatestResult<[Source<number>, Source<string>]>
 * // → [number, string]
 * ```
 */
export type CombineLatestResult<Sources extends readonly Source<any>[]> = {
  -readonly [K in keyof Sources]: Sources[K] extends Source<infer T> ? T : never;
};

// ---------------------------------------------------------------------------
// CombineLatestStream — active connection
// ---------------------------------------------------------------------------

class CombineLatestStream<T extends readonly any[]> implements Stream {
  #streams: (Stream | null)[];
  #latest: any[];
  #hasEmitted: boolean[];
  #remaining: number;
  #completedCount = 0;
  #paused = true;
  #disposed = false;
  #terminated = false;
  #dirty = false;
  #delivering = false;

  constructor(
    sources: readonly Source<any>[],
    private readonly sink: Sink<T>,
  ) {
    const n = sources.length;
    this.#streams = new Array(n);
    this.#latest = new Array(n);
    this.#hasEmitted = new Array(n).fill(false);
    this.#remaining = n;

    for (let i = 0; i < n; i++) {
      this.#streams[i] = sources[i].connect(this.#createInputSink(i));
    }
  }

  #createInputSink(index: number): Sink<any> {
    return {
      next: (value: any): undefined | PAUSE => {
        if (!this.#active) return PAUSE;

        this.#latest[index] = value;

        if (!this.#hasEmitted[index]) {
          this.#hasEmitted[index] = true;
          this.#remaining--;
        }

        // Not all inputs have emitted yet — nothing to deliver
        if (this.#remaining > 0) return undefined;

        // Downstream is paused or we're mid-delivery — mark dirty
        if (this.#paused || this.#delivering) {
          this.#dirty = true;
          return this.#paused ? PAUSE : undefined;
        }

        return this.#deliver();
      },

      complete: () => {
        if (!this.#active) return;
        this.#completedCount++;
        this.#streams[index] = null;
        // If a combined tuple is still owed (dirty while paused or while
        // delivering), completion waits until it has been delivered.
        this.#completeIfDone();
      },

      error: (err: unknown) => {
        if (!this.#active) return;
        this.#terminated = true;
        this.#disposeAll();
        this.sink.error(err);
      },
    };
  }

  get #active(): boolean {
    return !this.#terminated && !this.#disposed;
  }

  get #allCompleted(): boolean {
    return this.#completedCount === this.#streams.length;
  }

  /** Complete the downstream once every input is done and no tuple is owed. */
  #completeIfDone(): void {
    if (!this.#active || !this.#allCompleted) return;
    if (this.#dirty && this.#remaining === 0) {
      // Owed tuple. Deliver it now if we can; otherwise resume() will.
      if (this.#paused || this.#delivering) return;
      this.#deliver();
      if (!this.#active || this.#dirty) return;
    }
    this.#terminated = true;
    this.sink.complete();
  }

  /** Deliver a fresh tuple to the downstream sink. Returns PAUSE or undefined. */
  #deliver(): undefined | PAUSE {
    this.#delivering = true;

    // Drain loop: deliver, then check if dirty was set during delivery
    // (reentrant set() on an input during sink.next()).
    do {
      this.#dirty = false;
      const tuple = [...this.#latest] as unknown as T;
      const result = this.sink.next(tuple);

      if (result === PAUSE) {
        this.#delivering = false;
        this.#paused = true;
        return PAUSE;
      }
    } while (this.#dirty && this.#active);

    this.#delivering = false;
    return undefined;
  }

  resume(): void {
    if (!this.#active) return;
    this.#paused = false;

    // Deliver pending combined value if dirty and all have emitted
    if (this.#dirty && this.#remaining === 0) {
      this.#deliver();
    }

    // Resume all inputs (unless we just got re-paused)
    if (!this.#paused && this.#active) {
      for (let i = 0; i < this.#streams.length; i++) {
        if (this.#paused || !this.#active) break;
        this.#streams[i]?.resume();
      }
    }

    // Drain: input resumes or deliveries may have set dirty again
    while (this.#dirty && this.#remaining === 0 && !this.#paused && this.#active) {
      this.#deliver();
    }

    // All inputs may have completed while a tuple was owed.
    this.#completeIfDone();
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposeAll();
  }

  #disposeAll(): void {
    this.#disposed = true;
    for (let i = 0; i < this.#streams.length; i++) {
      this.#streams[i]?.[Symbol.dispose]();
      this.#streams[i] = null;
    }
  }
}

// ---------------------------------------------------------------------------
// CombineLatestSource — connectable template
// ---------------------------------------------------------------------------

class CombineLatestSource<T extends readonly any[]> extends AbstractSource<T> {
  constructor(private readonly sources: readonly Source<any>[]) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    // Empty sources → complete once, on the first resume
    if (this.sources.length === 0) return completeOnResume(sink);
    return new CombineLatestStream<T>(this.sources, sink);
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Combine the latest values from multiple sources into a tuple.
 *
 * Waits until every input source has emitted at least one value, then emits
 * a tuple `[A, B, ...]` of the latest values. After that, re-emits a fresh
 * tuple whenever any input source produces a new value.
 *
 * Backpressure: when the downstream sink returns `PAUSE`, all inputs are
 * paused. When the downstream calls `resume()`, all inputs are resumed.
 *
 * Completes when **all** inputs complete. If the inputs complete while the
 * downstream is paused and a fresh tuple is pending, that tuple is delivered
 * on the next `resume()` before the completion. An error from any input
 * disposes all others and forwards the error downstream.
 *
 * @example
 * ```ts
 * import { combineLatest, createSignal, fromSignal } from 'kilde';
 *
 * const a = createSignal(1);
 * const b = createSignal('x');
 * const combined = combineLatest([fromSignal(a), fromSignal(b)]);
 * // combined is Source<[number, string]>
 * ```
 */
export function combineLatest<const Sources extends readonly Source<any>[]>(
  sources: [...Sources],
): StreamableSource<CombineLatestResult<Sources>> {
  return new CombineLatestSource<CombineLatestResult<Sources>>(sources);
}
