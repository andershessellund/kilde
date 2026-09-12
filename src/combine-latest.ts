// ---------------------------------------------------------------------------
// combineLatest — combine latest values from multiple sources
//
// Takes an array of sources. Once every source has emitted at least once,
// emits a tuple of the latest values whenever any source changes.
//
// Backpressure: downstream PAUSE → pause all inputs.
//               downstream resume → resume all inputs.
//
// Completes when ALL inputs complete. Error from any input → dispose all,
// forward error.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, StreamableSource } from './types.js';
import { PAUSE } from './types.js';
import { AbstractSource } from './abstract-source.js';

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
        if (this.#disposed) return PAUSE;

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
        if (this.#disposed) return;
        this.#completedCount++;
        this.#streams[index] = null;

        if (this.#completedCount === this.#streams.length) {
          this.#disposed = true;
          this.sink.complete();
        }
      },

      error: (err: unknown) => {
        if (this.#disposed) return;
        this.#disposeAll();
        this.sink.error(err);
      },
    };
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
    } while (this.#dirty && !this.#disposed);

    this.#delivering = false;
    return undefined;
  }

  resume(): void {
    if (this.#disposed) return;
    this.#paused = false;

    // Deliver pending combined value if dirty and all have emitted
    if (this.#dirty && this.#remaining === 0) {
      this.#deliver();
    }

    // Resume all inputs (unless we just got re-paused)
    if (!this.#paused && !this.#disposed) {
      for (let i = 0; i < this.#streams.length; i++) {
        if (this.#paused || this.#disposed) break;
        this.#streams[i]?.resume();
      }
    }

    // Drain: input resumes or deliveries may have set dirty again
    while (this.#dirty && this.#remaining === 0 && !this.#paused && !this.#disposed) {
      this.#deliver();
    }
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
    // Empty sources → complete immediately on resume
    if (this.sources.length === 0) {
      return {
        resume() {
          sink.complete();
        },
        [Symbol.dispose]() {},
      };
    }
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
 * Completes when **all** inputs complete. An error from any input disposes
 * all others and forwards the error downstream.
 *
 * @example
 * ```ts
 * import { combineLatest, createSignal, stream, toArray } from 'kilde';
 *
 * const a = createSignal(1);
 * const b = createSignal('x');
 * const combined = combineLatest([a, b]);
 * // combined is Source<[number, string]>
 * ```
 */
export function combineLatest<const Sources extends readonly Source<any>[]>(
  sources: [...Sources],
): StreamableSource<CombineLatestResult<Sources>> {
  return new CombineLatestSource<CombineLatestResult<Sources>>(sources);
}
