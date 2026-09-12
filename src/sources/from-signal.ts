// ---------------------------------------------------------------------------
// fromSignal — Source<T> factory that bridges a Signal into the stream world
//
// Uses signal.observe() internally. Provides the full Source/Sink/Stream
// protocol including pause/resume and backpressure.
//
// Conflation: while the sink is paused, changes are not queued. The stream
// only remembers *that* the signal changed (a dirty flag) and delivers the
// signal's current value on the next resume(). resume() while already
// active is a no-op — it never re-delivers a value the sink has seen.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Signal } from '../types.js';
import { PAUSE } from '../types.js';

class SignalStream<T> implements Stream {
  #signal: Signal<T>;
  #sink: Sink<T>;
  #unsubscribe: (() => void) | null = null;
  #paused = true;
  #dirty = false;
  #disposed = false;

  constructor(signal: Signal<T>, sink: Sink<T>) {
    this.#signal = signal;
    this.#sink = sink;
  }

  resume(): void {
    if (this.#disposed) return;

    if (!this.#unsubscribe) {
      // First resume — start observing. observe() delivers the current
      // value immediately (synchronously), so the sink gets the initial
      // value as part of this resume() call.
      this.#paused = false;
      this.#unsubscribe = this.#signal.observe('value', (value: T) => {
        if (this.#disposed) return;
        if (this.#paused) {
          // Conflate: remember that something changed, deliver on resume.
          this.#dirty = true;
          return;
        }
        this.#deliver(value);
      });
      return;
    }

    if (!this.#paused) return; // already active — nothing to do

    this.#paused = false;
    if (this.#dirty) {
      this.#dirty = false;
      this.#deliver(this.#signal());
    }
  }

  #deliver(value: T): void {
    const result = this.#sink.next(value);
    if (result === PAUSE) {
      this.#paused = true;
    }
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    if (this.#unsubscribe) {
      this.#unsubscribe();
      this.#unsubscribe = null;
    }
  }
}

/**
 * Create a `Source<T>` from a `Signal<T>`.
 *
 * Bridges signals into the stream protocol. Each `connect()` call
 * creates an independent subscription. The stream starts paused;
 * calling `resume()` delivers the current value immediately, then
 * delivers subsequent changes.
 *
 * Backpressure conflates: while the sink is paused, intermediate values are
 * dropped and only the signal's current value is delivered on the next
 * `resume()` — and only if the signal changed while paused. `resume()` on an
 * already-active stream is a no-op.
 *
 * A signal never completes, so the stream ends only when disposed.
 *
 * @example
 * ```ts
 * const count = createSignal(0);
 * const source = fromSignal(count);
 * const stream = source.connect(mySink);
 * stream.resume(); // mySink.next(0) called immediately
 * count.set(1);    // mySink.next(1)
 * ```
 */
export function fromSignal<T>(signal: Signal<T>): Source<T> {
  return {
    connect(sink: Sink<T>): Stream {
      return new SignalStream(signal, sink);
    },
  };
}
