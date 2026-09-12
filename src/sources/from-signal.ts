// ---------------------------------------------------------------------------
// fromSignal — Source<T> factory that bridges a Signal into the stream world
//
// Uses signal.observe() internally. Provides the full Source/Sink/Stream
// protocol including pause/resume and backpressure.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Signal } from '../types.js';
import { PAUSE } from '../types.js';

class SignalStream<T> implements Stream {
  #signal: Signal<T>;
  #sink: Sink<T>;
  #unsubscribe: (() => void) | null = null;
  #paused = true;
  #disposed = false;

  constructor(signal: Signal<T>, sink: Sink<T>) {
    this.#signal = signal;
    this.#sink = sink;
  }

  resume(): void {
    if (this.#disposed) return;
    this.#paused = false;

    if (!this.#unsubscribe) {
      // First resume — start observing. observe() delivers current value
      // immediately (synchronously), so the sink gets the initial value
      // as part of this resume() call.
      this.#unsubscribe = this.#signal.observe('value', (value: T) => {
        if (this.#paused || this.#disposed) return;
        const result = this.#sink.next(value);
        if (result === PAUSE) {
          this.#paused = true;
        }
      });
    } else {
      // Subsequent resume after pause — deliver current value
      const result = this.#sink.next(this.#signal());
      if (result === PAUSE) {
        this.#paused = true;
      }
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
