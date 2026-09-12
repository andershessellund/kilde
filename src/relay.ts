// ---------------------------------------------------------------------------
// Relay — Source + Sink (Subject-like)
//
// Values pushed via next() are multicast to all connected subscribers.
// Each subscriber gets its own PauseBuffer so slow consumers don't block
// fast ones, and so that nothing — not even a terminal event the relay
// received before the subscriber connected — reaches a sink before its
// first resume().
// ---------------------------------------------------------------------------

import type { Sink, Stream, Relay } from './types.js';
import type { PAUSE } from './types.js';
import { AbstractSource } from './abstract-source.js';
import { PauseBuffer } from './internal/pause-buffer.js';

// ---------------------------------------------------------------------------
// Per-subscriber pausable connection
// ---------------------------------------------------------------------------

class RelaySubscription<T> implements Stream {
  readonly #buffer: PauseBuffer<T>;
  #disposed = false;

  constructor(
    private readonly relay: RelayImpl<T>,
    sink: Sink<T>,
  ) {
    this.#buffer = new PauseBuffer<T>(sink);
  }

  /** Push a value from the relay to this subscriber. */
  push(value: T): void {
    if (this.#disposed) return;
    // The relay never applies backpressure to its producer; a paused
    // subscriber simply accumulates.
    this.#buffer.push(value);
  }

  /** Notify this subscriber of completion (after any buffered values). */
  pushComplete(): void {
    if (this.#disposed) return;
    this.#buffer.complete();
    this.#leaveIfTerminated();
  }

  /** Notify this subscriber of an error (after any buffered values). */
  pushError(error: unknown): void {
    if (this.#disposed) return;
    this.#buffer.error(error);
    this.#leaveIfTerminated();
  }

  // --- Stream ---

  resume(): void {
    if (this.#disposed) return;
    this.#buffer.resume();
    this.#leaveIfTerminated();
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#buffer.dispose();
    this.relay._removeSubscription(this);
  }

  #leaveIfTerminated(): void {
    if (this.#buffer.terminated) this.relay._removeSubscription(this);
  }
}

// ---------------------------------------------------------------------------
// RelayImpl<T>
// ---------------------------------------------------------------------------

class RelayImpl<T> extends AbstractSource<T> implements Relay<T> {
  #subscriptions = new Set<RelaySubscription<T>>();
  #completed = false;
  #hasError = false;
  #error: unknown;

  // --- Sink<T> (push side) ---

  next(value: T): undefined | PAUSE {
    if (this.#completed || this.#hasError) return undefined;
    for (const sub of this.#subscriptions) {
      sub.push(value);
    }
    return undefined;
  }

  complete(): void {
    if (this.#completed || this.#hasError) return;
    this.#completed = true;
    for (const sub of [...this.#subscriptions]) {
      sub.pushComplete();
    }
  }

  error(error: unknown): void {
    if (this.#completed || this.#hasError) return;
    this.#hasError = true;
    this.#error = error;
    for (const sub of [...this.#subscriptions]) {
      sub.pushError(error);
    }
  }

  // --- Source<T> (subscribe side) ---

  connect(sink: Sink<T>): Stream {
    const sub = new RelaySubscription(this, sink);
    this.#subscriptions.add(sub);

    // Already ended: the terminal event is queued in the subscription's
    // buffer and delivered on its first resume() — never inside connect().
    if (this.#completed) {
      sub.pushComplete();
    } else if (this.#hasError) {
      sub.pushError(this.#error);
    }

    return sub;
  }

  // --- Internal ---

  _removeSubscription(sub: RelaySubscription<T>): void {
    this.#subscriptions.delete(sub);
  }
}

/**
 * Create a relay — a `Source<T>` and `Sink<T>` combined.
 *
 * Values pushed via `next()` are multicast to all connected subscribers.
 * Each subscriber gets its own pausable buffer for backpressure isolation:
 * values that arrive while a subscriber is paused are queued and drained
 * on its `resume()`, and `complete()`/`error()` reach a subscriber only
 * after its queue has drained. A subscriber that connects after the relay
 * has ended receives the terminal event on its first `resume()`.
 *
 * @example
 * ```ts
 * const relay = createRelay<number>();
 *
 * // Subscribe
 * const s1 = relay.connect({ next: v => console.log(v), complete() {}, error() {} });
 * s1.resume();
 *
 * // Push
 * relay.next(1); // logs 1
 * relay.next(2); // logs 2
 * relay.complete();
 * ```
 */
export function createRelay<T>(): Relay<T> {
  return new RelayImpl<T>();
}
