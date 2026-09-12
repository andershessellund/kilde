// ---------------------------------------------------------------------------
// Relay — Source + Sink (Subject-like)
//
// Values pushed via next() are multicast to all connected subscribers.
// Each subscriber gets its own pausable buffer so slow consumers don't
// block fast ones.
// ---------------------------------------------------------------------------

import type { Sink, Stream, Relay } from './types.js';
import { PAUSE } from './types.js';
import { AbstractSource } from './abstract-source.js';

// ---------------------------------------------------------------------------
// Per-subscriber pausable connection
// ---------------------------------------------------------------------------

class RelaySubscription<T> implements Stream {
  #buffer: T[] = [];
  #paused = true;
  #disposed = false;
  #completed = false;
  #error: unknown;
  #hasError = false;

  constructor(
    private readonly relay: RelayImpl<T>,
    private readonly sink: Sink<T>,
  ) {}

  /** Push a value from the relay to this subscriber. */
  push(value: T): void {
    if (this.#disposed) return;

    if (this.#paused) {
      this.#buffer.push(value);
      return;
    }

    const result = this.sink.next(value);
    if (result === PAUSE) {
      this.#paused = true;
    }
  }

  /** Notify this subscriber of completion. */
  pushComplete(): void {
    if (this.#disposed) return;
    if (this.#paused && this.#buffer.length > 0) {
      this.#completed = true;
    } else {
      this.sink.complete();
    }
  }

  /** Notify this subscriber of an error. */
  pushError(error: unknown): void {
    if (this.#disposed) return;
    if (this.#paused && this.#buffer.length > 0) {
      this.#hasError = true;
      this.#error = error;
    } else {
      this.sink.error(error);
    }
  }

  // --- Stream ---

  resume(): void {
    if (this.#disposed) return;

    // Drain buffer
    while (this.#buffer.length > 0) {
      const value = this.#buffer.shift()!;
      const result = this.sink.next(value);
      if (result === PAUSE) {
        // Still paused — stop draining
        return;
      }
    }

    if (this.#disposed) return;

    // Check deferred terminal events
    if (this.#completed) {
      this.sink.complete();
      return;
    }
    if (this.#hasError) {
      this.sink.error(this.#error);
      return;
    }

    this.#paused = false;
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#buffer.length = 0;
    this.relay._removeSubscription(this);
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

  next(value: T): undefined | typeof PAUSE {
    if (this.#completed || this.#hasError) return undefined;
    for (const sub of this.#subscriptions) {
      sub.push(value);
    }
    return undefined;
  }

  complete(): void {
    if (this.#completed || this.#hasError) return;
    this.#completed = true;
    for (const sub of this.#subscriptions) {
      sub.pushComplete();
    }
  }

  error(error: unknown): void {
    if (this.#completed || this.#hasError) return;
    this.#hasError = true;
    this.#error = error;
    for (const sub of this.#subscriptions) {
      sub.pushError(error);
    }
  }

  // --- Source<T> (subscribe side) ---

  connect(sink: Sink<T>): Stream {
    const sub = new RelaySubscription(this, sink);
    this.#subscriptions.add(sub);

    // If already completed/errored, notify immediately (deferred until resume)
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
 * Each subscriber gets its own pausable buffer for backpressure isolation.
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
