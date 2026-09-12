// ---------------------------------------------------------------------------
// PauseBuffer — the buffered-pause state machine
//
// Shared by pausable(), relay subscriptions, lines() and scheduleOn(). Owns a
// queue of values, the downstream's pause state and one pending terminal
// event, and delivers to a sink under the stream protocol:
//
//   - nothing is delivered before the first resume()
//   - values are delivered directly while unpaused, queued while paused
//   - a terminal event is delivered at once when the queue is empty, and
//     otherwise held until the queue has drained (values are never lost)
//   - nothing is delivered after a terminal event or dispose
// ---------------------------------------------------------------------------

import type { Sink } from '../types.js';
import { PAUSE } from '../types.js';

/** A pending terminal event. */
export type Terminal = { kind: 'complete' } | { kind: 'error'; error: unknown };

export class PauseBuffer<T> {
  #queue: T[] = [];
  #started = false;
  #paused = true;
  #terminal: Terminal | null = null;
  #terminated = false;
  #disposed = false;
  #draining = false;

  constructor(private readonly sink: Sink<T>) {}

  /** Whether the downstream is currently paused (or not yet started). */
  get paused(): boolean {
    return this.#paused;
  }

  /** Number of queued values. */
  get size(): number {
    return this.#queue.length;
  }

  /** Whether a terminal event has been delivered downstream. */
  get terminated(): boolean {
    return this.#terminated;
  }

  /** Whether a terminal event is queued or has been delivered. */
  get closing(): boolean {
    return this.#terminated || this.#terminal !== null;
  }

  /** Whether the buffer can still accept anything. */
  get active(): boolean {
    return !this.#terminated && !this.#disposed && this.#terminal === null;
  }

  /**
   * Offer a value. Delivered now if unpaused, queued otherwise.
   * Returns `PAUSE` when the downstream is paused after this call.
   */
  push(value: T): undefined | PAUSE {
    if (!this.active) return PAUSE;
    if (this.#paused || this.#draining) {
      this.#queue.push(value);
      return PAUSE;
    }
    return this.#deliver(value);
  }

  /** Offer completion. Delivered now if the queue is empty and started. */
  complete(): void {
    if (!this.active) return;
    this.#terminal = { kind: 'complete' };
    this.#flushTerminal();
  }

  /** Offer an error. Delivered now if the queue is empty and started. */
  error(error: unknown): void {
    if (!this.active) return;
    this.#terminal = { kind: 'error', error };
    this.#flushTerminal();
  }

  /**
   * Downstream resumed. Drains the queue, then any pending terminal.
   * Returns `true` when the queue is empty, the downstream is unpaused and
   * no terminal event has been delivered — i.e. the caller should ask its
   * own upstream for more.
   */
  resume(): boolean {
    if (this.#terminated || this.#disposed) return false;
    this.#started = true;
    this.#paused = false;
    if (this.#draining) return false;
    this.#draining = true;
    try {
      while (this.#queue.length > 0) {
        if (this.#terminated || this.#disposed) return false;
        const value = this.#queue.shift()!;
        if (this.#deliver(value) === PAUSE) break;
      }
    } finally {
      this.#draining = false;
    }
    if (this.#terminated || this.#disposed) return false;
    // A pending terminal follows the last owed value at once, even if that
    // value was answered with PAUSE (terminals are not held back by PAUSE).
    if (this.#terminal && this.#queue.length === 0) {
      this.#flushTerminal();
      return false;
    }
    return !this.#paused && this.#queue.length === 0;
  }

  /** Drop everything. Nothing is delivered afterwards. */
  dispose(): void {
    this.#disposed = true;
    this.#queue.length = 0;
    this.#terminal = null;
  }

  #deliver(value: T): undefined | PAUSE {
    const result = this.sink.next(value);
    if (result === PAUSE) this.#paused = true;
    return result;
  }

  #flushTerminal(): void {
    if (!this.#started || this.#queue.length > 0 || this.#draining) return;
    if (this.#terminated || this.#disposed) return;
    const t = this.#terminal;
    if (!t) return;
    this.#terminal = null;
    this.#terminated = true;
    if (t.kind === 'complete') this.sink.complete();
    else this.sink.error(t.error);
  }
}
