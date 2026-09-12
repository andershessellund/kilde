// ---------------------------------------------------------------------------
// fromChannel(ch) — create a Source<T> that pulls values from a channel
//
// Each connect() creates an independent competing consumer. Values go to
// whichever consumer's take completes first (standard CSP semantics).
//
// The stream starts paused. On resume, it begins pulling values from the
// channel buffer / pending senders. When the sink returns PAUSE, it stops
// pulling until the next resume(). On dispose, it deregisters from the
// channel's pending receiver queue.
//
// Completes when the channel is closed and all buffered values are drained.
// ---------------------------------------------------------------------------

import type { Sink, Stream as StreamConnection, StreamableSource } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';
import type { ReadChannel } from '../channel.js';
import type { ChannelImpl, PendingReceiver } from '../channel.js';
import {
  ChanBuf,
  ChanPendingSenders,
  ChanPendingReceivers,
  ChanCloseListeners,
  ChanClosed,
} from '../channel.js';

// ---------------------------------------------------------------------------
// ChannelStream<T> — active connection pulling from a channel
// ---------------------------------------------------------------------------

class ChannelStream<T> implements StreamConnection {
  #disposed = false;
  #paused = true;
  #pendingReceiver: PendingReceiver | null = null;
  #closeListener: (() => void) | null = null;

  constructor(
    private readonly sink: Sink<T>,
    private readonly impl: ChannelImpl<T>,
  ) {}

  // --- Stream ---

  resume(): void {
    if (this.#disposed) return;
    this.#paused = false;
    this.#pull();
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#paused = true;
    this.#deregister();
  }

  // --- Internal ---

  /** Try to take a value synchronously. If none available, register as pending receiver. */
  #pull(): void {
    while (!this.#paused && !this.#disposed) {
      const result = this.#tryTake();

      if (result !== undefined) {
        const signal = this.sink.next(result.value);
        if (signal === PAUSE) {
          this.#paused = true;
          return;
        }
        // No PAUSE — loop to take the next value
        continue;
      }

      // Nothing available — check if channel is closed + drained
      if (this.impl[ChanClosed]) {
        this.#disposed = true;
        this.sink.complete();
        return;
      }

      // Register as pending receiver and wait
      this.#registerReceiver();
      return;
    }
  }

  /** Try to take a value from buffer or a pending sender. Same logic as take() choice poll(). */
  #tryTake(): { value: T } | undefined {
    const impl = this.impl;
    const buf = impl[ChanBuf];

    // Try buffer first
    if (buf.count > 0) {
      const value = buf.pop();

      // If there are pending senders, move one into the buffer
      if (impl[ChanPendingSenders].length > 0) {
        const sender = impl[ChanPendingSenders].shift()!;
        buf.push(sender.value);
        sender.notify();
      }

      return { value };
    }

    // Try a pending sender directly (rendezvous or empty buffer)
    if (impl[ChanPendingSenders].length > 0) {
      const sender = impl[ChanPendingSenders].shift()!;
      sender.notify();
      return { value: sender.value };
    }

    return undefined;
  }

  /** Register as a pending receiver on the channel. */
  #registerReceiver(): void {
    // Already registered
    if (this.#pendingReceiver) return;

    const receiver: PendingReceiver = {
      notify: () => {
        this.#pendingReceiver = null;
        // Notified — data available or channel closed. Resume pull loop.
        if (!this.#disposed && !this.#paused) {
          this.#pull();
        }
      },
    };

    this.#pendingReceiver = receiver;
    this.impl[ChanPendingReceivers].push(receiver);

    // Also listen for close so we can complete
    if (!this.#closeListener) {
      const onClose = () => {
        this.#closeListener = null;
        // Channel closed — if we're not paused, pull will drain and complete.
        // If we're paused, we'll drain on next resume.
        if (!this.#disposed && !this.#paused) {
          this.#pull();
        }
      };
      this.#closeListener = onClose;
      this.impl[ChanCloseListeners].add(onClose);
    }
  }

  /** Clean up pending receiver registration and close listener. */
  #deregister(): void {
    if (this.#pendingReceiver) {
      const idx = this.impl[ChanPendingReceivers].indexOf(this.#pendingReceiver);
      if (idx >= 0) {
        this.impl[ChanPendingReceivers].splice(idx, 1);
      }
      this.#pendingReceiver = null;
    }

    if (this.#closeListener) {
      this.impl[ChanCloseListeners].delete(this.#closeListener);
      this.#closeListener = null;
    }
  }
}

// ---------------------------------------------------------------------------
// ChannelSource<T>
// ---------------------------------------------------------------------------

class ChannelSource<T> extends AbstractSource<T> {
  constructor(private readonly impl: ChannelImpl<T>) {
    super();
  }

  connect(sink: Sink<T>): StreamConnection {
    return new ChannelStream(sink, this.impl);
  }
}

// ---------------------------------------------------------------------------
// fromChannel() — public API
// ---------------------------------------------------------------------------

/**
 * Create a `Source<T>` that pulls values from a channel.
 *
 * Each `connect()` call creates an independent competing consumer — if
 * multiple sources are connected, each value goes to exactly one of them
 * (standard CSP competing-consumer semantics).
 *
 * The source completes when the channel is closed and all buffered values
 * have been delivered. Disposing the stream stops consuming without
 * affecting the channel.
 *
 * @example
 * ```ts
 * const ch = createChannel<string>(10);
 *
 * // Use as a source with operators
 * fromChannel(ch).stream(
 *   map(s => s.toUpperCase()),
 *   toCallback(console.log),
 * );
 *
 * // Pipe between channels
 * fromChannel(input).stream(
 *   filter(x => x > 0),
 *   intoChannel(output),
 * );
 * ```
 */
export function fromChannel<T>(ch: ReadChannel<T>): StreamableSource<T> {
  return new ChannelSource(ch as ChannelImpl<T>);
}

// Re-export Source type for the return type
