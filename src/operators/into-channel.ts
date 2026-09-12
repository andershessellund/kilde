// ---------------------------------------------------------------------------
// intoChannel(ch, options?) — operator that pipes a source into a channel
//
// Terminal operator: consumes all upstream values and forwards them into
// the channel, respecting backpressure. Returns a Promise<void> that
// resolves when the source completes (or the channel closes externally).
//
// Usage:
//   const promise = source.stream(intoChannel(ch));
//   await promise;
//
// Options:
//   close — whether to close the channel when the source ends (default: true)
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import type { WriteChannel } from '../channel.js';
import type { ChannelImpl, PendingSender } from '../channel.js';
import {
  ChanBuf,
  ChanPendingSenders,
  ChanPendingReceivers,
  ChanCloseListeners,
  ChanClosed,
} from '../channel.js';
import { AbstractSource } from '../abstract-source.js';

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface IntoChannelOptions {
  /** Whether to close the channel when the source ends. Defaults to `true`. */
  close?: boolean;
}

// ---------------------------------------------------------------------------
// IntoChannelStream — the active connection
// ---------------------------------------------------------------------------

class IntoChannelStream<T> implements Stream {
  #disposed = false;
  #teardown: (() => void) | null = null;

  constructor(
    private readonly outerSink: Sink<Promise<void>>,
    private readonly source: Source<T>,
    private readonly impl: ChannelImpl<T>,
    private readonly closeOnEnd: boolean,
  ) {}

  resume(): void {
    if (this.#disposed) return;

    const impl = this.impl;
    const closeOnEnd = this.closeOnEnd;
    let upstream: Stream | null = null;

    let resolveP!: () => void;
    let rejectP!: (err: unknown) => void;
    const promise = new Promise<void>((res, rej) => {
      resolveP = res;
      rejectP = rej;
    });

    let settled = false;

    const onExternalClose = () => {
      if (upstream) upstream[Symbol.dispose]();
      settle(() => resolveP());
    };

    const settle = (fn: () => void) => {
      if (settled) return;
      settled = true;
      impl[ChanCloseListeners].delete(onExternalClose);
      this.#teardown = null;
      fn();
    };

    this.#teardown = () => {
      if (upstream) upstream[Symbol.dispose]();
      settle(() => resolveP());
    };

    // Channel already closed — resolve immediately
    if (impl[ChanClosed]) {
      settle(() => resolveP());
      this.outerSink.next(promise);
      this.outerSink.complete();
      return;
    }

    const innerSink: Sink<T> = {
      next(value: T): undefined | typeof PAUSE {
        if (impl[ChanClosed]) {
          if (upstream) upstream[Symbol.dispose]();
          settle(() => resolveP());
          return PAUSE;
        }

        const buf = impl[ChanBuf];

        // Deliver to a pending receiver (rendezvous)
        if (impl[ChanPendingReceivers].length > 0) {
          buf.push(value);
          const receiver = impl[ChanPendingReceivers].shift()!;
          receiver.notify();
          return undefined;
        }

        // Buffer has space
        if (!buf.isFull) {
          buf.push(value);
          return undefined;
        }

        // Buffer full — register as pending sender and PAUSE
        const sender: PendingSender<T> = {
          value,
          notify() {
            if (upstream && !impl[ChanClosed]) {
              upstream.resume();
            } else if (upstream && impl[ChanClosed]) {
              upstream[Symbol.dispose]();
              settle(() => resolveP());
            }
          },
        };
        impl[ChanPendingSenders].push(sender);
        return PAUSE;
      },

      complete(): void {
        settle(() => {
          if (closeOnEnd && !impl[ChanClosed]) impl.close();
          resolveP();
        });
      },

      error(err: unknown): void {
        settle(() => {
          if (closeOnEnd && !impl[ChanClosed]) impl.close();
          rejectP(err);
        });
      },
    };

    upstream = this.source.connect(innerSink);

    if (!impl[ChanClosed]) {
      impl[ChanCloseListeners].add(onExternalClose);
    }

    upstream.resume();

    this.outerSink.next(promise);
    this.outerSink.complete();
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#teardown?.();
  }
}

// ---------------------------------------------------------------------------
// IntoChannelSource — the connectable source
// ---------------------------------------------------------------------------

class IntoChannelSource<T> extends AbstractSource<Promise<void>> {
  constructor(
    private readonly source: Source<T>,
    private readonly impl: ChannelImpl<T>,
    private readonly closeOnEnd: boolean,
  ) {
    super();
  }

  connect(sink: Sink<Promise<void>>): Stream {
    return new IntoChannelStream(sink, this.source, this.impl, this.closeOnEnd);
  }
}

// ---------------------------------------------------------------------------
// intoChannel() — public API
// ---------------------------------------------------------------------------

/**
 * Operator that pipes all upstream values into a channel.
 *
 * Returns a `Promise<void>` (via `stream()`) that resolves when the source
 * completes or the channel is closed externally. Rejects if the source errors.
 *
 * Respects channel backpressure: when the channel buffer is full and no
 * receiver is waiting, the upstream source is paused via the `PAUSE` signal.
 *
 * @param ch — the target channel
 * @param options.close — whether to close the channel when the source ends
 *   (default: `true`)
 *
 * @example
 * ```ts
 * // Pipe array into channel, closing when done
 * await source.stream(intoChannel(ch));
 *
 * // Keep channel open for other producers
 * await source.stream(intoChannel(ch, { close: false }));
 * ```
 */
export function intoChannel<T>(
  ch: WriteChannel<T>,
  options?: IntoChannelOptions,
): Operator<T, Promise<void>> {
  const closeOnEnd = options?.close ?? true;
  return (source) => new IntoChannelSource(source, ch as ChannelImpl<T>, closeOnEnd);
}
