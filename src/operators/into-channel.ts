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
//   owner — who tears the pipe down (default: the ambient owner)
//
// Backpressure: when the channel cannot take a value, the value is parked
// in the channel's pending-sender queue and the upstream is paused. The
// channel calls the sender's notify() from inside the taker's poll — i.e.
// from inside someone else's synchronous code. Resuming the upstream right
// there would run the whole producer pipeline re-entrantly inside the
// consumer's take, so the resume is deferred to a microtask.
//
// A terminal event that arrives while a value is parked is applied only
// after that value has been taken: closing the channel would drop it.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import type { Terminal } from '../internal/pause-buffer.js';
import type { WriteChannel } from '../channel.js';
import type { ChannelImpl } from '../channel.js';
import {
  ChanBuf,
  ChanPendingSenders,
  ChanPendingReceivers,
  ChanCloseListeners,
  ChanClosed,
} from '../channel.js';
import type { OwnedOptions } from '../owner.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { AbstractSource } from '../abstract-source.js';
import { registerWithOwner } from '../internal/owned.js';
import type { OwnedRegistration } from '../internal/owned.js';
import { SingleValueStream } from '../internal/single-value-stream.js';

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface IntoChannelOptions extends OwnedOptions {
  /** Whether to close the channel when the source ends. Defaults to `true`. */
  close?: boolean;
}


// ---------------------------------------------------------------------------
// pipeIntoChannel — the active pipe
// ---------------------------------------------------------------------------

function pipeIntoChannel<T>(
  source: Source<T>,
  impl: ChannelImpl<T>,
  closeOnEnd: boolean,
  opts: OwnedOptions | undefined,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let settled = false;
    let upstream: Stream | undefined;
    // Assigned below; an already-disposed owner runs the teardown (and so
    // settle()) synchronously inside registerWithOwner, before it exists.
    let registration: OwnedRegistration | undefined;
    // A value of ours is parked in the channel's pending-sender queue.
    let parked = false;
    // The source ended while a value was parked.
    let deferredTerminal: Terminal | null = null;

    const settle = (fn: () => void) => {
      if (settled) return;
      settled = true;
      impl[ChanCloseListeners].delete(onExternalClose);
      registration?.unregister();
      fn();
    };

    const onExternalClose = () => {
      upstream?.[Symbol.dispose]();
      settle(resolve);
    };

    const end = (terminal: Terminal) => {
      settle(() => {
        if (closeOnEnd && !impl[ChanClosed]) impl.close();
        if (terminal.kind === 'complete') resolve();
        else reject(terminal.error);
      });
    };

    // The channel took our parked value (or closed). Called from inside
    // the taker's poll — defer everything to a microtask (see header).
    const onParkedValueConsumed = () => {
      parked = false;
      queueMicrotask(() => {
        if (settled) return;
        if (impl[ChanClosed]) {
          upstream?.[Symbol.dispose]();
          settle(resolve);
          return;
        }
        if (deferredTerminal) {
          const terminal = deferredTerminal;
          deferredTerminal = null;
          end(terminal);
          return;
        }
        upstream?.resume();
      });
    };

    registration = registerWithOwner(opts?.owner, 'intoChannel', () => {
      if (settled) return;
      upstream?.[Symbol.dispose]();
      settle(() => reject(new StreamDisposedError()));
    });
    if (settled) return; // owner already disposed

    // Channel already closed — resolve immediately
    if (impl[ChanClosed]) {
      settle(resolve);
      return;
    }

    const innerSink: Sink<T> = {
      next(value: T): undefined | PAUSE {
        if (settled) return PAUSE;
        if (impl[ChanClosed]) {
          upstream?.[Symbol.dispose]();
          settle(resolve);
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

        // Buffer full — park the value as a pending sender and PAUSE
        parked = true;
        impl[ChanPendingSenders].push({ value, notify: onParkedValueConsumed });
        return PAUSE;
      },

      complete(): void {
        if (settled) return;
        if (parked) deferredTerminal = { kind: 'complete' };
        else end({ kind: 'complete' });
      },

      error(err: unknown): void {
        if (settled) return;
        if (parked) deferredTerminal = { kind: 'error', error: err };
        else end({ kind: 'error', error: err });
      },
    };

    upstream = source.connect(innerSink);
    impl[ChanCloseListeners].add(onExternalClose);
    upstream.resume();
  });
}

// ---------------------------------------------------------------------------
// IntoChannelSource — the connectable source
// ---------------------------------------------------------------------------

class IntoChannelSource<T> extends AbstractSource<Promise<void>> {
  constructor(
    private readonly source: Source<T>,
    private readonly impl: ChannelImpl<T>,
    private readonly closeOnEnd: boolean,
    private readonly opts: OwnedOptions | undefined,
  ) {
    super();
  }

  connect(sink: Sink<Promise<void>>): Stream {
    return new SingleValueStream(sink, () =>
      pipeIntoChannel(this.source, this.impl, this.closeOnEnd, this.opts),
    );
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
 * receiver is waiting, the value is parked as a pending sender and the
 * upstream source is paused via the `PAUSE` signal; once the value is
 * taken, the upstream is resumed on the next microtask (never from inside
 * the taker's own call). If the source ends while a value is parked, the
 * channel is closed (and the promise settled) only after that value has
 * been taken, so nothing is lost.
 *
 * The pipe is registered with its owner (`options.owner`, else the ambient
 * owner). Owner disposal tears down the upstream and rejects the promise
 * with `StreamDisposedError`; the channel is left open.
 *
 * @param ch — the target channel
 * @param options.close — whether to close the channel when the source ends
 *   (default: `true`)
 * @param options.owner — owner that adopts the pipe (default: ambient owner)
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
  return (source) => new IntoChannelSource(source, ch as ChannelImpl<T>, closeOnEnd, options);
}
