// ---------------------------------------------------------------------------
// put() — Choice to send a value into a channel
//
// For buffered channels, put completes immediately if buffer has space.
// For rendezvous channels, put blocks until a receiver takes the value.
// Sending on a closed channel throws immediately.
// If the channel closes while a put is pending, it becomes a dead end
// (lazy exhaustiveness — throws only if no closed() handler is present).
// ---------------------------------------------------------------------------

import type { Choice } from '../choice.js';
import { makeAwaitable, ChoiceDeadEnd } from '../choice.js';
import type { ChannelImpl } from '../channel.js';
import {
  ChanBuf,
  ChanPendingSenders,
  ChanPendingReceivers,
  ChanClosed,
} from '../channel.js';
import type { WriteChannel } from '../channel.js';
import { UnhandledCloseError } from '../select.js';

/**
 * Create a choice that sends a value into a channel.
 *
 * - Buffered channel with space: completes immediately.
 * - Rendezvous or full buffer: blocks until a receiver is ready.
 * - Closed channel: throws immediately (programming error).
 * - Channel closes while pending: becomes a dead end (lazy throw via select).
 *
 * @example
 * ```ts
 * // Direct await
 * await put(ch, 'hello');
 *
 * // In select (e.g., with timeout)
 * const result = await select({
 *   sent: put(ch, 'hello'),
 *   timeout: timeout(5000),
 * });
 * ```
 */
export function put<T>(ch: WriteChannel<T>, value: T): Choice<{ value: void }> {
  const impl = ch as ChannelImpl<T>;

  if (impl[ChanClosed]) {
    throw new Error('Cannot send on a closed channel');
  }

  // done = true once the value has been delivered, either by poll() itself
  // or by take() consuming our pending-sender entry.
  let done = false;

  const choice = makeAwaitable({
    poll(): { result: { value: void } } | undefined {
      if (done) return { result: { value: undefined } };

      if (impl[ChanClosed]) {
        // Channel closed while we were waiting — dead end
        (choice as any)[ChoiceDeadEnd] = new UnhandledCloseError();
        return undefined;
      }

      const buf = impl[ChanBuf];

      // If there's a pending receiver, deliver directly (rendezvous)
      if (impl[ChanPendingReceivers].length > 0) {
        done = true;
        buf.push(value);
        const receiver = impl[ChanPendingReceivers].shift()!;
        receiver.notify();
        return { result: { value: undefined } };
      }

      // If buffer has space, enqueue
      if (!buf.isFull) {
        done = true;
        buf.push(value);
        // Notify any pending receivers about the new value
        if (impl[ChanPendingReceivers].length > 0) {
          const receiver = impl[ChanPendingReceivers].shift()!;
          receiver.notify();
        }
        return { result: { value: undefined } };
      }

      return undefined;
    },

    onReady(notifyFn: () => void): () => void {
      if (done) return () => {};

      if (impl[ChanClosed]) {
        // Channel closed while we were waiting — dead end
        (choice as any)[ChoiceDeadEnd] = new UnhandledCloseError();
        queueMicrotask(notifyFn);
        return () => {};
      }

      const buf = impl[ChanBuf];

      // If we can deliver now (pending receiver or buffer space), notify immediately
      if (impl[ChanPendingReceivers].length > 0 || !buf.isFull) {
        queueMicrotask(notifyFn);
        return () => {};
      }

      // Block — register as pending sender.
      // Wrap notify: when take() consumes our entry, done is set.
      // When channel closes, mark dead end instead.
      const sender = {
        value,
        notify() {
          if (impl[ChanClosed]) {
            // Channel closed — our value was NOT delivered
            (choice as any)[ChoiceDeadEnd] = new UnhandledCloseError();
          } else {
            done = true;
          }
          notifyFn();
        },
      };
      impl[ChanPendingSenders].push(sender);

      return () => {
        const idx = impl[ChanPendingSenders].indexOf(sender);
        if (idx >= 0) {
          impl[ChanPendingSenders].splice(idx, 1);
        }
      };
    },
  });

  return choice;
}
