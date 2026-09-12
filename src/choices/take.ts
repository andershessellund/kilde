// ---------------------------------------------------------------------------
// take() — Choice to receive a value from a channel
//
// Carries channel reference so tagged results include `result.channel`
// for fan-in identification. Lazy throw on close without closed() handler.
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
import type { ReadChannel } from '../channel.js';

import { UnhandledCloseError } from '../select.js';

// ---------------------------------------------------------------------------
// TakeChoice<T> — receives from a channel
// ---------------------------------------------------------------------------

/**
 * Create a choice that receives the next value from a channel.
 *
 * When used in `select()`, the result carries `result.channel` to
 * identify which channel produced the value (useful for fan-in).
 *
 * If the channel closes and there is no `closed()` choice for this
 * channel in the same `select()`, the select throws (lazy exhaustiveness).
 *
 * @example
 * ```ts
 * // Direct await
 * const msg = await take(ch);
 *
 * // In select
 * const result = await select({
 *   msg: take(ch),
 *   closed: closed(ch),
 *   timeout: timeout(5000),
 * });
 * ```
 *
 * @example Fan-in:
 * ```ts
 * const result = await select({
 *   msg: [take(ch1), take(ch2)],
 * });
 * result.channel; // ch1 or ch2
 * ```
 */
/** Result shape for a take choice. */
export type TakeResult<T> = { value: T; channel: ReadChannel<T> };

export function take<T>(ch: ReadChannel<T>): Choice<TakeResult<T>> {
  const impl = ch as ChannelImpl<T>;

  const choice = makeAwaitable({
    poll(): { result: TakeResult<T> } | undefined {
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

        return { result: { value, channel: ch } };
      }

      // Try a pending sender directly (rendezvous or empty buffer)
      if (impl[ChanPendingSenders].length > 0) {
        const sender = impl[ChanPendingSenders].shift()!;
        sender.notify();
        return { result: { value: sender.value, channel: ch } };
      }

      return undefined;
    },

    onReady(notify: () => void): () => void {
      const buf = impl[ChanBuf];

      // If closed and nothing in buffer/pending senders, this is a dead end
      if (impl[ChanClosed] && buf.count === 0 && impl[ChanPendingSenders].length === 0) {
        (choice as any)[ChoiceDeadEnd] = new UnhandledCloseError();
        queueMicrotask(notify);
        return () => {};
      }

      // If buffer has values or there are pending senders, notify immediately
      if (buf.count > 0 || impl[ChanPendingSenders].length > 0) {
        queueMicrotask(notify);
        return () => {};
      }

      // Register as pending receiver — also listen for close
      const receiver = {
        notify() {
          // On notification, check if this is a close with no data
          if (impl[ChanClosed] && buf.count === 0 && impl[ChanPendingSenders].length === 0) {
            (choice as any)[ChoiceDeadEnd] = new UnhandledCloseError();
          }
          notify();
        },
      };
      impl[ChanPendingReceivers].push(receiver);

      return () => {
        const idx = impl[ChanPendingReceivers].indexOf(receiver);
        if (idx >= 0) {
          impl[ChanPendingReceivers].splice(idx, 1);
        }
      };
    },
  });

  return choice;
}
