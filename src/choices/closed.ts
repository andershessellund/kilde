// ---------------------------------------------------------------------------
// closed() — Choice that fires when a channel closes
// ---------------------------------------------------------------------------

import type { Choice } from '../choice.js';
import { makeAwaitable } from '../choice.js';
import type { ChannelImpl } from '../channel.js';
import { ChanClosed, ChanCloseListeners } from '../channel.js';
import type { ReadChannel, WriteChannel } from '../channel.js';

/**
 * Create a choice that becomes ready when a channel closes.
 *
 * Pair with `take()` in the same `select()` to handle close gracefully
 * instead of getting a lazy throw.
 *
 * @example
 * ```ts
 * const result = await select({
 *   msg: take(ch),
 *   done: closed(ch),
 * });
 *
 * if (result.tag === 'done') {
 *   // Channel was closed — exit loop
 *   break;
 * }
 * ```
 */
export function closed<T>(ch: ReadChannel<T> | WriteChannel<T>): Choice<{ value: void }> {
  const impl = ch as ChannelImpl<T>;

  return makeAwaitable({
    poll(): { result: { value: void } } | undefined {
      return impl[ChanClosed] ? { result: { value: undefined } } : undefined;
    },

    onReady(notify: () => void): () => void {
      if (impl[ChanClosed]) {
        queueMicrotask(notify);
        return () => {};
      }

      impl[ChanCloseListeners].add(notify);
      return () => {
        impl[ChanCloseListeners].delete(notify);
      };
    },
  });
}
