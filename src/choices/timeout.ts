// ---------------------------------------------------------------------------
// timeout() — Choice that fires after a delay
// ---------------------------------------------------------------------------

import type { Choice } from '../choice.js';
import { makeAwaitable } from '../choice.js';

/**
 * Create a choice that becomes ready after `ms` milliseconds.
 *
 * @example
 * ```ts
 * // Direct await
 * await timeout(1000); // waits 1 second
 *
 * // In select
 * const result = await select({
 *   msg: take(ch),
 *   timeout: timeout(5000),
 * });
 * ```
 */
export function timeout(ms: number): Choice<{ value: void }> {
  let fired = false;
  const listeners = new Set<() => void>();

  // Start the timer immediately
  setTimeout(() => {
    fired = true;
    for (const fn of listeners) {
      fn();
    }
    listeners.clear();
  }, ms);

  return makeAwaitable({
    poll(): { result: { value: void } } | undefined {
      return fired ? { result: { value: undefined } } : undefined;
    },

    onReady(notify: () => void): () => void {
      if (fired) {
        // Already fired — notify on next microtask
        queueMicrotask(notify);
        return () => {};
      }

      listeners.add(notify);
      return () => {
        listeners.delete(notify);
        // If no more listeners and timer still active, leave it running —
        // the choice may be polled later or re-entered in another select.
      };
    },
  });
}
