// ---------------------------------------------------------------------------
// timeout() — Choice that fires after a delay
//
// The deadline is fixed at creation. The timer itself runs only while some
// select() is waiting on the choice: it starts with the first listener and
// is cleared when the last listener leaves, so a select that settles on
// another branch leaves no timer behind and does not keep the process alive.
// ---------------------------------------------------------------------------

import type { Choice } from '../choice.js';
import { makeAwaitable } from '../choice.js';

/**
 * Create a choice that becomes ready `ms` milliseconds after it is created.
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
  const deadline = Date.now() + Math.max(0, ms);
  let fired = false;
  let timer: ReturnType<typeof setTimeout> | null = null;
  const listeners = new Set<() => void>();

  function isDue(): boolean {
    if (!fired && Date.now() >= deadline) fired = true;
    return fired;
  }

  function fire(): void {
    timer = null;
    fired = true;
    const pending = [...listeners];
    listeners.clear();
    for (const fn of pending) fn();
  }

  function ensureTimer(): void {
    if (timer !== null || fired) return;
    timer = setTimeout(fire, Math.max(0, deadline - Date.now()));
  }

  function clearTimer(): void {
    if (timer === null) return;
    clearTimeout(timer);
    timer = null;
  }

  return makeAwaitable({
    poll(): { result: { value: void } } | undefined {
      return isDue() ? { result: { value: undefined } } : undefined;
    },

    onReady(notify: () => void): () => void {
      if (isDue()) {
        queueMicrotask(notify);
        return () => {};
      }
      listeners.add(notify);
      ensureTimer();
      return () => {
        listeners.delete(notify);
        if (listeners.size === 0) clearTimer();
      };
    },
  });
}
