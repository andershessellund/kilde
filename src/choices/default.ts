// ---------------------------------------------------------------------------
// defaultChoice() — Choice that is always immediately ready
//
// Provides a non-blocking escape hatch in select(), like Go's `default`.
// ---------------------------------------------------------------------------

import type { Choice } from '../choice.js';
import { makeAwaitable } from '../choice.js';

/**
 * Create a choice that is always immediately ready.
 *
 * Use as a non-blocking fallback in `select()` — if no other choice
 * is ready, the default branch fires immediately.
 *
 * Named `defaultChoice` because `default` is a reserved keyword.
 *
 * @example
 * ```ts
 * const result = await select({
 *   msg: take(ch),
 *   none: defaultChoice(),
 * });
 *
 * if (result.tag === 'none') {
 *   // No message available right now
 * }
 * ```
 */
export function defaultChoice(): Choice<{ value: void }> {
  return makeAwaitable({
    poll(): { result: { value: void } } {
      return { result: { value: undefined } };
    },

    onReady(notify: () => void): () => void {
      queueMicrotask(notify);
      return () => {};
    },
  });
}
