// ---------------------------------------------------------------------------
// Choice<T> — the selectable protocol
//
// A Choice represents one possible outcome in a select(). Anything that
// implements poll() + onReady() can participate in select.
//
// T describes the full result shape (minus tag) that select() will return.
// For example, take on a string channel is Choice<{ value: string, channel: ReadChannel<string> }>.
//
// Choices are awaitable — the then() mixin delegates to a single-choice
// select, so `await take(ch)` works and returns just the value.
//
// choice.ts and select.ts import each other. That is safe because neither
// uses the other's exports during module evaluation, only inside functions.
// Importing select here (rather than wiring it in from select.ts at load
// time) keeps `await timeout(ms)` working when a bundler tree-shakes modules
// nothing else imports.
// ---------------------------------------------------------------------------

import { select } from './select.js';

// ---------------------------------------------------------------------------
// ChoiceAwaitValue — extracts the `.value` field for direct await
// ---------------------------------------------------------------------------

/** Extract the `.value` field from a choice result for direct await. */
export type ChoiceAwaitValue<T> = T extends { value: infer V } ? V : never;

// ---------------------------------------------------------------------------
// Choice<T> — core protocol
// ---------------------------------------------------------------------------

/**
 * A selectable outcome for use with `select()`.
 *
 * T is the result shape that select() merges with `{ tag }`. It should
 * contain at least `{ value: V }` — that V is what direct `await` resolves to.
 *
 * - `poll()` — check if this choice is ready now. Returns `{ result: T }`
 *   if ready, `undefined` if not.
 * - `onReady(notify)` — register a callback for when this choice becomes
 *   ready. Returns a cleanup function to unregister.
 * - `then()` — makes the choice directly awaitable. Resolves to `.value`.
 */
export interface Choice<T> {
  poll(): { result: T } | undefined;
  onReady(notify: () => void): () => void;
  then<R1 = ChoiceAwaitValue<T>, R2 = never>(
    onFulfilled?: ((value: ChoiceAwaitValue<T>) => R1 | PromiseLike<R1>) | null,
    onRejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): Promise<R1 | R2>;
}

// ---------------------------------------------------------------------------
// Dead-end signaling — for lazy exhaustiveness
// ---------------------------------------------------------------------------

/**
 * @internal Symbol for a dead-end error on a choice.
 * When set, indicates this choice can never produce a value and the select
 * should throw with this error if no other choice can provide a value.
 */
export const ChoiceDeadEnd: unique symbol = Symbol('choice.deadEnd');

// ---------------------------------------------------------------------------
// makeAwaitable — mixin that adds then()
// ---------------------------------------------------------------------------

/**
 * Add a `then()` method to a choice, making it directly awaitable.
 *
 * The `then()` delegates to a single-choice select, so all select
 * semantics (lazy exhaustiveness, etc.) apply.
 * Resolves to `.value` from the result T.
 */
export function makeAwaitable<T>(choice: {
  poll(): { result: T } | undefined;
  onReady(notify: () => void): () => void;
}): Choice<T> {
  (choice as any).then = function then(
    onFulfilled?: ((value: ChoiceAwaitValue<T>) => any) | null,
    onRejected?: ((reason: unknown) => any) | null,
  ): Promise<any> {
    return select({ _: this as Choice<T> })
      .then((r) => (r as unknown as { value: ChoiceAwaitValue<T> }).value)
      .then(onFulfilled, onRejected);
  };
  return choice as Choice<T>;
}
