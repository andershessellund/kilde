// ---------------------------------------------------------------------------
// resolved() / rejected() — Promise-based choices
//
// Each promise gets a single .then(onFulfill, onReject) handler shared
// between resolved() and rejected() via a WeakMap cache. Latch semantics —
// once settled, poll() returns immediately forever.
//
// Create these outside loops — you cannot unregister a .then() handler.
// ---------------------------------------------------------------------------

import type { Choice } from '../choice.js';
import { makeAwaitable, ChoiceDeadEnd } from '../choice.js';
import { UnhandledRejectionError } from '../select.js';

// ---------------------------------------------------------------------------
// PromiseState — shared backing for resolved/rejected choices
// ---------------------------------------------------------------------------

type PromiseSettlement<T> =
  | { kind: 'resolved'; value: T }
  | { kind: 'rejected'; reason: unknown };

class PromiseState<T> {
  settlement: PromiseSettlement<T> | null = null;
  listeners = new Set<() => void>();

  constructor(promise: Promise<T>) {
    promise.then(
      (value) => {
        this.settlement = { kind: 'resolved', value };
        this.notify();
      },
      (reason) => {
        this.settlement = { kind: 'rejected', reason };
        this.notify();
      },
    );
  }

  private notify(): void {
    for (const fn of this.listeners) {
      fn();
    }
    this.listeners.clear();
  }
}

const cache = new WeakMap<Promise<any>, PromiseState<any>>();

function getState<T>(promise: Promise<T>): PromiseState<T> {
  let state = cache.get(promise);
  if (!state) {
    state = new PromiseState(promise);
    cache.set(promise, state);
  }
  return state;
}

// ---------------------------------------------------------------------------
// resolved() — fires on promise fulfillment
// ---------------------------------------------------------------------------

/**
 * Create a choice that becomes ready when a promise fulfills.
 *
 * Shares internal state with `rejected()` on the same promise —
 * only one `.then()` handler is registered per promise.
 *
 * Create outside loops — handler cannot be unregistered.
 *
 * If the promise rejects and this is the only choice in a select
 * (no `rejected()` present), the select throws lazily.
 *
 * @example
 * ```ts
 * const data = await resolved(fetchData());
 *
 * // In select with rejection handling:
 * const result = await select({
 *   data: resolved(fetchData()),
 *   error: rejected(fetchData()), // same promise
 *   timeout: timeout(5000),
 * });
 * ```
 */
export function resolved<T>(promise: Promise<T>): Choice<{ value: T }> {
  const state = getState(promise);

  const choice = makeAwaitable({
    poll(): { result: { value: T } } | undefined {
      if (state.settlement?.kind === 'resolved') {
        return { result: { value: state.settlement.value } };
      }
      // If rejected, mark dead-end
      if (state.settlement?.kind === 'rejected') {
        (choice as any)[ChoiceDeadEnd] = new UnhandledRejectionError(state.settlement.reason);
      }
      return undefined;
    },

    onReady(notify: () => void): () => void {
      if (state.settlement) {
        if (state.settlement.kind === 'rejected') {
          (choice as any)[ChoiceDeadEnd] = new UnhandledRejectionError(state.settlement.reason);
        }
        queueMicrotask(notify);
        return () => {};
      }

      // Wrap the listener to check for rejection on settlement
      const wrappedNotify = () => {
        if (state.settlement?.kind === 'rejected') {
          (choice as any)[ChoiceDeadEnd] = new UnhandledRejectionError(state.settlement.reason);
        }
        notify();
      };
      state.listeners.add(wrappedNotify);
      return () => {
        state.listeners.delete(wrappedNotify);
      };
    },
  });

  return choice;
}

// ---------------------------------------------------------------------------
// rejected() — fires on promise rejection
// ---------------------------------------------------------------------------

/**
 * Create a choice that becomes ready when a promise rejects.
 *
 * Shares internal state with `resolved()` on the same promise.
 *
 * @example
 * ```ts
 * const result = await select({
 *   data: resolved(promise),
 *   error: rejected(promise),
 * });
 * if (result.tag === 'error') {
 *   console.error('Failed:', result.value);
 * }
 * ```
 */
export function rejected<T>(promise: Promise<T>): Choice<{ value: unknown }> {
  const state = getState(promise);

  const choice = makeAwaitable({
    poll(): { result: { value: unknown } } | undefined {
      if (state.settlement?.kind === 'rejected') {
        return { result: { value: state.settlement.reason } };
      }
      return undefined;
    },

    onReady(notify: () => void): () => void {
      if (state.settlement) {
        queueMicrotask(notify);
        return () => {};
      }

      state.listeners.add(notify);
      return () => {
        state.listeners.delete(notify);
      };
    },
  });

  return choice;
}
