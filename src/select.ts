// ---------------------------------------------------------------------------
// select() — multi-choice coordination primitive
//
// select({ msg: take(ch), timeout: timeout(5000) })
//
// Object-form API with named branches. Values can be a single Choice or
// an array of Choices (fan-in). Priority follows key insertion order.
// Returns a tagged result: { tag, value } (plus `channel` for take choices).
//
// Lazy exhaustiveness: paired choices (take↔closed, resolved↔rejected)
// throw only when the uncovered termination case fires at runtime.
// ---------------------------------------------------------------------------

import type { Choice } from './choice.js';
import { ChoiceDeadEnd } from './choice.js';

// ---------------------------------------------------------------------------
// SelectMap — the input type for select()
// ---------------------------------------------------------------------------

/** A single choice or array of choices for one branch. */
export type SelectBranch<T> = Choice<T> | Choice<T>[];

/** The object passed to select(). Keys become tags in the result. */
export type SelectMap = Record<string, SelectBranch<any>>;

// ---------------------------------------------------------------------------
// SelectResult — the output type
// ---------------------------------------------------------------------------

/** Extract the result type T from a Choice<T> or Choice<T>[]. */
type ChoiceResult<B> = B extends Choice<infer T>[]
  ? T
  : B extends Choice<infer T>
    ? T
    : never;

/** Result for a single branch — tag from key, rest from Choice<T>. */
type BranchResult<K extends string, B> = { tag: K } & ChoiceResult<B>;

/** The discriminated union result of select(). */
export type SelectResult<M extends SelectMap> = {
  [K in keyof M & string]: BranchResult<K, M[K]>;
}[keyof M & string];

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class UnhandledCloseError extends Error {
  constructor() {
    super(
      `Channel closed in select without a close handler. ` +
        `Add a closed() choice to handle channel close.`,
    );
    this.name = 'UnhandledCloseError';
  }
}

export class UnhandledRejectionError extends Error {
  readonly reason: unknown;
  constructor(reason: unknown) {
    super(
      `Promise rejected in select without a rejected() handler. ` +
        `Add a rejected() choice to handle promise rejection.`,
    );
    this.name = 'UnhandledRejectionError';
    this.reason = reason;
  }
}

// ---------------------------------------------------------------------------
// Internal — flattened entry for processing
// ---------------------------------------------------------------------------

interface FlatEntry {
  tag: string;
  choice: Choice<any>;
}

// ---------------------------------------------------------------------------
// select() implementation
// ---------------------------------------------------------------------------

/**
 * Wait for the first ready choice among named branches.
 *
 * Priority follows key insertion order — if multiple choices are ready
 * simultaneously, the first key wins.
 *
 * Values can be a single `Choice<T>` or an array `Choice<T>[]` (fan-in).
 * Arrays are flattened — any match within the array wins for that tag.
 *
 * @example
 * ```ts
 * const result = await select({
 *   msg: take(ch),
 *   timeout: timeout(5000),
 * });
 *
 * if (result.tag === 'msg') {
 *   console.log(result.value);
 * }
 * ```
 *
 * @example Fan-in from multiple channels:
 * ```ts
 * const result = await select({
 *   msg: [take(ch1), take(ch2)],
 *   timeout: timeout(5000),
 * });
 * ```
 */
export function select<M extends SelectMap>(map: M): Promise<SelectResult<M>> {
  const keys = Object.keys(map);
  if (keys.length === 0) {
    return Promise.reject(new Error('select() requires at least one choice'));
  }

  // Flatten into ordered entries
  const entries: FlatEntry[] = [];
  for (const key of keys) {
    const branch = map[key];
    if (Array.isArray(branch)) {
      for (const choice of branch) {
        entries.push({ tag: key, choice });
      }
    } else {
      entries.push({ tag: key, choice: branch });
    }
  }

  // --- Synchronous poll (priority order) ---
  for (const entry of entries) {
    const result = entry.choice.poll();
    if (result !== undefined) {
      return Promise.resolve(buildResult(entry, result.result) as SelectResult<M>);
    }
  }

  // --- Async: register onReady on all, first wins ---
  return new Promise<SelectResult<M>>((resolve, reject) => {
    let settled = false;
    const cleanups: (() => void)[] = [];

    const cleanup = () => {
      for (const fn of cleanups) {
        fn();
      }
      cleanups.length = 0;
    };

    // When any choice signals ready, re-poll ALL entries in priority order.
    // This handles cases where e.g. a channel close fires onReady for take()
    // but take() returns undefined — the closed() choice (if present) will
    // then be found by the full re-poll.
    const onAnyReady = () => {
      if (settled) return;

      // Re-poll all in priority order
      for (const entry of entries) {
        const result = entry.choice.poll();
        if (result !== undefined) {
          settled = true;
          cleanup();
          resolve(buildResult(entry, result.result) as SelectResult<M>);
          return;
        }
      }

      // Nothing polled — check for dead-end choices (lazy exhaustiveness).
      // If any choice has signaled a dead end (e.g. take on closed channel)
      // and no other choice resolved, throw the dead-end error.
      for (const entry of entries) {
        const deadEnd = (entry.choice as any)[ChoiceDeadEnd];
        if (deadEnd instanceof Error) {
          settled = true;
          cleanup();
          reject(deadEnd);
          return;
        }
      }

      // Legitimate spurious wake — stay registered.
    };

    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i]!;
      const unregister = entry.choice.onReady(onAnyReady);
      cleanups.push(unregister);
    }
  });
}

// ---------------------------------------------------------------------------
// buildResult — construct the tagged result object
// ---------------------------------------------------------------------------

function buildResult(entry: FlatEntry, result: unknown): Record<string, unknown> {
  return {
    tag: entry.tag,
    ...(result as Record<string, unknown>),
  };
}
