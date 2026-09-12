// ---------------------------------------------------------------------------
// toAsyncSignal — convert a Source<T> into an AsyncSignal<T>
//
// Cold mode (default): ref-counted. Starts as unavailable, connects on
//   first observer, disconnects on last observer.
// Hot mode: connects immediately, registers teardown with the owner.
// ---------------------------------------------------------------------------

import type { Source, Signal, Stream } from '../types.js';
import type { AsyncValue } from '../async-value.js';
import { loading, available, errored, unavailable, isErrored } from '../async-value.js';
import { createSignal } from '../signal.js';
import { currentOwner } from '../owner.js';
import type { OwnedOptions } from '../owner.js';
import type { AsyncSignal } from '../async-state.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function wrapAsyncSignal<T>(
  signal: Signal<AsyncValue<T>>,
  retry: () => void,
): AsyncSignal<T> {
  Object.defineProperties(signal, {
    retry: { value: retry, configurable: true },
  });
  return signal as AsyncSignal<T>;
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface ToAsyncSignalOptions extends OwnedOptions {
  /**
   * When `true`, connects to the source immediately and registers for
   * structured teardown with the owner (`owner`, else the ambient owner).
   *
   * When `false` (default), the signal is cold/ref-counted — it connects
   * on the first observer and disconnects when the last observer leaves.
   */
  hot?: boolean;
}

// ---------------------------------------------------------------------------
// toAsyncSignal
// ---------------------------------------------------------------------------

/**
 * Convert a `Source<T>` into an `AsyncSignal<T>`.
 *
 * Each emission from the source updates the signal to `available(value)`.
 * Before the first emission the signal is `loading()` (once connected).
 * Source errors produce `errored(error, staleValue)`.
 * Source completion is absorbed — the signal retains its last value.
 *
 * **Cold mode** (default): the signal starts as `unavailable()` and connects
 * to the source when the first observer arrives. When the last observer
 * leaves, the source is disconnected and the signal returns to `unavailable`.
 *
 * **Hot mode** (`{ hot: true }`): the signal starts as `loading()` and
 * connects immediately. A teardown resource is registered with the owner
 * for structured disposal.
 *
 * `retry()` reconnects to the source when the signal is in an errored
 * state — it disposes the old connection and opens a fresh one.
 *
 * @example
 * ```ts
 * // Cold — lazy, ref-counted
 * const sig = toAsyncSignal<number>()(source);
 *
 * // Hot — immediate, owner-scoped
 * const sig = toAsyncSignal<number>({ hot: true })(source);
 * ```
 */
export function toAsyncSignal<T>(
  opts?: ToAsyncSignalOptions,
): (source: Source<T>) => AsyncSignal<T> {
  const hot = opts?.hot ?? false;

  return (source: Source<T>): AsyncSignal<T> => {
    const state = createSignal<AsyncValue<T>>(
      hot ? loading<T>() : unavailable<T>(),
    );
    let upstream: Stream | null = null;
    let lastGoodValue: T | undefined;

    function connectUpstream(): void {
      if (upstream) return;

      state.set(loading<T>(lastGoodValue));

      upstream = source.connect({
        next(value: T): undefined {
          lastGoodValue = value;
          state.set(available(value));
          return undefined;
        },
        complete() {
          // Absorbed — keep last value
          upstream = null;
        },
        error(err: unknown) {
          upstream = null;
          state.set(errored<T>(err, lastGoodValue));
        },
      });
      upstream.resume();
    }

    function disconnectUpstream(): void {
      if (upstream) {
        upstream[Symbol.dispose]();
        upstream = null;
      }
    }

    function retry(): void {
      if (!isErrored(state())) return;
      disconnectUpstream();
      connectUpstream();
    }

    if (hot) {
      // Hot: connect immediately
      connectUpstream();

      // Register teardown with the owner
      (opts?.owner ?? currentOwner()).register(
        { [Symbol.dispose]: () => disconnectUpstream() },
        'toAsyncSignal(hot)',
      );
    } else {
      // Cold: connect/disconnect on observer lifecycle
      state.observe('activate', () => connectUpstream());
      state.observe('deactivate', () => {
        disconnectUpstream();
        state.set(unavailable<T>(lastGoodValue));
      });
    }

    return wrapAsyncSignal(state as Signal<AsyncValue<T>>, retry);
  };
}
