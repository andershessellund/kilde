// ---------------------------------------------------------------------------
// AsyncSignal<T> — reactive async state signals
//
// Primitives:
// - mapAsync(signal, fn):            synchronous envelope transform
// - combineAsync(signals):           combine N signals into tuple
// - createAsyncSignal(fn):           cold async signal from Promise factory
// - switchMapAsync(source, fn):      dependent async chain (switch semantics)
// - computedAsync(signals, fn):      sugar over combineAsync + switchMapAsync
// - deriveResource(signal, fn):      hot derived resource (starts immediately)
// - alwaysAvailable(sig):            lift Signal<T> → AsyncSignal<T>
// ---------------------------------------------------------------------------

import type { Stream, Signal, WritableSignal } from './types.js';
import type { AsyncValue } from './async-value.js';
import {
  unavailable,
  loading,
  available,
  errored,
  combineValues,
  isAvailable,
  isErrored,
} from './async-value.js';
import { createSignal, computed } from './signal.js';
import { fromSignal } from './sources/from-signal.js';
import { currentOwner } from './owner.js';
import type { Owner, OwnedOptions, OwnedTask, OwnerHandle } from './owner.js';

// ---------------------------------------------------------------------------
// AsyncSignal<T> — Signal<AsyncValue<T>> + retry()
// ---------------------------------------------------------------------------

/**
 * A reactive signal holding an {@link AsyncValue} with error retry.
 *
 * Extends `Signal<AsyncValue<T>>` — call `signal()` to read the current
 * async value.
 *
 * - `retry()` — error recovery. Recursively retries this signal and all
 *   upstream async signals. **No-op when the signal is not errored.**
 */
export interface AsyncSignal<T> extends Signal<AsyncValue<T>> {
  /** Retry error recovery. Chains to upstream retries. No-op when not errored. */
  retry(): void;
}

/**
 * An async signal that can be reloaded.
 *
 * Extends {@link AsyncSignal} with `reload()` — only present on signals
 * that own async work (e.g. {@link createAsyncSignal}, {@link switchMapAsync},
 * {@link computedAsync}).
 *
 * Signals that are pure derivations ({@link mapAsync}, {@link combineAsync},
 * {@link alwaysAvailable}) do not support reload.
 */
export interface ReloadableAsyncSignal<T> extends AsyncSignal<T> {
  /**
   * Re-run this signal's computation (non-recursive).
   * Returns a `Promise<T>` that settles with the new value.
   * If the signal is already loading, the returned promise coalesces
   * with the in-flight load.
   */
  reload(): Promise<T>;
}

/**
 * A reloadable async signal with explicit lifecycle management.
 *
 * Returned by {@link deriveResource}. The resource is registered with its
 * {@link Owner} for structured teardown. Call `.dispose()` to stop early
 * and unregister from the owner.
 */
export interface ManagedAsyncSignal<T> extends ReloadableAsyncSignal<T> {
  /** Stop the resource and unregister from its owner. */
  dispose(): void;
  /**
   * Begin observing the input. Only needed when the resource was created
   * with `{ start: false }`. Idempotent; a no-op after `dispose()`.
   */
  start(): void;
}

// ---------------------------------------------------------------------------
// Reload waiter — lazy promise settlement for reload() coalescing
// ---------------------------------------------------------------------------

interface ReloadWaiter<T> {
  resolve(value: T): void;
  reject(error: unknown): void;
}

function flushWaiters<T>(waiters: ReloadWaiter<T>[], value: T): void {
  const flushed = waiters.splice(0);
  for (const w of flushed) w.resolve(value);
}

function rejectWaiters<T>(waiters: ReloadWaiter<T>[], error: unknown): void {
  const flushed = waiters.splice(0);
  for (const w of flushed) w.reject(error);
}

// ---------------------------------------------------------------------------
// Internal helpers — attach .retry / .reload / [Symbol.dispose] to existing signals
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

function wrapReloadableAsyncSignal<T>(
  signal: Signal<AsyncValue<T>>,
  retry: () => void,
  reload: () => Promise<T>,
): ReloadableAsyncSignal<T> {
  Object.defineProperties(signal, {
    retry: { value: retry, configurable: true },
    reload: { value: reload, configurable: true },
  });
  return signal as ReloadableAsyncSignal<T>;
}

function wrapManagedAsyncSignal<T>(
  signal: Signal<AsyncValue<T>>,
  retry: () => void,
  reload: () => Promise<T>,
  dispose: () => void,
  start: () => void,
): ManagedAsyncSignal<T> {
  Object.defineProperties(signal, {
    retry: { value: retry, configurable: true },
    reload: { value: reload, configurable: true },
    dispose: { value: dispose, configurable: true },
    start: { value: start, configurable: true },
  });
  return signal as unknown as ManagedAsyncSignal<T>;
}

// ---------------------------------------------------------------------------
// alwaysAvailable — lift a regular Signal<T> into AsyncSignal<T>
// ---------------------------------------------------------------------------

/**
 * Wrap a regular signal as an always-available async signal.
 *
 * The output is an `AsyncSignal<T>` that is always `available(sig())`.
 * `retry()` is a no-op. Useful for mixing regular signals with APIs
 * that accept `AsyncSignal<T>[]` (e.g. `computedAsync`, `deriveResource`).
 *
 * @example
 * ```ts
 * const count = createSignal(0);
 * const asyncCount = alwaysAvailable(count); // always available(0)
 * ```
 */
export function alwaysAvailable<T>(sig: Signal<T>): AsyncSignal<T> {
  return wrapAsyncSignal(
    computed(() => available(sig())),
    () => {}, // no-op retry — always available
  );
}

// ---------------------------------------------------------------------------
// mapAsync — synchronous envelope transform
// ---------------------------------------------------------------------------

/**
 * Transform the envelope of an async signal synchronously.
 *
 * The callback receives the full {@link AsyncValue} and must return a new
 * `AsyncValue`. This is a pure derivation — no async work. The result is
 * a computed signal that updates whenever the source updates.
 *
 * `retry()` delegates to the source signal's `retry()`.
 *
 * Does NOT support `reload()` — use {@link switchMapAsync} or
 * {@link computedAsync} if you need reload.
 *
 * @example
 * ```ts
 * const userId = mapAsync(session, (v) =>
 *   v.status === 'available' && v.value.userId
 *     ? available(v.value.userId)
 *     : unavailable(),
 * );
 * ```
 */
export function mapAsync<A, B>(
  source: AsyncSignal<A>,
  fn: (value: AsyncValue<A>) => AsyncValue<B>,
): AsyncSignal<B> {
  return wrapAsyncSignal(
    computed(() => fn(source())),
    () => source.retry(),
  );
}

// ---------------------------------------------------------------------------
// combineAsync — combine N async signals into a tuple
// ---------------------------------------------------------------------------

/**
 * Combine multiple async signals into a single `AsyncSignal` of a tuple.
 *
 * The output uses {@link combineValues} semantics:
 * - All `available` → `available([...values])`
 * - Any `errored` → `errored` (first error wins)
 * - Any `loading` → `loading`
 * - Otherwise → `unavailable`
 *
 * The result is a cold, ref-counted signal: it activates when the first
 * subscriber connects and deactivates when the last disconnects.
 *
 * `retry()` chains to all input signals.
 * Does NOT support `reload()`.
 *
 * @example
 * ```ts
 * const combined = combineAsync([userSignal, configSignal]);
 * // combined() → AsyncValue<[User, Config]>
 * ```
 */
export function combineAsync<T extends readonly unknown[]>(
  signals: { readonly [K in keyof T]: AsyncSignal<T[K]> },
): AsyncSignal<T> {
  const inner = createSignal<AsyncValue<T>>(unavailable<T>());
  let inputConnections: Stream[] = [];
  let combinedConnection: Stream | null = null;

  const combinedInput = computed(() => {
    const inputValues = signals.map((s) => s()) as {
      readonly [K in keyof T]: AsyncValue<T[K]>;
    };
    return combineValues<T>(...inputValues);
  });

  function activate(): void {
    // Subscribe to input signals to activate ref-counted sources
    for (let i = 0; i < signals.length; i++) {
      const conn = fromSignal(signals[i]).connect({
        next() { return undefined; },
        complete() {},
        error() {},
      });
      conn.resume();
      inputConnections.push(conn);
    }

    // Observe combined input changes
    combinedConnection = fromSignal(combinedInput).connect({
      next(value: AsyncValue<T>) {
        inner.set(value);
        return undefined;
      },
      complete() { combinedConnection = null; },
      error() { combinedConnection = null; },
    });
    combinedConnection.resume();
  }

  function deactivate(): void {
    if (combinedConnection) {
      combinedConnection[Symbol.dispose]();
      combinedConnection = null;
    }
    for (const conn of inputConnections) {
      conn[Symbol.dispose]();
    }
    inputConnections = [];
    inner.set(unavailable<T>());
  }

  inner.observe('activate', activate);
  inner.observe('deactivate', deactivate);

  return wrapAsyncSignal(
    inner as Signal<AsyncValue<T>>,
    () => { for (const sig of signals) sig.retry(); },
  );
}

// ---------------------------------------------------------------------------
// computedAsync — derive an async value from several async signals
// ---------------------------------------------------------------------------

/** Options for {@link computedAsync}. */
export interface ComputedAsyncOptions {
  keepStale?: boolean;
}

/**
 * The shapes a derivation may return, accepted by {@link asAsyncSignal}.
 * - `Promise<R>` — async operation (signal tracks promise state)
 * - `AsyncSignal<R>` — subscribe to inner signal (switch semantics)
 * - `AsyncValue<R>` — static snapshot (e.g. `available(value)`)
 */
export type ComputedAsyncReturn<R> = Promise<R> | AsyncSignal<R> | AsyncValue<R>;

// ---- Helpers for flexible factory return ---------------------------------

/** Wrap an already-started Promise as an AsyncSignal. */
function wrapPromise<T>(promise: Promise<T>): AsyncSignal<T> {
  const state = createSignal<AsyncValue<T>>(loading<T>());
  promise.then(
    (value) => state.set(available(value)),
    (err) => state.set(errored(err)),
  );
  return wrapAsyncSignal(state as Signal<AsyncValue<T>>, () => {});
}

/** Wrap a static AsyncValue as an AsyncSignal. */
function wrapStaticAsyncValue<T>(value: AsyncValue<T>): AsyncSignal<T> {
  const sig = computed<AsyncValue<T>>(() => value);
  return wrapAsyncSignal(sig, () => {});
}

/**
 * Normalise a {@link ComputedAsyncReturn} into an `AsyncSignal`.
 *
 * - a thenable becomes a signal that is `loading` until it settles
 * - an existing `AsyncSignal` is returned as-is
 * - a plain `AsyncValue` becomes a constant signal
 *
 * Useful when building `switchMapAsync` factories that may answer
 * synchronously (`available(x)`) or asynchronously (`fetch(...)`).
 */
export function asAsyncSignal<R>(result: ComputedAsyncReturn<R>): AsyncSignal<R> {
  // Promise<R>
  if (result != null && typeof (result as any).then === 'function') {
    return wrapPromise(result as Promise<R>);
  }
  // AsyncSignal<R> — a callable with .retry
  if (typeof result === 'function' && 'retry' in result) {
    return result as AsyncSignal<R>;
  }
  // AsyncValue<R>
  return wrapStaticAsyncValue(result as AsyncValue<R>);
}

/**
 * Derive an async value from multiple async signals.
 *
 * Sugar over
 * `switchMapAsync(combineAsync(signals), values => createAsyncSignal(() => fn(...values)))`.
 * The result is cold (ref-counted) and reloadable.
 *
 * @example
 * ```ts
 * const overview = computedAsync([user, orders], async (u, o) => ({
 *   name: u.name,
 *   total: o.length,
 * }));
 * ```
 */
export function computedAsync<T extends readonly unknown[], R>(
  signals: { readonly [K in keyof T]: AsyncSignal<T[K]> },
  fn: (...values: NoInfer<T>) => Promise<R>,
  _options?: ComputedAsyncOptions,
): ReloadableAsyncSignal<R> {
  const combined = combineAsync(signals as AsyncSignal<any>[]);
  return switchMapAsync(combined, (values: any[]) =>
    createAsyncSignal(() => fn(...(values as unknown as T))),
  );
}

// ---------------------------------------------------------------------------
// createAsyncSignal — cold async signal from an async function
// ---------------------------------------------------------------------------

/**
 * Create a cold (ref-counted) `ReloadableAsyncSignal<T>` from an async factory.
 *
 * The signal starts as `unavailable`. When the first subscriber connects,
 * the factory is invoked and the signal transitions to `loading`. When the
 * last subscriber disconnects, pending promises are discarded and the
 * signal returns to `unavailable`.
 *
 * - `retry()` — re-invoke the factory on error. No-op when not errored.
 * - `reload()` — re-invoke the factory unconditionally. Returns a promise.
 *
 * @example
 * ```ts
 * const user = createAsyncSignal(() => fetchUser(userId));
 * user();               // read AsyncValue<User>
 * user.retry();          // retry on error
 * await user.reload();   // force re-fetch
 * ```
 */
export function createAsyncSignal<T>(fn: () => Promise<T>): ReloadableAsyncSignal<T> {
  const state: WritableSignal<AsyncValue<T>> = createSignal<AsyncValue<T>>(unavailable<T>());
  let reloadWaiters: ReloadWaiter<T>[] | null = null;
  let version = 0;
  let lastGoodOutput: T | undefined;

  function invoke(): void {
    version++;
    const myVersion = version;
    const stale = lastGoodOutput;
    state.set(loading(stale));

    fn().then(
      (value) => {
        if (myVersion !== version) return;
        lastGoodOutput = value;
        state.set(available(value));
        if (reloadWaiters) flushWaiters(reloadWaiters, value);
      },
      (err) => {
        if (myVersion !== version) return;
        state.set(errored(err, lastGoodOutput));
        if (reloadWaiters) rejectWaiters(reloadWaiters, err);
      },
    );
  }

  function activate(): void {
    invoke();
  }

  function deactivate(): void {
    version++; // invalidate pending
    state.set(unavailable(lastGoodOutput));
  }

  state.observe('activate', activate);
  state.observe('deactivate', deactivate);

  function retry(): void {
    if (state.observed && isErrored(state())) invoke();
  }

  function reload(): Promise<T> {
    if (!state.observed) {
      return Promise.reject(new Error('Cannot reload an inactive (cold) createAsyncSignal. The signal must have at least one subscriber.'));
    }
    if (!reloadWaiters) reloadWaiters = [];
    const current = state();
    if (current.status === 'loading') {
      return new Promise<T>((resolve, reject) => reloadWaiters!.push({ resolve, reject }));
    }
    return new Promise<T>((resolve, reject) => {
      reloadWaiters!.push({ resolve, reject });
      invoke();
    });
  }

  return wrapReloadableAsyncSignal(state as Signal<AsyncValue<T>>, retry, reload);
}

// ---------------------------------------------------------------------------
// switchMapAsync — dependent async chain with switch semantics
// ---------------------------------------------------------------------------

/**
 * Switch-map from a source async signal through a function that produces
 * a new `AsyncSignal<B>` for each available source value.
 *
 * The returned signal is **cold** (ref-counted). When the first subscriber
 * connects, it activates the source. When the source becomes `available`,
 * the factory `fn` is called to produce an inner `AsyncSignal<B>`. If the
 * source changes while an inner signal is active, the old inner signal is
 * disconnected and a new one is created (switch semantics).
 *
 * `retry()` chains to the source, and (if the inner signal exists) to the
 * inner signal as well.
 *
 * `reload()` delegates to the inner signal's `reload()` if it is a
 * {@link ReloadableAsyncSignal}; otherwise throws.
 *
 * @example
 * ```ts
 * const userDetails = switchMapAsync(userId, (id) =>
 *   createAsyncSignal(() => fetchUser(id)),
 * );
 * ```
 */
export function switchMapAsync<A, B>(
  source: AsyncSignal<A>,
  fn: (value: A) => AsyncSignal<B>,
): ReloadableAsyncSignal<B> {
  const inner = createSignal<AsyncValue<B>>(unavailable<B>());
  let sourceConnection: Stream | null = null;
  let innerConnection: Stream | null = null;
  let currentInner: AsyncSignal<B> | null = null;
  let lastGoodOutput: B | undefined;
  let reloadWaiters: ReloadWaiter<B>[] | null = null;

  function detachInner(): void {
    if (innerConnection) {
      innerConnection[Symbol.dispose]();
      innerConnection = null;
    }
    currentInner = null;
  }

  function attachInner(sig: AsyncSignal<B>): void {
    currentInner = sig;
    innerConnection = fromSignal(sig).connect({
      next(value: AsyncValue<B>) {
        inner.set(value);
        if (isAvailable(value)) {
          lastGoodOutput = value.value;
          if (reloadWaiters) flushWaiters(reloadWaiters, value.value);
        } else if (isErrored(value)) {
          if (reloadWaiters) rejectWaiters(reloadWaiters, value.error);
        }
        return undefined;
      },
      complete() { innerConnection = null; },
      error() { innerConnection = null; },
    });
    innerConnection.resume();
  }

  function handleSource(value: AsyncValue<A>): void {
    if (isAvailable(value)) {
      detachInner();
      const newInner = fn(value.value);
      attachInner(newInner);
    } else {
      detachInner();
      const stale = lastGoodOutput;
      switch (value.status) {
        case 'unavailable':
          inner.set(unavailable<B>(stale));
          break;
        case 'errored':
          inner.set(errored<B>(value.error, stale));
          break;
        case 'loading':
          inner.set(loading<B>(stale));
          break;
      }
    }
  }

  function activate(): void {
    sourceConnection = fromSignal(source).connect({
      next(value: AsyncValue<A>) {
        handleSource(value);
        return undefined;
      },
      complete() { sourceConnection = null; },
      error() { sourceConnection = null; },
    });
    sourceConnection.resume();
  }

  function deactivate(): void {
    detachInner();
    if (sourceConnection) {
      sourceConnection[Symbol.dispose]();
      sourceConnection = null;
    }
    inner.set(unavailable<B>(lastGoodOutput));
  }

  inner.observe('activate', activate);
  inner.observe('deactivate', deactivate);

  function retry(): void {
    source.retry();
    if (currentInner) currentInner.retry();
  }

  function reload(): Promise<B> {
    if (!inner.observed) {
      return Promise.reject(new Error('Cannot reload an inactive (cold) switchMapAsync signal. The signal must have at least one subscriber.'));
    }
    // If the inner signal is reloadable, delegate
    if (currentInner && 'reload' in currentInner) {
      return (currentInner as ReloadableAsyncSignal<B>).reload();
    }
    // Otherwise re-create inner from current source
    const srcVal = source();
    if (!isAvailable(srcVal)) {
      return Promise.reject(new Error('Cannot reload switchMapAsync: source is not available.'));
    }
    if (!reloadWaiters) reloadWaiters = [];
    return new Promise<B>((resolve, reject) => {
      reloadWaiters!.push({ resolve, reject });
      detachInner();
      const newInner = fn(srcVal.value);
      attachInner(newInner);
    });
  }

  return wrapReloadableAsyncSignal(inner as Signal<AsyncValue<B>>, retry, reload);
}

// ---------------------------------------------------------------------------
// deriveResource — hot derived resource from a single AsyncSignal
// ---------------------------------------------------------------------------

/** Return value produced by a {@link deriveResource} factory. */
export interface DerivedValue<R> {
  readonly value: R;
  /** Release the resource. Called when it is replaced, and on dispose. */
  [Symbol.dispose]?(): void;
  /** Async alternative to `[Symbol.dispose]`. Its promise is not awaited. */
  [Symbol.asyncDispose]?(): PromiseLike<void>;
}

/**
 * Flexible return type for {@link deriveResource} factories.
 *
 * - `AsyncValue<DerivedValue<R>>` — sync result:
 *   - `available(resource)` → install resource immediately
 *   - `unavailable()` → skip (conditions not met)
 *   - `errored(err)` → set error state
 *   - `loading()` → set loading state (effectively a no-op dead end)
 * - `Promise<DerivedValue<R>>` — async result: runs under the owner
 */
export type DeriveResourceReturn<R> = AsyncValue<DerivedValue<R>> | Promise<DerivedValue<R>>;

/** Options for {@link deriveResource}. */
export interface DeriveResourceOptions extends OwnedOptions {
  /**
   * When `true`, the state carries the last successful output
   * as a stale value during loading/error transitions.
   * Default: `true`.
   */
  keepStale?: boolean;
  /** Name for diagnostics (owner registration, spawned tasks). */
  name?: string;
  /**
   * Begin observing the input immediately. Default: `true`.
   * With `false`, nothing happens until `.start()` is called — useful when
   * the owner is not ready at construction time.
   */
  start?: boolean;
}

/**
 * Derive a disposable resource from a single async signal.
 *
 * The returned signal is **hot**: it starts observing immediately and stays
 * active until disposed. When the input is `available`, the factory runs.
 * If it returns a promise, the work runs as an {@link Owner.spawn} task when
 * the owner supervises tasks. When the input changes mid-flight, the
 * in-flight task is abandoned (and aborted, if supervised) and a new one
 * starts. When the factory resolves, the old resource (if any) is disposed
 * before the new one is installed.
 *
 * For multiple inputs, combine them first:
 * ```ts
 * deriveResource(combineAsync([a, b]), ([va, vb]) => ...)
 * ```
 *
 * The resource registers itself with its owner — `options.owner`, else the
 * ambient owner at creation time — for structured teardown. Calling
 * `.dispose()` stops the resource and unregisters it. Without an owner it
 * simply runs until disposed.
 *
 * @example
 * ```ts
 * const db = deriveResource(config, async (cfg) => {
 *   const pool = await connect(cfg);
 *   return { value: pool, [Symbol.dispose]: () => pool.close() };
 * });
 * ```
 */
export function deriveResource<T, R>(
  signal: AsyncSignal<T>,
  fn: (value: NoInfer<T>) => DeriveResourceReturn<R>,
  options: DeriveResourceOptions = {},
): ManagedAsyncSignal<R> {
  const keepStale = options.keepStale ?? true;
  const name = options.name ?? 'deriveResource';
  const owner: Owner = options.owner ?? currentOwner();

  const inner = createSignal<AsyncValue<R>>(loading<R>());
  let lastGoodOutput: R | undefined;
  let currentResource: DerivedValue<R> | null = null;
  let currentTask: OwnedTask<DerivedValue<R>> | null = null;
  let disposed = false;
  let started = false;
  let inputConnection: Stream | null = null;
  let unsub: (() => void) | null = null;
  let reloadWaiters: ReloadWaiter<R>[] | null = null;

  function disposeCurrentResource(): void {
    if (currentResource) {
      const r = currentResource;
      currentResource = null;
      const sync = r[Symbol.dispose];
      const async = r[Symbol.asyncDispose];
      if (typeof sync === 'function') sync.call(r);
      else if (typeof async === 'function') void async.call(r);
    }
  }

  function abortCurrentTask(): void {
    if (currentTask) {
      currentTask.abort();
      currentTask = null;
    }
  }

  function installSyncResult(result: AsyncValue<DerivedValue<R>>): void {
    const stale = keepStale ? lastGoodOutput : undefined;
    if (isAvailable(result)) {
      disposeCurrentResource();
      currentResource = result.value;
      lastGoodOutput = result.value.value;
      inner.set(available(result.value.value));
      if (reloadWaiters) flushWaiters(reloadWaiters, result.value.value);
    } else {
      disposeCurrentResource();
      switch (result.status) {
        case 'unavailable':
          inner.set(unavailable<R>(stale));
          break;
        case 'errored':
          inner.set(errored<R>(result.error, stale));
          if (reloadWaiters) rejectWaiters(reloadWaiters, result.error);
          break;
        case 'loading':
          inner.set(loading<R>(stale));
          break;
      }
    }
  }

  function handleInputChange(inputValue: AsyncValue<T>): void {
    if (disposed) return;

    // Abandon any in-flight derivation
    abortCurrentTask();

    if (isAvailable(inputValue)) {
      const value = inputValue.value;
      const result = fn(value);

      // Promise path — run under the owner's supervision when available
      if (result != null && typeof (result as any).then === 'function') {
        const stale = keepStale ? lastGoodOutput : undefined;
        inner.set(loading<R>(stale));

        const promise = result as Promise<DerivedValue<R>>;
        const task: OwnedTask<DerivedValue<R>> = owner.spawn
          ? owner.spawn(() => promise, `${name}::derive`)
          : { promise, abort() {} };
        currentTask = task;

        task.promise.then(
          (derived) => {
            if (task !== currentTask || disposed) return; // stale
            currentTask = null;
            disposeCurrentResource();
            currentResource = derived;
            lastGoodOutput = derived.value;
            inner.set(available(derived.value));
            if (reloadWaiters) flushWaiters(reloadWaiters, derived.value);
          },
          (err) => {
            if (task !== currentTask || disposed) return; // stale (aborted)
            currentTask = null;
            inner.set(errored<R>(err, keepStale ? lastGoodOutput : undefined));
            if (reloadWaiters) rejectWaiters(reloadWaiters, err);
          },
        );
      } else {
        // AsyncValue path — sync result, install directly
        installSyncResult(result as AsyncValue<DerivedValue<R>>);
      }
    } else {
      // Dispose current resource when input becomes unavailable
      disposeCurrentResource();
      const stale = keepStale ? lastGoodOutput : undefined;
      switch (inputValue.status) {
        case 'unavailable':
          inner.set(unavailable<R>(stale));
          break;
        case 'errored':
          inner.set(errored<R>(inputValue.error, stale));
          break;
        case 'loading':
          inner.set(loading<R>(stale));
          break;
      }
    }
  }

  // Self-register with the owner for structured teardown
  const resourceHandle: OwnerHandle | undefined = owner.register(
    { [Symbol.dispose]: () => teardown() },
    name,
  );

  function start(): void {
    if (started || disposed) return;
    started = true;

    // Hot: keep the input's ref-counted source active for our lifetime
    inputConnection = fromSignal(signal).connect({
      next() { return undefined; },
      complete() {},
      error() {},
    });
    inputConnection.resume();

    unsub = signal.observe('value', handleInputChange);
  }

  function teardown(): void {
    if (disposed) return;
    disposed = true;
    if (unsub) {
      unsub();
      unsub = null;
    }
    if (inputConnection) {
      inputConnection[Symbol.dispose]();
      inputConnection = null;
    }
    abortCurrentTask();
    disposeCurrentResource();
  }

  function retry(): void {
    if (disposed) return;
    // Chain retry to upstream input (recursive)
    signal.retry();
    // Re-derive only if this signal is errored
    if (isErrored(inner())) handleInputChange(signal());
  }

  function reload(): Promise<R> {
    if (disposed) {
      return Promise.reject(new Error('Cannot reload a disposed deriveResource signal.'));
    }
    if (!started) {
      return Promise.reject(new Error('Cannot reload a deriveResource signal that has not been started.'));
    }
    if (!reloadWaiters) reloadWaiters = [];
    const current = inner();
    if (current.status === 'loading') {
      return new Promise<R>((resolve, reject) => reloadWaiters!.push({ resolve, reject }));
    }
    return new Promise<R>((resolve, reject) => {
      reloadWaiters!.push({ resolve, reject });
      handleInputChange(signal());
    });
  }

  function dispose(): void {
    teardown();
    resourceHandle?.unregister();
    if (reloadWaiters) rejectWaiters(reloadWaiters, new Error('deriveResource disposed during reload.'));
  }

  if (options.start !== false) start();

  return wrapManagedAsyncSignal(inner as Signal<AsyncValue<R>>, retry, reload, dispose, start);
}
