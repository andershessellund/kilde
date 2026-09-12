// ---------------------------------------------------------------------------
// Store — signal with lifecycle
//
// A store wraps a WritableSignal, adding lifecycle (dispose completes
// subscribers) and the ability to fold event streams via intoStore().
//
// createStore(initial, opts?) — create a writable store (callable)
// intoStore(store, reducer)  — operator: pipe a source into a store
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Store, Scheduler, WritableSignal } from './types.js';
import { createSignal, SIGNAL_BRAND } from './signal.js';
import { fromSignal } from './sources/from-signal.js';
import { completeOnResume } from './internal/complete-on-resume.js';

// ---------------------------------------------------------------------------
// StoreStream — one connection to a store
//
// Wraps a fromSignal() connection and adds the store's lifecycle: when the
// store is disposed the sink is completed — right away if the stream has
// been resumed, otherwise on its first resume() (a sink must not hear
// anything before the first resume). Each connection is tracked by
// identity, so the same sink connected twice gets two independent streams.
// ---------------------------------------------------------------------------

class StoreStream<T> implements Stream {
  #started = false;
  #terminated = false;
  #disposed = false;
  /** Set when the store was disposed before this stream was resumed. */
  #completeOnResume = false;

  constructor(
    private readonly sink: Sink<T>,
    private readonly inner: Stream,
    private readonly onDispose: (stream: StoreStream<T>) => void,
  ) {}

  resume(): void {
    if (this.#terminated || this.#disposed) return;
    if (this.#completeOnResume) {
      this.#terminated = true;
      this.sink.complete();
      return;
    }
    this.#started = true;
    this.inner.resume();
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.inner[Symbol.dispose]();
    this.onDispose(this);
  }

  /** @internal The store was disposed: stop observing, complete the sink. */
  _storeDisposed(): void {
    if (this.#disposed) return;
    this.inner[Symbol.dispose]();
    if (this.#terminated) return;
    if (!this.#started) {
      this.#completeOnResume = true;
      return;
    }
    this.#terminated = true;
    this.sink.complete();
  }
}

// ---------------------------------------------------------------------------
// StoreImpl — internal state holder
// ---------------------------------------------------------------------------

class StoreImpl<T> {
  readonly #signal: WritableSignal<T>;
  readonly #streams = new Set<StoreStream<T>>();
  #disposed = false;

  constructor(initial: T, equals?: (a: T, b: T) => boolean) {
    this.#signal = createSignal<T>(initial, equals ? { equals } : undefined);
  }

  // --- Read value (delegates to inner callable signal) ---

  readValue(): T {
    return this.#signal();
  }

  get observed(): boolean {
    return this.#signal.observed;
  }

  // --- WritableSignal ---

  set(value: T): void {
    if (this.#disposed) return;
    this.#signal.set(value);
  }

  update(fn: (current: T) => T): void {
    if (this.#disposed) return;
    this.#signal.update(fn);
  }

  observe(type: 'value', callback: (value: T) => void, scheduler: Scheduler): () => void;
  observe(type: 'activate', callback: () => void): () => void;
  observe(type: 'deactivate', callback: () => void): () => void;
  observe(type: string, callback: (...args: any[]) => void, scheduler?: Scheduler): () => void {
    return this.#signal.observe(type as any, callback as any, scheduler as any);
  }

  // --- Source ---

  connect(sink: Sink<T>): Stream {
    // Already disposed — complete on the first resume, nothing before.
    if (this.#disposed) return completeOnResume(sink);

    const inner = fromSignal(this.#signal).connect(sink);
    const stream = new StoreStream<T>(sink, inner, (s) => this.#streams.delete(s));
    this.#streams.add(stream);
    return stream;
  }

  // --- Store lifecycle ---

  get disposed(): boolean {
    return this.#disposed;
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;

    const streams = [...this.#streams];
    this.#streams.clear();
    for (const stream of streams) {
      stream._storeDisposed();
    }
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export interface CreateStoreOptions<T> {
  /** Custom equality function. Default: `deepEqual`. */
  equals?: (a: T, b: T) => boolean;
}

/**
 * Create a writable store with lifecycle.
 *
 * A store is a callable signal — invoke `store()` to read the value.
 * Always holds a value, replays to new subscribers, and deduplicates
 * via deep equality (configurable). Disposing the store completes all
 * connected sinks.
 *
 * @example
 * ```ts
 * const todos = createStore<Todo[]>([]);
 *
 * todos();                                  // []
 * todos.set([{ id: 1, text: 'Buy milk' }]);
 * todos.update(list => [...list, newTodo]);
 *
 * // Subscribe
 * const s = todos.connect(mySink);
 * s.resume(); // receives current value immediately
 *
 * // Lifecycle
 * todos[Symbol.dispose](); // completes all subscribers
 * ```
 */
export function createStore<T>(initial: T, opts?: CreateStoreOptions<T>): Store<T> {
  const impl = new StoreImpl(initial, opts?.equals);
  const fn = function (this: void): T {
    return impl.readValue();
  };
  Object.defineProperties(fn, {
    [SIGNAL_BRAND]: { value: true, configurable: true },
    observed: { get() { return impl.observed; }, configurable: true },
    observe: {
      value: (type: string, callback: (...args: any[]) => void, scheduler?: Scheduler) =>
        impl.observe(type as any, callback as any, scheduler as any),
      configurable: true,
    },
    set: { value: (v: T) => impl.set(v), configurable: true },
    update: { value: (updater: (current: T) => T) => impl.update(updater), configurable: true },
    connect: { value: (sink: Sink<T>) => impl.connect(sink), configurable: true },
    disposed: { get() { return impl.disposed; }, configurable: true },
    [Symbol.dispose]: { value: () => impl[Symbol.dispose](), configurable: true },
  });
  return fn as unknown as Store<T>;
}

/**
 * Fold a source stream into a store using a reducer function.
 *
 * Each value emitted by the source is combined with the store's current
 * value via `reducer(storeValue, item)` and the result is set on the store.
 *
 * Returns a `Promise<void>` that:
 * - Resolves when the source completes
 * - Rejects when the source errors
 * - Resolves when the store is disposed (source is disconnected)
 *
 * The source is never paused — the store always accepts values.
 *
 * @example
 * ```ts
 * const todos = createStore<Todo[]>([]);
 *
 * // Fold addTodo events into the store
 * await intoStore(todos, (list, todo) => [...list, todo])(addTodo$);
 *
 * // Or with pipe:
 * pipe(addTodo$, intoStore(todos, (list, todo) => [...list, todo]));
 * ```
 */
export function intoStore<V, T>(
  store: Store<V>,
  reducer: (value: V, item: T) => V,
): (source: Source<T>) => Promise<void> {
  return (source) =>
    new Promise<void>((resolve, reject) => {
      if (store.disposed) {
        resolve();
        return;
      }

      let stream: Stream | null = null;

      // Watch for store disposal — hook into the store as a subscriber
      // that resolves the promise on complete.
      const storeStream = store.connect({
        next() {
          return undefined;
        },
        complete() {
          // Store was disposed — disconnect the source
          stream?.[Symbol.dispose]();
          resolve();
        },
        error(err: unknown) {
          stream?.[Symbol.dispose]();
          reject(err);
        },
      });
      storeStream.resume();

      stream = source.connect({
        next(item: T): undefined {
          store.update((v) => reducer(v, item));
          return undefined; // never pause
        },
        complete() {
          storeStream[Symbol.dispose]();
          resolve();
        },
        error(err: unknown) {
          storeStream[Symbol.dispose]();
          reject(err);
        },
      });
      stream.resume();
    });
}
