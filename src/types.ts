// ---------------------------------------------------------------------------
// Stream protocol — types and symbols
//
// Push-based reactive streams with cooperative backpressure (PAUSE signal).
// A Source<T> is a connectable template. Connecting produces a Stream
// (the active connection). Streams start paused — call resume() to begin.
// ---------------------------------------------------------------------------


// ---------------------------------------------------------------------------
// PAUSE — backpressure signal
// ---------------------------------------------------------------------------

/** Backpressure signal returned by `Sink.next()` to pause the source. */
export const PAUSE: unique symbol = Symbol('kilde.pause');

/** The PAUSE symbol type. */
export type PAUSE = typeof PAUSE;

// ---------------------------------------------------------------------------
// Sink<T> — receives values from a source
// ---------------------------------------------------------------------------

/**
 * Receives values pushed by a source.
 *
 * - `next(value)` — receive a value. Return `PAUSE` to pause the source,
 *   or `undefined` to keep receiving.
 * - `complete()` — no more values will be sent.
 * - `error(err)` — an error occurred; no more values.
 *
 * The protocol between a source and its sink:
 *
 * 1. A source makes no calls on the sink before the first `resume()` on
 *    the stream returned by `connect()`.
 * 2. After `next()` returns `PAUSE`, the source sends no further `next()`
 *    until `resume()` is called again.
 * 3. `PAUSE` governs `next()` only. `complete()` and `error()` may be
 *    delivered at any time after the first `resume()`, **including while
 *    paused**. A sink that returns `PAUSE` must therefore be prepared to
 *    receive a terminal event before it resumes.
 * 4. After `complete()` or `error()` no further calls of any kind are
 *    made, and `resume()` on the stream is a no-op.
 *
 * Operators that buffer values for a paused downstream deliver a terminal
 * event only after the buffer has drained, so a sink never observes a
 * terminal event ahead of a value it was already owed.
 */
export interface Sink<T> {
  next(value: T): undefined | PAUSE;
  complete(): void;
  error(error: unknown): void;
}

// ---------------------------------------------------------------------------
// Stream — active connection between a source and a sink
// ---------------------------------------------------------------------------

/**
 * An active connection between a source and a sink.
 *
 * Streams start **paused**. Call `resume()` to begin receiving values.
 * After `resume()`, the source pushes values to the connected sink until
 * the sink returns `PAUSE`, the source completes, or an error occurs.
 * Calling `resume()` while already delivering, or after the stream has
 * completed, errored, or been disposed, is a no-op.
 *
 * Implements the standard `Disposable` protocol via `[Symbol.dispose]`.
 */
export interface Stream {
  /** Start or resume value delivery to the connected sink. */
  resume(): void;

  /** Dispose the stream, stopping delivery and releasing resources. */
  [Symbol.dispose](): void;
}

// ---------------------------------------------------------------------------
// Source<T> — connectable template
// ---------------------------------------------------------------------------

/**
 * A connectable template that produces values when connected to a sink.
 *
 * Calling `connect(sink)` returns a `Stream` (active connection) in a
 * **paused** state. The stream begins delivering values only after
 * `resume()` is called.
 *
 * Sources can be connected multiple times — each connection is independent.
 */
export interface Source<T> {
  connect(sink: Sink<T>): Stream;
}

// ---------------------------------------------------------------------------
// StreamableSource<T> — source with left-to-right .stream() composition
// ---------------------------------------------------------------------------

/**
 * A `Source<T>` that also provides the `.stream()` method for ergonomic
 * left-to-right operator composition.
 *
 * All built-in source factories (`fromArray`, `fromIterator`, `empty`, etc.)
 * return `StreamableSource<T>`. Operators accept and return plain `Source<T>`,
 * keeping the protocol minimal.
 *
 * @example
 * ```ts
 * fromArray([1, 2, 3]).stream(map(x => x * 2), reduce((a, b) => a + b, 0))
 * ```
 */
export interface StreamableSource<T> extends Source<T> {
  stream(): T;
  stream<R1>(op1: Operator<T, R1>): R1;
  stream<T1, R2>(op1: Operator<T, T1>, op2: Operator<T1, R2>): R2;
  stream<T1, T2, R3>(
    op1: Operator<T, T1>,
    op2: Operator<T1, T2>,
    op3: Operator<T2, R3>,
  ): R3;
  stream<T1, T2, T3, R4>(
    op1: Operator<T, T1>,
    op2: Operator<T1, T2>,
    op3: Operator<T2, T3>,
    op4: Operator<T3, R4>,
  ): R4;
  stream<T1, T2, T3, T4, R5>(
    op1: Operator<T, T1>,
    op2: Operator<T1, T2>,
    op3: Operator<T2, T3>,
    op4: Operator<T3, T4>,
    op5: Operator<T4, R5>,
  ): R5;
  stream<T1, T2, T3, T4, T5, R6>(
    op1: Operator<T, T1>,
    op2: Operator<T1, T2>,
    op3: Operator<T2, T3>,
    op4: Operator<T3, T4>,
    op5: Operator<T4, T5>,
    op6: Operator<T5, R6>,
  ): R6;
  stream<T1, T2, T3, T4, T5, T6, R7>(
    op1: Operator<T, T1>,
    op2: Operator<T1, T2>,
    op3: Operator<T2, T3>,
    op4: Operator<T3, T4>,
    op5: Operator<T4, T5>,
    op6: Operator<T5, T6>,
    op7: Operator<T6, R7>,
  ): R7;
  stream<T1, T2, T3, T4, T5, T6, T7, R8>(
    op1: Operator<T, T1>,
    op2: Operator<T1, T2>,
    op3: Operator<T2, T3>,
    op4: Operator<T3, T4>,
    op5: Operator<T4, T5>,
    op6: Operator<T5, T6>,
    op7: Operator<T6, T7>,
    op8: Operator<T7, R8>,
  ): R8;
  stream(...operators: Operator<any, any>[]): any;
}

// ---------------------------------------------------------------------------
// Operator<T, R> — transforms a source
// ---------------------------------------------------------------------------

/**
 * A function that transforms a `Source<T>` into a `Source<R>`.
 *
 * Operators are plain functions — no class, no `.apply()` method.
 * They compose naturally via `pipe()` and `comp()`.
 */
export type Operator<T, R> = (source: Source<T>) => Source<R>;

// ---------------------------------------------------------------------------
// Deferred<T> — resolvable single-value source
// ---------------------------------------------------------------------------

/**
 * A resolvable single-value source. Like a Promise but supports
 * unsubscription via stream disposal.
 *
 * - `resolve(value)` — emit the value to all waiting subscribers.
 * - `reject(error)` — emit an error to all waiting subscribers.
 * - `promise` — a standard Promise that resolves/rejects with the deferred.
 *
 * Multiple subscribers are supported. Each gets the value after `resume()`.
 * Disposing a stream before resolution means that sink never receives a value.
 */
export interface Deferred<T> extends StreamableSource<T> {
  resolve(value: T): void;
  reject(error: unknown): void;
  readonly promise: Promise<T>;
}

// ---------------------------------------------------------------------------
// Relay<T> — source + sink (Subject-like)
// ---------------------------------------------------------------------------

/**
 * A relay is both a `Source<T>` and a `Sink<T>`.
 *
 * Values pushed via `next()` are multicast to all connected subscribers.
 * Each subscriber gets its own pausable buffer.
 */
export interface Relay<T> extends StreamableSource<T>, Sink<T> {}

// ---------------------------------------------------------------------------
// Scheduler — controls when signal delivery happens
// ---------------------------------------------------------------------------

/**
 * A scheduler controls when signal delivery happens.
 *
 * `schedule(callback)` — request a flush. The callback must be called
 * exactly once. Implementations should deduplicate: if schedule() is
 * called while a previous callback is still pending, the second call
 * is a no-op (the coordinator handles this).
 */
export interface Scheduler {
  schedule(callback: () => void): void;
}

// ---------------------------------------------------------------------------
// Signal<T> — synchronous, pull-based reactive value
// ---------------------------------------------------------------------------

/**
 * A read-only signal. Always holds a current value.
 *
 * Signals are **callable** — invoke `signal()` to read the current value.
 * This is the primary way to access signal state.
 *
 * Only emits when the value actually changes (`Object.is` by default).
 *
 * Calling `signal()` inside a `computed()` callback automatically registers
 * this signal as a dependency.
 */
export interface Signal<T> {
  /** Read the current value. Inside `computed()`, auto-registers as dependency. */
  (): T;

  /**
   * Whether this signal has any observers — stream subscribers or
   * computed nodes that depend on it.
   *
   * Used by {@link SignalDeduplicator} to determine which cached signals
   * can be safely evicted.
   */
  readonly observed: boolean;

  /**
   * Subscribe to signal lifecycle events.
   *
   * - `'value'` — delivers the current value immediately, then on each change.
   * - `'activate'` — fires when the signal transitions from unobserved to observed (0→1 dependents).
   * - `'deactivate'` — fires when the signal transitions from observed to unobserved (last dependent leaves).
   *
   * @returns unsubscribe function
   */
  observe(type: 'value', callback: (value: T) => void, scheduler?: Scheduler): () => void;
  observe(type: 'activate', callback: () => void): () => void;
  observe(type: 'deactivate', callback: () => void): () => void;
  observe(type: 'read', callback: (current: T) => T): () => void;
}

/**
 * A writable signal. Always holds a current value.
 *
 * `set(value)` updates the signal and notifies subscribers, skipping
 * the update if the new value is deeply equal to the current one.
 *
 * `update(fn)` applies a function to the current value and sets the result.
 */
export interface WritableSignal<T> extends Signal<T> {
  /** Set a new value. Skipped if equal to the current value. */
  set(value: T): void;

  /** Update the value via a function. Skipped if the result is equal. */
  update(fn: (current: T) => T): void;
}

// ---------------------------------------------------------------------------
// Store<T> — signal with lifecycle
// ---------------------------------------------------------------------------

/**
 * A store is a {@link WritableSignal} with lifecycle management.
 *
 * Like WritableSignal, it always holds a current value, replays on subscribe,
 * and deduplicates via equality. Additionally:
 *
 * - Implements `[Symbol.dispose]` — disposing completes all connected sinks
 *   and prevents further updates.
 * - Can be fed from event streams via `intoStore()`.
 *
 * Use `createStore(initial)` to create a store.
 */
export interface Store<T> extends WritableSignal<T> {
  /** Whether the store has been disposed. */
  readonly disposed: boolean;

  /** Connect a sink. Delivers the current value on `resume()`, then each change. */
  connect(sink: Sink<T>): Stream;

  /** Dispose the store: complete all subscribers, reject pending intoStore promises. */
  [Symbol.dispose](): void;
}
