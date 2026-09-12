// ---------------------------------------------------------------------------
// deferred — resolvable single-value source
//
// Like a Promise, but supports unsubscription via stream disposal.
// Multiple subscribers are supported — each gets the value after resume().
//
// Each connection is a PauseBuffer. On connect the stream registers with the
// deferred; when the deferred settles (or if it already has), the settlement
// is pushed into the buffer, which delivers it under the stream protocol:
// nothing before the first resume(), the value exactly once, and the
// terminal event either right away or — if the sink paused on the value —
// on the next resume(). resume() is idempotent.
// ---------------------------------------------------------------------------

import type { Sink, Stream, Deferred } from '../types.js';
import { AbstractSource } from '../abstract-source.js';
import { PauseBuffer } from '../internal/pause-buffer.js';

type Settlement<T> = { kind: 'resolved'; value: T } | { kind: 'rejected'; error: unknown };

class DeferredStream<T> implements Stream {
  readonly #buffer: PauseBuffer<T>;
  #disposed = false;

  constructor(
    private readonly deferred: DeferredSource<T>,
    sink: Sink<T>,
  ) {
    this.#buffer = new PauseBuffer(sink);
    if (deferred._settlement) {
      this._settle(deferred._settlement);
    } else {
      deferred._waiting.add(this);
    }
  }

  /** @internal Called by the deferred when it settles. */
  _settle(settlement: Settlement<T>): void {
    if (settlement.kind === 'resolved') {
      this.#buffer.push(settlement.value);
      this.#buffer.complete();
    } else {
      this.#buffer.error(settlement.error);
    }
  }

  resume(): void {
    this.#buffer.resume();
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.deferred._waiting.delete(this);
    this.#buffer.dispose();
  }
}

class DeferredSource<T> extends AbstractSource<T> implements Deferred<T> {
  /** @internal Streams connected before settlement. */
  readonly _waiting = new Set<DeferredStream<T>>();
  /** @internal The settlement, once it has happened. */
  _settlement: Settlement<T> | null = null;

  readonly promise: Promise<T>;
  readonly #promiseResolve: (value: T) => void;
  readonly #promiseReject: (error: unknown) => void;

  constructor() {
    super();
    let res!: (value: T) => void;
    let rej!: (error: unknown) => void;
    this.promise = new Promise<T>((resolve, reject) => {
      res = resolve;
      rej = reject;
    });
    this.#promiseResolve = res;
    this.#promiseReject = rej;
  }

  connect(sink: Sink<T>): Stream {
    return new DeferredStream(this, sink);
  }

  resolve(value: T): void {
    if (this._settlement) return;
    this.#promiseResolve(value);
    this.#settle({ kind: 'resolved', value });
  }

  reject(error: unknown): void {
    if (this._settlement) return;
    this.#promiseReject(error);
    this.#settle({ kind: 'rejected', error });
  }

  /**
   * Record the settlement and notify every waiting stream. A sink that
   * throws does not prevent the others from being notified — errors are
   * collected and rethrown once everyone has been told.
   */
  #settle(settlement: Settlement<T>): void {
    this._settlement = settlement;
    const streams = [...this._waiting];
    this._waiting.clear();
    const errors: unknown[] = [];
    for (const stream of streams) {
      try {
        stream._settle(settlement);
      } catch (err) {
        errors.push(err);
      }
    }
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) {
      throw new AggregateError(errors, 'deferred: one or more sinks threw during settlement');
    }
  }
}

/**
 * Create a resolvable single-value source.
 *
 * Unlike a Promise, supports unsubscription — disposing the stream before
 * resolution means the sink never receives a value.
 *
 * `resolve()` / `reject()` are idempotent: only the first settlement counts.
 * Every subscriber is notified even if one of them throws; the exception(s)
 * are rethrown to the caller of `resolve()` / `reject()` afterwards.
 *
 * @example
 * ```ts
 * const d = deferred<number>();
 * const s = d.connect(mySink);
 * s.resume();
 * d.resolve(42); // mySink receives 42 then complete()
 * ```
 */
export function deferred<T>(): Deferred<T> {
  return new DeferredSource<T>();
}
