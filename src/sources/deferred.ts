// ---------------------------------------------------------------------------
// deferred — resolvable single-value source
//
// Like a Promise, but supports unsubscription via stream disposal.
// Multiple subscribers are supported — each gets the value after resume().
// ---------------------------------------------------------------------------

import type { Sink, Stream, Deferred } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class DeferredStream<T> implements Stream {
  #disposed = false;

  constructor(
    private readonly deferred: DeferredSource<T>,
    private readonly sink: Sink<T>,
  ) {
    deferred._pausedSinks.add(sink);
  }

  resume(): void {
    if (this.#disposed) return;

    this.deferred._pausedSinks.delete(this.sink);

    if (this.deferred._resolved) {
      this.sink.next(this.deferred._resolvedValue!);
      this.sink.complete();
    } else if (this.deferred._errored) {
      this.sink.error(this.deferred._error);
    } else {
      // Not yet resolved — add to waiting set
      this.deferred._waitingSinks.add(this.sink);
    }
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.deferred._pausedSinks.delete(this.sink);
    this.deferred._waitingSinks.delete(this.sink);
  }
}

class DeferredSource<T> extends AbstractSource<T> implements Deferred<T> {
  _pausedSinks = new Set<Sink<T>>();
  _waitingSinks = new Set<Sink<T>>();
  _resolved = false;
  _resolvedValue: T | undefined;
  _errored = false;
  _error: unknown;

  readonly promise: Promise<T>;
  private _promiseResolve!: (value: T) => void;
  private _promiseReject!: (error: unknown) => void;

  constructor() {
    super();
    this.promise = new Promise<T>((resolve, reject) => {
      this._promiseResolve = resolve;
      this._promiseReject = reject;
    });
  }

  connect(sink: Sink<T>): Stream {
    return new DeferredStream(this, sink);
  }

  resolve(value: T): void {
    if (this._resolved || this._errored) return;
    this._resolved = true;
    this._resolvedValue = value;
    this._promiseResolve(value);

    for (const sink of this._waitingSinks) {
      sink.next(value);
      sink.complete();
    }
    this._waitingSinks.clear();
  }

  reject(error: unknown): void {
    if (this._resolved || this._errored) return;
    this._errored = true;
    this._error = error;
    this._promiseReject(error);

    for (const sink of this._waitingSinks) {
      sink.error(error);
    }
    this._waitingSinks.clear();
  }
}

/**
 * Create a resolvable single-value source.
 *
 * Unlike a Promise, supports unsubscription — disposing the stream before
 * resolution means the sink never receives a value.
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
