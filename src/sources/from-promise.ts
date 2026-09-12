// ---------------------------------------------------------------------------
// fromPromise / fromAsyncFn — Source bridges for promise-based async
//
// fromPromise(promise)  — wraps an existing PromiseLike into a Source.
// fromAsyncFn(fn)       — creates a Source that calls fn() on each connect.
//
// Both deliver the resolved value on resume(). If the promise rejects,
// delivers an error. Respects disposal — if disposed before the promise
// settles, the result is silently discarded.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class PromiseStream<T> implements Stream {
  #disposed = false;
  #resumed = false;
  #resolved = false;
  #errored = false;
  #result: T | undefined;
  #error: unknown;

  constructor(
    private readonly sink: Sink<T>,
    promise: PromiseLike<T>,
  ) {
    promise.then(
      (value) => {
        if (this.#disposed) return;
        this.#resolved = true;
        this.#result = value;
        if (this.#resumed) {
          this.sink.next(value);
          this.sink.complete();
        }
      },
      (err) => {
        if (this.#disposed) return;
        this.#errored = true;
        this.#error = err;
        if (this.#resumed) {
          this.sink.error(err);
        }
      },
    );
  }

  resume(): void {
    if (this.#disposed) return;
    this.#resumed = true;
    if (this.#resolved) {
      this.sink.next(this.#result!);
      this.sink.complete();
    } else if (this.#errored) {
      this.sink.error(this.#error);
    }
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
  }
}

class FromPromiseSource<T> extends AbstractSource<T> {
  constructor(private readonly promise: PromiseLike<T>) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    return new PromiseStream(sink, this.promise);
  }
}

class FromAsyncFnSource<T> extends AbstractSource<T> {
  constructor(private readonly fn: () => Promise<T>) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    return new PromiseStream(sink, this.fn());
  }
}

/**
 * Create a source from an existing `PromiseLike`.
 *
 * The promise is already running — each `connect()` shares the same
 * settlement. The resolved value is delivered on `resume()`.
 *
 * @example
 * ```ts
 * const resp = fetch('/api/me');
 * stream(fromPromise(resp), toPromise());
 * ```
 */
export function fromPromise<T>(promise: PromiseLike<T>): Source<T> {
  return new FromPromiseSource(promise);
}

/**
 * Create a source from an async function.
 *
 * The function is called on each `connect()`, so each subscriber gets
 * its own promise. The resolved value is delivered on `resume()`.
 *
 * @example
 * ```ts
 * const user = fromAsyncFn(() => fetch('/api/me').then(r => r.json()));
 * stream(user, toPromise()).then(console.log);
 * ```
 */
export function fromAsyncFn<T>(fn: () => Promise<T>): Source<T> {
  return new FromAsyncFnSource(fn);
}
