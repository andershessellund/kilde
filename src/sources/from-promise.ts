// ---------------------------------------------------------------------------
// fromPromise / fromAsyncFn — Source bridges for promise-based async
//
// fromPromise(promise)  — wraps an existing PromiseLike into a Source.
// fromAsyncFn(fn)       — creates a Source that calls fn() on each connect.
//
// Both deliver the resolved value on resume(), then complete. If the promise
// rejects, they deliver an error. Respects disposal — if disposed before the
// promise settles, the result is silently discarded.
//
// Each connection is a PauseBuffer: the settlement is pushed into it when it
// arrives (queued until the first resume), the value is delivered exactly
// once, and the terminal event follows — at once if the sink did not pause,
// otherwise on the next resume(). resume() is idempotent.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream } from '../types.js';
import { AbstractSource } from '../abstract-source.js';
import { PauseBuffer } from '../internal/pause-buffer.js';

class PromiseStream<T> implements Stream {
  readonly #buffer: PauseBuffer<T>;

  constructor(sink: Sink<T>, promise: PromiseLike<T>) {
    this.#buffer = new PauseBuffer(sink);
    promise.then(
      (value) => {
        // No-ops after dispose (the buffer is inactive).
        this.#buffer.push(value);
        this.#buffer.complete();
      },
      (err) => {
        this.#buffer.error(err);
      },
    );
  }

  resume(): void {
    this.#buffer.resume();
  }

  [Symbol.dispose](): void {
    this.#buffer.dispose();
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
    let promise: Promise<T>;
    try {
      promise = this.fn();
    } catch (err) {
      // A synchronous throw is treated like a rejection.
      promise = Promise.reject(err);
    }
    return new PromiseStream(sink, promise);
  }
}

/**
 * Create a source from an existing `PromiseLike`.
 *
 * The promise is already running — each `connect()` shares the same
 * settlement. The resolved value is delivered on `resume()`, followed by
 * `complete()`; a rejection is delivered as `error()`.
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
 * its own promise. The resolved value is delivered on `resume()`, followed
 * by `complete()`; a rejection (or a synchronous throw from `fn`) is
 * delivered as `error()`.
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
