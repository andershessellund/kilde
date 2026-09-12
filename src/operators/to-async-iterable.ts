// ---------------------------------------------------------------------------
// toAsyncIterable — wrap upstream as an AsyncIterable, emit it immediately
//
// Uses pause/resume for backpressure: the source is resumed only while a
// consumer pull is waiting, and paused once every waiting pull has been
// satisfied. Concurrent next() calls queue up FIFO and are satisfied in
// order.
//
// Registers with the owner (before connecting upstream) so that owner
// disposal rejects the pending next() with StreamDisposedError instead of
// leaving it pending forever.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import type { OwnedOptions } from '../owner.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { AbstractSource } from '../abstract-source.js';
import { registerWithOwner } from '../internal/owned.js';
import { SingleValueStream } from '../internal/single-value-stream.js';

interface Waiter<T> {
  resolve: (result: IteratorResult<T>) => void;
  reject: (error: unknown) => void;
}

const DONE: IteratorResult<never> = { value: undefined, done: true };

function iterate<T>(source: Source<T>, opts: OwnedOptions | undefined): AsyncIterator<T> {
  // Pulls waiting for a value, oldest first.
  const waiters: Waiter<T>[] = [];
  // Values that arrived while no pull was waiting (at most one, since the
  // source is paused as soon as the last waiter is served — unless the
  // source ignores PAUSE).
  const buffered: T[] = [];
  // A failure that arrived while no pull was waiting; the next pull rejects.
  let pendingError: { error: unknown } | null = null;
  let done = false;
  let upstream: Stream | undefined;
  // Whether the upstream is paused (or not yet started) and needs resume().
  let upstreamPaused = true;

  const registration = registerWithOwner(opts?.owner, 'toAsyncIterable', () => {
    if (done) return;
    done = true;
    upstream?.[Symbol.dispose]();
    fail(new StreamDisposedError());
  });

  /** Reject the oldest waiter with `error`; later waiters see done. */
  function fail(error: unknown): void {
    const first = waiters.shift();
    if (first) first.reject(error);
    else pendingError = { error };
    for (const w of waiters.splice(0)) w.resolve(DONE);
  }

  function finish(): void {
    for (const w of waiters.splice(0)) w.resolve(DONE);
  }

  if (!done) {
    upstream = source.connect({
      next(value: T): undefined | PAUSE {
        if (done) return PAUSE;
        const w = waiters.shift();
        if (w) w.resolve({ value, done: false });
        else buffered.push(value);
        // Keep going while more pulls are waiting; otherwise pause until
        // the consumer asks again.
        if (waiters.length > 0) return undefined;
        upstreamPaused = true;
        return PAUSE;
      },
      complete() {
        if (done) return;
        done = true;
        registration.unregister();
        finish();
      },
      error(error: unknown) {
        if (done) return;
        done = true;
        registration.unregister();
        fail(error);
      },
    });
  }

  return {
    next(): Promise<IteratorResult<T>> {
      if (buffered.length > 0) {
        return Promise.resolve({ value: buffered.shift()!, done: false });
      }
      if (pendingError) {
        const { error } = pendingError;
        pendingError = null;
        return Promise.reject(error);
      }
      if (done) {
        return Promise.resolve(DONE);
      }
      return new Promise<IteratorResult<T>>((resolve, reject) => {
        waiters.push({ resolve, reject });
        if (upstreamPaused) {
          upstreamPaused = false;
          upstream?.resume();
        }
      });
    },
    return(): Promise<IteratorResult<T>> {
      if (!done) {
        done = true;
        registration.unregister();
        upstream?.[Symbol.dispose]();
      }
      buffered.length = 0;
      pendingError = null;
      // Any pull still waiting is over too.
      finish();
      return Promise.resolve(DONE);
    },
  };
}

class ToAsyncIterableSource<T> extends AbstractSource<AsyncIterable<T>> {
  constructor(
    private readonly source: Source<T>,
    private readonly opts: OwnedOptions | undefined,
  ) {
    super();
  }

  connect(sink: Sink<AsyncIterable<T>>): Stream {
    const source = this.source;
    const opts = this.opts;
    return new SingleValueStream(sink, () => ({
      [Symbol.asyncIterator]: () => iterate(source, opts),
    }));
  }
}

/**
 * Wrap the upstream as an `AsyncIterable<T>` and emit it immediately.
 *
 * The async iterable uses pause/resume for backpressure: the source is
 * resumed while the consumer has a `next()` pending and paused once every
 * pending `next()` has been satisfied. Concurrent `next()` calls are
 * served in order. `return()` (e.g. `break` out of `for await`) disposes
 * the upstream and settles any pending `next()` with `done: true`.
 *
 * Each iterator is registered with its owner (`opts.owner`, else the
 * ambient owner at the time iteration starts). Owner disposal rejects any
 * pending `next()` call with `StreamDisposedError` and tears down the
 * upstream.
 *
 * @example
 * ```ts
 * const iterable = stream(source, toAsyncIterable());
 * for await (const value of iterable) {
 *   console.log(value);
 * }
 * ```
 */
export function toAsyncIterable<T>(opts?: OwnedOptions): Operator<T, AsyncIterable<T>> {
  return (source) => new ToAsyncIterableSource(source, opts);
}
