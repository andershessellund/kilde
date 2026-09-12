// ---------------------------------------------------------------------------
// toAsyncIterable — wrap upstream as an AsyncIterable, emit it immediately
//
// Uses pause/resume for backpressure: the source is resumed only when the
// consumer pulls, and paused after every value.
//
// Registers with the owner so that owner disposal rejects the pending
// next() with StreamDisposedError instead of leaving it pending forever.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { currentOwner } from '../owner.js';
import type { OwnedOptions } from '../owner.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { AbstractSource } from '../abstract-source.js';

class ToAsyncIterableStream<T> implements Stream {
  #disposed = false;

  constructor(
    private readonly sink: Sink<AsyncIterable<T>>,
    private readonly source: Source<T>,
    private readonly opts: OwnedOptions | undefined,
  ) {}

  resume(): void {
    if (this.#disposed) return;

    const source = this.source;
    const opts = this.opts;
    const iterable: AsyncIterable<T> = {
      [Symbol.asyncIterator]() {
        let resolveNext: ((result: IteratorResult<T>) => void) | null = null;
        let rejectNext: ((error: unknown) => void) | null = null;
        // Values / failure that arrived while no pull was pending.
        const buffered: T[] = [];
        let pendingError: { error: unknown } | null = null;
        let done = false;
        let upstream: Stream | null = null;

        const cleanup = () => {
          handle?.unregister();
        };

        const settleWaiter = (): {
          resolve: (r: IteratorResult<T>) => void;
          reject: (e: unknown) => void;
        } | null => {
          if (!resolveNext || !rejectNext) return null;
          const w = { resolve: resolveNext, reject: rejectNext };
          resolveNext = null;
          rejectNext = null;
          return w;
        };

        upstream = source.connect({
          next(value: T): PAUSE {
            const w = settleWaiter();
            if (w) w.resolve({ value, done: false });
            else buffered.push(value);
            // Always pause — wait for the consumer to pull the next value
            return PAUSE;
          },
          complete() {
            done = true;
            cleanup();
            settleWaiter()?.resolve({ value: undefined, done: true });
          },
          error(error: unknown) {
            done = true;
            cleanup();
            const w = settleWaiter();
            if (w) w.reject(error);
            else pendingError = { error };
          },
        });

        // Register with the owner. On disposal, reject any pending next()
        // with StreamDisposedError and dispose the upstream.
        const teardown = () => {
          if (done) return;
          done = true;
          upstream?.[Symbol.dispose]();
          const err = new StreamDisposedError();
          const w = settleWaiter();
          if (w) w.reject(err);
          else pendingError = { error: err };
        };
        const handle = (opts?.owner ?? currentOwner()).register(
          { [Symbol.dispose]: teardown },
          'toAsyncIterable',
        );

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
              return Promise.resolve({ value: undefined, done: true });
            }
            return new Promise<IteratorResult<T>>((resolve, reject) => {
              resolveNext = resolve;
              rejectNext = reject;
              // Resume to get the next value
              upstream!.resume();
            });
          },
          return(): Promise<IteratorResult<T>> {
            done = true;
            cleanup();
            upstream?.[Symbol.dispose]();
            return Promise.resolve({ value: undefined, done: true });
          },
        };
      },
    };

    this.sink.next(iterable);
    this.sink.complete();
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
  }
}

class ToAsyncIterableSource<T> extends AbstractSource<AsyncIterable<T>> {
  constructor(
    private readonly source: Source<T>,
    private readonly opts: OwnedOptions | undefined,
  ) {
    super();
  }

  connect(sink: Sink<AsyncIterable<T>>): Stream {
    return new ToAsyncIterableStream(sink, this.source, this.opts);
  }
}

/**
 * Wrap the upstream as an `AsyncIterable<T>` and emit it immediately.
 *
 * The async iterable uses pause/resume for backpressure: each value
 * pauses the source until the consumer calls `next()`.
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
