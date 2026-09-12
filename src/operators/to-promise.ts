// ---------------------------------------------------------------------------
// toPromise — wire upstream to a Promise, emit the Promise immediately
//
// Resolves with the FIRST emitted value, then disposes the upstream.
// Registers with the owner so that owner disposal rejects the promise with
// StreamDisposedError instead of leaving it pending forever.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { currentOwner } from '../owner.js';
import type { OwnedOptions } from '../owner.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { AbstractSource } from '../abstract-source.js';

class ToPromiseStream<T> implements Stream {
  #disposed = false;

  constructor(
    private readonly sink: Sink<Promise<T>>,
    private readonly source: Source<T>,
    private readonly opts: OwnedOptions | undefined,
  ) {}

  resume(): void {
    if (this.#disposed) return;

    const promise = new Promise<T>((resolve, reject) => {
      let settled = false;
      let upstream: Stream | null = null;

      const settle = () => {
        settled = true;
        handle?.unregister();
      };

      upstream = this.source.connect({
        next(value: T): undefined {
          if (settled) return undefined;
          settle();
          // Dispose upstream — we only need the first value
          upstream?.[Symbol.dispose]();
          resolve(value);
          return undefined;
        },
        complete() {
          if (settled) return;
          settle();
          reject(new Error('toPromise(): source completed without emitting a value'));
        },
        error(error: unknown) {
          if (settled) return;
          settle();
          reject(error);
        },
      });

      // Register with the owner. On disposal, reject the promise with
      // StreamDisposedError and tear down the upstream.
      const teardown = () => {
        if (settled) return;
        settled = true;
        upstream?.[Symbol.dispose]();
        reject(new StreamDisposedError());
      };
      const handle = (this.opts?.owner ?? currentOwner()).register(
        { [Symbol.dispose]: teardown },
        'toPromise',
      );

      upstream.resume();
    });

    this.sink.next(promise);
    this.sink.complete();
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
  }
}

class ToPromiseSource<T> extends AbstractSource<Promise<T>> {
  constructor(
    private readonly source: Source<T>,
    private readonly opts: OwnedOptions | undefined,
  ) {
    super();
  }

  connect(sink: Sink<Promise<T>>): Stream {
    return new ToPromiseStream(sink, this.source, this.opts);
  }
}

/**
 * Wire the upstream to a `Promise<T>` and emit the promise immediately.
 *
 * The promise resolves with the **first** emitted value, then disposes
 * the upstream connection. Rejects on error or if the source completes
 * without emitting.
 *
 * The pending promise is registered with its owner (`opts.owner`, else the
 * ambient owner). Owner disposal rejects the promise with
 * `StreamDisposedError` and tears down the upstream connection.
 *
 * @example
 * ```ts
 * const promise = stream(asyncSource, toPromise());
 * const value = await promise;
 * ```
 */
export function toPromise<T>(opts?: OwnedOptions): Operator<T, Promise<T>> {
  return (source) => new ToPromiseSource(source, opts);
}
