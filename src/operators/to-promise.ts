// ---------------------------------------------------------------------------
// toPromise — wire upstream to a Promise, emit the Promise immediately
//
// Resolves with the FIRST emitted value, then disposes the upstream.
// Registers with the owner so that owner disposal rejects the promise with
// StreamDisposedError instead of leaving it pending forever.
//
// The owner registration happens BEFORE the upstream is connected: a
// source may end synchronously during the first resume(), and the sink
// then unregisters — so the registration has to exist already.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import type { OwnedOptions } from '../owner.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { AbstractSource } from '../abstract-source.js';
import { registerWithOwner } from '../internal/owned.js';
import { SingleValueStream } from '../internal/single-value-stream.js';

/** Connect `source` and settle a promise with its first value. */
function firstValue<T>(source: Source<T>, opts: OwnedOptions | undefined): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    let upstream: Stream | undefined;

    const registration = registerWithOwner(opts?.owner, 'toPromise', () => {
      if (settled) return;
      settled = true;
      upstream?.[Symbol.dispose]();
      reject(new StreamDisposedError());
    });
    // The owner may already be disposed, in which case the teardown ran
    // synchronously and there is nothing to connect.
    if (settled) return;

    const settle = () => {
      settled = true;
      registration.unregister();
    };

    upstream = source.connect({
      next(value: T): PAUSE {
        if (settled) return PAUSE;
        settle();
        // Dispose upstream — we only need the first value
        upstream?.[Symbol.dispose]();
        resolve(value);
        return PAUSE;
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

    upstream.resume();
  });
}

class ToPromiseSource<T> extends AbstractSource<Promise<T>> {
  constructor(
    private readonly source: Source<T>,
    private readonly opts: OwnedOptions | undefined,
  ) {
    super();
  }

  connect(sink: Sink<Promise<T>>): Stream {
    return new SingleValueStream(sink, () => firstValue(this.source, this.opts));
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
 * The upstream is connected on the first `resume()` of the returned
 * stream; later `resume()` calls are no-ops.
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
