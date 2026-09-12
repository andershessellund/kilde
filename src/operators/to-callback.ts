// ---------------------------------------------------------------------------
// toCallback — push values to a callback, return a Promise<void>
//
// Registers with the owner (before connecting upstream) so that owner
// disposal rejects the promise with StreamDisposedError instead of leaving
// it pending forever. A throwing callback rejects the promise and releases
// the upstream — it never escapes into the producer.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import type { OwnedOptions } from '../owner.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { AbstractSource } from '../abstract-source.js';
import { registerWithOwner } from '../internal/owned.js';
import { SingleValueStream } from '../internal/single-value-stream.js';

/** Connect `source`, feed every value to `fn`, settle when it ends. */
function drainInto<T>(
  source: Source<T>,
  fn: (value: T) => void,
  opts: OwnedOptions | undefined,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let settled = false;
    let upstream: Stream | undefined;

    const registration = registerWithOwner(opts?.owner, 'toCallback', () => {
      if (settled) return;
      settled = true;
      upstream?.[Symbol.dispose]();
      reject(new StreamDisposedError());
    });
    if (settled) return; // owner already disposed

    const settle = () => {
      settled = true;
      registration.unregister();
    };

    upstream = source.connect({
      next(value: T): undefined | PAUSE {
        if (settled) return PAUSE;
        try {
          fn(value);
        } catch (err) {
          settle();
          upstream?.[Symbol.dispose]();
          reject(err);
          return PAUSE;
        }
        return undefined;
      },
      complete() {
        if (settled) return;
        settle();
        resolve();
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

class ToCallbackSource<T> extends AbstractSource<Promise<void>> {
  constructor(
    private readonly source: Source<T>,
    private readonly fn: (value: T) => void,
    private readonly opts: OwnedOptions | undefined,
  ) {
    super();
  }

  connect(sink: Sink<Promise<void>>): Stream {
    return new SingleValueStream(sink, () => drainInto(this.source, this.fn, this.opts));
  }
}

/**
 * Push each upstream value to a callback function and return a
 * `Promise<void>` that resolves when the source completes, or rejects
 * on error. If `fn` throws, the promise rejects with that error and the
 * upstream is disposed.
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
 * // Simple observation — await completion
 * await source.stream(toCallback(v => console.log(v)));
 *
 * // Scoped to an owner — disposing `page` ends the subscription
 * const page = createOwner();
 * withOwner(page, () => {
 *   void stream(events$, toCallback(v => render(v)));
 * });
 * ```
 */
export function toCallback<T>(
  fn: (value: T) => void,
  opts?: OwnedOptions,
): Operator<T, Promise<void>> {
  return (source) => new ToCallbackSource(source, fn, opts);
}
