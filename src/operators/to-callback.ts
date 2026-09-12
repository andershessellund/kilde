// ---------------------------------------------------------------------------
// toCallback — push values to a callback, return a Promise<void>
//
// Registers with the owner so that owner disposal rejects the promise with
// StreamDisposedError instead of leaving it pending forever.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { currentOwner } from '../owner.js';
import type { OwnedOptions } from '../owner.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { AbstractSource } from '../abstract-source.js';

class ToCallbackStream<T> implements Stream {
  #disposed = false;

  constructor(
    private readonly sink: Sink<Promise<void>>,
    private readonly source: Source<T>,
    private readonly fn: (value: T) => void,
    private readonly opts: OwnedOptions | undefined,
  ) {}

  resume(): void {
    if (this.#disposed) return;

    const fn = this.fn;

    const promise = new Promise<void>((resolve, reject) => {
      let settled = false;
      let upstream: Stream | null = null;

      const settle = () => {
        settled = true;
        handle?.unregister();
      };

      upstream = this.source.connect({
        next(value: T): undefined {
          fn(value);
          return undefined;
        },
        complete() {
          settle();
          resolve();
        },
        error(error: unknown) {
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
        'toCallback',
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

class ToCallbackSource<T> extends AbstractSource<Promise<void>> {
  constructor(
    private readonly source: Source<T>,
    private readonly fn: (value: T) => void,
    private readonly opts: OwnedOptions | undefined,
  ) {
    super();
  }

  connect(sink: Sink<Promise<void>>): Stream {
    return new ToCallbackStream(sink, this.source, this.fn, this.opts);
  }
}

/**
 * Push each upstream value to a callback function and return a
 * `Promise<void>` that resolves when the source completes, or rejects
 * on error.
 *
 * The pending promise is registered with its owner (`opts.owner`, else the
 * ambient owner). Owner disposal rejects the promise with
 * `StreamDisposedError` and tears down the upstream connection.
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
