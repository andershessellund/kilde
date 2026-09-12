// ---------------------------------------------------------------------------
// connect() — tracked stream connection (manual boundary API)
//
// Wraps source.connect(sink) to:
// 1. Register the connection with its owner (explicit or ambient)
// 2. On external disposal (owner teardown), notify the sink with an error
//
// Most code should NOT use this directly. Edge operators (toPromise,
// toAsyncIterable) handle resource registration internally. Use connect()
// only when manually wiring a source to a sink at the application boundary.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream } from './types.js';
import { PAUSE } from './types.js';
import type { OwnedOptions } from './owner.js';
import { registerWithOwner } from './internal/owned.js';
import { StreamDisposedError } from './stream-disposed-error.js';

export { StreamDisposedError } from './stream-disposed-error.js';

/**
 * Connect a source to a sink, registering the connection with an owner:
 * `opts.owner`, else the ambient owner (see `currentOwner`).
 *
 * When the owner is disposed, the connection is disposed and the sink
 * receives a `StreamDisposedError`. This ensures that promises or other
 * consumers waiting on the stream are notified rather than hanging forever.
 * Disposing the returned handle yourself does the same: an unsettled sink
 * receives `StreamDisposedError` so that whatever is waiting on it can
 * finish. (A plain `source.connect()` stream tells the sink nothing.)
 *
 * With no owner, the connection is untracked — just like calling
 * `source.connect(sink)` directly — and the returned handle is the only way
 * to tear it down.
 *
 * @example
 * ```ts
 * const conn = connect(source, {
 *   next(v) { values.push(v); return undefined; },
 *   complete() { done = true; },
 *   error(err) { error = err; },
 * });
 * conn.resume();
 * ```
 */
export function connect<T>(source: Source<T>, sink: Sink<T>, opts?: OwnedOptions): Stream {
  let settled = false;
  let disposed = false;
  let rawConn: Stream | null = null;

  // Tear down, and tell a sink that has not settled yet. Used both for owner
  // disposal and for explicit disposal of the returned handle.
  const dispose = () => {
    if (disposed) return;
    disposed = true;
    registration.unregister();
    rawConn?.[Symbol.dispose]();
    if (!settled) {
      settled = true;
      sink.error(new StreamDisposedError());
    }
  };

  // Register first: a source may settle synchronously during the first
  // resume(), and the sink then unregisters — the handle must already exist.
  const registration = registerWithOwner(opts?.owner, 'stream', dispose);

  // Wrap the sink to track settlement (complete or error)
  const trackedSink: Sink<T> = {
    next(value: T) {
      if (settled || disposed) return PAUSE;
      return sink.next(value);
    },
    complete() {
      if (settled) return;
      settled = true;
      registration.unregister();
      sink.complete();
    },
    error(err: unknown) {
      if (settled) return;
      settled = true;
      registration.unregister();
      sink.error(err);
    },
  };

  rawConn = source.connect(trackedSink);

  return {
    resume() {
      if (settled || disposed) return;
      rawConn!.resume();
    },
    [Symbol.dispose]: dispose,
  };
}
