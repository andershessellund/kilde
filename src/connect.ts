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
import { currentOwner } from './owner.js';
import type { OwnedOptions } from './owner.js';
import { StreamDisposedError } from './stream-disposed-error.js';

export { StreamDisposedError } from './stream-disposed-error.js';

/**
 * Connect a source to a sink, registering the connection with an owner:
 * `opts.owner`, else the ambient owner (see `currentOwner`).
 *
 * When the owner is disposed, the connection is disposed and the sink
 * receives a `StreamDisposedError`. This ensures that promises or other
 * consumers waiting on the stream are notified rather than hanging forever.
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

  // Wrap the sink to track settlement (complete or error)
  const trackedSink: Sink<T> = {
    next(value: T) {
      return sink.next(value);
    },
    complete() {
      settled = true;
      handle?.unregister();
      sink.complete();
    },
    error(err: unknown) {
      settled = true;
      handle?.unregister();
      sink.error(err);
    },
  };

  const rawConn = source.connect(trackedSink);

  // Wrap the connection to intercept external disposal
  const trackedConn: Stream = {
    resume() {
      rawConn.resume();
    },
    [Symbol.dispose]() {
      if (disposed) return;
      disposed = true;
      handle?.unregister();
      rawConn[Symbol.dispose]();

      // If the stream hasn't settled yet, notify the sink
      if (!settled) {
        settled = true;
        sink.error(new StreamDisposedError());
      }
    },
  };

  // Register with the owner (graceful — untracked if the owner declines).
  const dispose = () => trackedConn[Symbol.dispose]();
  const handle = (opts?.owner ?? currentOwner()).register(
    { [Symbol.dispose]: dispose },
    'stream',
  );

  return trackedConn;
}
