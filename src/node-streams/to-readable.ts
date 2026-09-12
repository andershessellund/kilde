// ---------------------------------------------------------------------------
// toReadable — kilde Source → Node Readable
//
// Creates a Node.js Readable (object mode) from a kilde source.
// Backpressure maps 1:1:
//   Readable._read()    → resume()
//   push() returns false → PAUSE
//   Readable.destroy()  → [Symbol.dispose]
//
// Zero buffering in the adapter — Node's internal highWaterMark buffer
// is the only buffer.
//
// `null` is the end-of-stream marker for readable.push(), so a source value
// of `null` cannot be represented: it destroys the Readable with an error.
// ---------------------------------------------------------------------------

import { Readable } from 'node:stream';
import type { Source, Sink, Stream as StreamConnection, Operator, PAUSE } from '../types.js';
import { PAUSE as PAUSE_SYM } from '../types.js';
import { AbstractSource } from '../abstract-source.js';
import { SingleValueStream } from '../internal/single-value-stream.js';

function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

function sourceToReadable<T>(source: Source<T>): Readable {
  let conn: StreamConnection | undefined;
  let terminated = false;

  const readable = new Readable({
    objectMode: true,
    read() {
      // Sources treat resume() while already active as a no-op, so Node
      // asking for more while we are still delivering is harmless.
      conn?.resume();
    },
    destroy(err, callback) {
      conn?.[Symbol.dispose]();
      callback(err);
    },
  });

  conn = source.connect({
    next(value: T): undefined | PAUSE {
      if (terminated) return PAUSE_SYM;
      if (value === null) {
        terminated = true;
        readable.destroy(
          new TypeError(
            'toReadable(): a source value of null cannot be pushed into a Readable — null is the end-of-stream marker',
          ),
        );
        return PAUSE_SYM;
      }
      return readable.push(value) ? undefined : PAUSE_SYM;
    },
    complete() {
      if (terminated) return;
      terminated = true;
      readable.push(null);
    },
    error(err: unknown) {
      if (terminated) return;
      terminated = true;
      readable.destroy(toError(err));
    },
  });

  return readable;
}

class ToReadableSource<T> extends AbstractSource<Readable> {
  constructor(private readonly source: Source<T>) {
    super();
  }

  connect(sink: Sink<Readable>): StreamConnection {
    let readable: Readable | undefined;
    return new SingleValueStream<Readable>(
      sink,
      () => (readable = sourceToReadable(this.source)),
      // Tearing down the Readable disposes the upstream connection.
      () => { if (readable && !readable.destroyed) readable.destroy(); },
    );
  }
}

/**
 * Convert a kilde source into a Node.js `Readable` stream (object mode).
 *
 * Returns an operator that emits the Readable as a single value,
 * suitable as a terminal in `stream()`:
 *
 * ```ts
 * const readable = stream(mySource, map(transform), toReadable());
 * readable.pipe(process.stdout);
 * ```
 *
 * Backpressure flows end-to-end:
 * - Consumer reads → Readable `_read()` → `resume()`
 * - Readable buffer full (`push()` returns `false`) → `PAUSE`
 * - Consumer destroys → `[Symbol.dispose]`
 *
 * Zero buffering in the adapter — Node's internal `highWaterMark`
 * buffer (default 16 objects in object mode) is the only buffer.
 *
 * **`null` values are an error.** `readable.push(null)` is Node's
 * end-of-stream marker, so a source that emits `null` cannot be represented;
 * the Readable is destroyed with a `TypeError` and the upstream is disposed.
 * Map such values to a sentinel first.
 *
 * The Readable is created once, on the first `resume()`. Disposing the
 * connection destroys the Readable (and with it the upstream connection).
 *
 * Also exported as `sourceToReadable(source)` standalone function.
 */
export function toReadable<T>(): Operator<T, Readable> {
  return (source) => new ToReadableSource(source);
}

export { sourceToReadable };
