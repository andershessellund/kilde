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
// ---------------------------------------------------------------------------

import { Readable } from 'node:stream';
import type { Source, Sink, Stream as StreamConnection, Operator, PAUSE } from '../types.js';
import { PAUSE as PAUSE_SYM } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

function sourceToReadable<T>(source: Source<T>): Readable {
  let conn: StreamConnection | undefined;

  const readable = new Readable({
    objectMode: true,
    read() {
      conn?.resume();
    },
    destroy(err, callback) {
      conn?.[Symbol.dispose]();
      callback(err);
    },
  });

  conn = source.connect({
    next(value: T): undefined | PAUSE {
      if (!readable.push(value)) {
        return PAUSE_SYM;
      }
      return undefined;
    },
    complete() {
      readable.push(null);
    },
    error(err: unknown) {
      readable.destroy(err instanceof Error ? err : new Error(String(err)));
    },
  });

  return readable;
}

class ToReadableSource<T> extends AbstractSource<Readable> {
  constructor(private readonly source: Source<T>) {
    super();
  }

  connect(sink: Sink<Readable>): StreamConnection {
    const source = this.source;
    return {
      resume() {
        const readable = sourceToReadable(source);
        sink.next(readable);
        sink.complete();
      },
      [Symbol.dispose]() {},
    };
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
 * Also exported as `sourceToReadable(source)` standalone function.
 */
export function toReadable<T>(): Operator<T, Readable> {
  return (source) => new ToReadableSource(source);
}

export { sourceToReadable };
