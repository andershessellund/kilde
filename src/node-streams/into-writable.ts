// ---------------------------------------------------------------------------
// intoWritable — kilde Source → Node Writable (operator, direct sink)
//
// Terminal operator that consumes upstream and writes values directly
// into a Node.js Writable. Emits a single Promise<void> that resolves
// when the writable finishes. Backpressure maps 1:1:
//
//   writable.write() returns false → PAUSE
//   'drain' event                  → resume()
//   source complete                → writable.end()
//   source error                   → writable.destroy(err)
//   writable error/close           → source [Symbol.dispose]
// ---------------------------------------------------------------------------

import type { Writable } from 'node:stream';
import type { Source, Sink, Stream as StreamConnection, Operator, PAUSE } from '../types.js';
import { PAUSE as PAUSE_SYM } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

/** Connect a source to a writable, returning a promise for completion. */
function sourceIntoWritable<T>(source: Source<T>, writable: Writable): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let settled = false;
    let conn: StreamConnection;

    function cleanup() {
      writable.removeListener('drain', onDrain);
      writable.removeListener('finish', onFinish);
      writable.removeListener('error', onError);
      writable.removeListener('close', onClose);
    }

    function settle(err?: unknown) {
      if (settled) return;
      settled = true;
      cleanup();
      if (err) reject(err);
      else resolve();
    }

    function onDrain() {
      conn.resume();
    }

    function onFinish() {
      settle();
    }

    function onError(err: Error) {
      conn[Symbol.dispose]();
      settle(err);
    }

    function onClose() {
      if (!settled) {
        conn[Symbol.dispose]();
        settle();
      }
    }

    const sink: Sink<T> = {
      next(value: T): undefined | PAUSE {
        if (settled) return PAUSE_SYM;
        const ok = writable.write(value as any);
        if (!ok) return PAUSE_SYM;
        return undefined;
      },
      complete() {
        if (!settled) writable.end();
      },
      error(err: unknown) {
        if (!settled) {
          writable.destroy(err instanceof Error ? err : new Error(String(err)));
        }
      },
    };

    writable.on('drain', onDrain);
    writable.on('finish', onFinish);
    writable.on('error', onError);
    writable.on('close', onClose);

    conn = source.connect(sink);
    conn.resume();
  });
}

class IntoWritableSource<T> extends AbstractSource<Promise<void>> {
  constructor(
    private readonly source: Source<T>,
    private readonly writable: Writable,
  ) {
    super();
  }

  connect(sink: Sink<Promise<void>>): StreamConnection {
    const source = this.source;
    const writable = this.writable;
    return {
      resume() {
        const promise = sourceIntoWritable(source, writable);
        sink.next(promise);
        sink.complete();
      },
      [Symbol.dispose]() {},
    };
  }
}

/**
 * Terminal operator: consume upstream by writing into a Node.js `Writable`.
 *
 * Returns an `Operator<T, Promise<void>>`. Use as the final step in
 * `stream()` — extracts a `Promise<void>` that resolves when the
 * writable finishes.
 *
 * Backpressure flows end-to-end:
 * - `writable.write(value)` returns `false` → source pauses (PAUSE)
 * - `'drain'` event fires → source resumes
 * - Source completes → `writable.end()`
 * - Source errors → `writable.destroy(err)`
 * - Writable errors/closes early → source disposed
 *
 * @example
 * ```ts
 * import { stream, fromReadable, lines, map, filter } from 'kilde';
 * import { intoWritable } from 'kilde/node';
 *
 * res.writeHead(200, { 'Content-Type': 'application/x-ndjson' });
 * await stream(
 *   fromReadable(createReadStream('events.ndjson')),
 *   lines(),
 *   filter(line => line.length > 0),
 *   map(line => JSON.parse(line)),
 *   filter(event => event.type === 'purchase'),
 *   map(event => JSON.stringify(event) + '\n'),
 *   intoWritable(res),
 * );
 * ```
 */
export function intoWritable<T>(writable: Writable): Operator<T, Promise<void>> {
  return (source) => new IntoWritableSource(source, writable);
}

export { sourceIntoWritable };
