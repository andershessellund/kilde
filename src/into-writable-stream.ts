// ---------------------------------------------------------------------------
// intoWritableStream — kilde Source → Web WritableStream (operator)
//
// Terminal operator that consumes upstream and writes values directly
// into a Web WritableStream. Emits a single Promise<void> that resolves
// when the writer closes. Backpressure maps 1:1 via desiredSize/ready:
//
//   writer.desiredSize ≤ 0 → PAUSE
//   writer.ready resolves  → resume()
//   source complete        → writer.close()
//   source error           → writer.abort(err)
//   writer error           → source [Symbol.dispose]
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream as StreamConnection, Operator, PAUSE } from './types.js';
import { PAUSE as PAUSE_SYM } from './types.js';
import { AbstractSource } from './abstract-source.js';

/** Connect a source to a WritableStream writer, returning a promise. */
function sourceIntoWritableStream<T>(
  source: Source<T>,
  writable: WritableStream<T>,
): Promise<void> {
  const writer = writable.getWriter();
  let conn: StreamConnection;
  let paused = false;
  let completed = false;
  let errored = false;

  function finish() {
    completed = true;
    writer.close().catch(() => {});
  }

  function fail(err: unknown) {
    if (errored) return;
    errored = true;
    writer.abort(err).catch(() => {});
  }

  const sink: Sink<T> = {
    next(value: T): undefined | PAUSE {
      if (completed || errored) return PAUSE_SYM;

      writer.write(value).catch(() => {
        conn[Symbol.dispose]();
      });

      if (writer.desiredSize !== null && writer.desiredSize <= 0) {
        paused = true;
        writer.ready.then(
          () => {
            if (paused && !completed && !errored) {
              paused = false;
              conn.resume();
            }
          },
          () => {
            conn[Symbol.dispose]();
          },
        );
        return PAUSE_SYM;
      }
      return undefined;
    },
    complete() {
      finish();
    },
    error(err: unknown) {
      fail(err);
    },
  };

  conn = source.connect(sink);
  conn.resume();

  return writer.closed.then(
    () => {},
    (err) => {
      conn[Symbol.dispose]();
      throw err;
    },
  );
}

class IntoWritableStreamSource<T> extends AbstractSource<Promise<void>> {
  constructor(
    private readonly source: Source<T>,
    private readonly writable: WritableStream<T>,
  ) {
    super();
  }

  connect(sink: Sink<Promise<void>>): StreamConnection {
    const source = this.source;
    const writable = this.writable;
    return {
      resume() {
        const promise = sourceIntoWritableStream(source, writable);
        sink.next(promise);
        sink.complete();
      },
      [Symbol.dispose]() {},
    };
  }
}

/**
 * Terminal operator: consume upstream by writing into a Web `WritableStream`.
 *
 * Returns an `Operator<T, Promise<void>>`. Use as the final step in
 * `stream()` — extracts a `Promise<void>` that resolves when the
 * writer closes.
 *
 * Backpressure flows end-to-end via the writer's `ready` promise.
 *
 * @example
 * ```ts
 * import { stream, fromReadableStream, lines, map, intoWritableStream } from 'kilde';
 *
 * await stream(
 *   fromReadableStream(response.body!),
 *   lines(),
 *   map(line => JSON.parse(line)),
 *   map(event => JSON.stringify(event) + '\n'),
 *   intoWritableStream(writable),
 * );
 * ```
 */
export function intoWritableStream<T>(writable: WritableStream<T>): Operator<T, Promise<void>> {
  return (source) => new IntoWritableStreamSource(source, writable);
}

export { sourceIntoWritableStream };
