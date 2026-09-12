// ---------------------------------------------------------------------------
// intoWritableStream — kilde Source → Web WritableStream (operator)
//
// Terminal operator that consumes upstream and writes values directly
// into a Web WritableStream. Emits a single Promise<void> that resolves
// when the writer closes. Backpressure maps 1:1 via desiredSize/ready:
//
//   writer.desiredSize ≤ 0 → PAUSE
//   writer.ready resolves  → resume()
//   source complete        → writer.close()   (queued writes still land)
//   source error           → writer.abort(err)
//   writer error           → source [Symbol.dispose]
//
// A terminal event may arrive while the source is paused (waiting on
// writer.ready); the ready callback then finds the pipe finished and does
// not resume.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream as StreamConnection, Operator } from './types.js';
import { PAUSE } from './types.js';
import { AbstractSource } from './abstract-source.js';
import { SingleValueStream } from './internal/single-value-stream.js';

/** Connect a source to a WritableStream writer, returning a promise. */
function sourceIntoWritableStream<T>(
  source: Source<T>,
  writable: WritableStream<T>,
): Promise<void> {
  const writer = writable.getWriter();
  let conn: StreamConnection | undefined;
  let paused = false;
  let done = false; // source ended, or the writer failed

  const abandon = () => {
    if (done) return;
    done = true;
    conn?.[Symbol.dispose]();
  };

  const sink: Sink<T> = {
    next(value: T): undefined | PAUSE {
      if (done) return PAUSE;

      writer.write(value).catch(abandon);

      if (writer.desiredSize !== null && writer.desiredSize <= 0) {
        paused = true;
        writer.ready.then(
          () => {
            if (paused && !done) {
              paused = false;
              conn?.resume();
            }
          },
          abandon,
        );
        return PAUSE;
      }
      return undefined;
    },
    complete() {
      if (done) return;
      done = true;
      writer.close().catch(() => {});
    },
    error(err: unknown) {
      if (done) return;
      done = true;
      writer.abort(err).catch(() => {});
    },
  };

  conn = source.connect(sink);
  conn.resume();

  return writer.closed.then(
    () => {},
    (err) => {
      abandon();
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
    return new SingleValueStream(sink, () => sourceIntoWritableStream(this.source, this.writable));
  }
}

/**
 * Terminal operator: consume upstream by writing into a Web `WritableStream`.
 *
 * Returns an `Operator<T, Promise<void>>`. Use as the final step in
 * `stream()` — extracts a `Promise<void>` that resolves when the
 * writer closes, or rejects if the source errors (the writer is aborted
 * with the error) or the writable fails.
 *
 * Backpressure flows end-to-end via the writer's `ready` promise. The
 * upstream is connected on the first `resume()`; later `resume()` calls
 * are no-ops.
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
