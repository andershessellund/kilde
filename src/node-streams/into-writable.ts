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
//   writable 'error'               → source disposed, promise rejects
//   'close' before 'finish'        → source disposed, promise rejects
//                                    (PrematureCloseError)
//   write() throws synchronously   → source disposed, writable destroyed,
//                                    promise rejects
//
// Once the promise has settled the adapter detaches from the writable. A
// no-op 'error' listener is left behind so a late 'error' on the writable
// cannot crash the process.
// ---------------------------------------------------------------------------

import type { Writable } from 'node:stream';
import type { Source, Sink, Stream as StreamConnection, Operator, PAUSE } from '../types.js';
import { PAUSE as PAUSE_SYM } from '../types.js';
import { AbstractSource } from '../abstract-source.js';
import { SingleValueStream } from '../internal/single-value-stream.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { PrematureCloseError } from './premature-close.js';

const noop = () => {};

interface Run {
  readonly promise: Promise<void>;
  /** Stop: dispose the upstream connection and reject the promise. */
  abort(): void;
}

function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}

/** Connect a source to a writable. Returns the completion promise and an abort handle. */
function runIntoWritable<T>(source: Source<T>, writable: Writable): Run {
  let conn: StreamConnection | undefined;
  let settled = false;
  let settle!: (err?: unknown) => void;

  const promise = new Promise<void>((resolve, reject) => {
    settle = (err?: unknown) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (err !== undefined) reject(err);
      else resolve();
    };
  });

  function cleanup() {
    writable.removeListener('drain', onDrain);
    writable.removeListener('finish', onFinish);
    writable.removeListener('error', onError);
    writable.removeListener('close', onClose);
    writable.on('error', noop);
  }

  /** The writable side broke: drop the source, reject. */
  function fail(err: unknown) {
    if (settled) return;
    conn?.[Symbol.dispose]();
    settle(err);
  }

  function onDrain() {
    conn?.resume();
  }

  function onFinish() {
    settle();
  }

  function onError(err: Error) {
    fail(err);
  }

  function onClose() {
    // 'close' after 'finish' has already settled us; here it came first.
    fail(new PrematureCloseError());
  }

  const sink: Sink<T> = {
    next(value: T): undefined | PAUSE {
      if (settled) return PAUSE_SYM;
      let ok: boolean;
      try {
        ok = writable.write(value as any);
      } catch (err) {
        // e.g. ERR_STREAM_NULL_VALUES — the pipeline is broken.
        fail(err);
        writable.destroy(toError(err));
        return PAUSE_SYM;
      }
      return ok ? undefined : PAUSE_SYM;
    },
    complete() {
      if (!settled) writable.end();
    },
    error(err: unknown) {
      if (!settled) writable.destroy(toError(err));
    },
  };

  writable.on('drain', onDrain);
  writable.on('finish', onFinish);
  writable.on('error', onError);
  writable.on('close', onClose);

  // Bail out early if the writable is already unusable.
  if (writable.errored) {
    fail(writable.errored);
  } else if (writable.writableEnded) {
    fail(new Error('intoWritable(): the writable has already ended'));
  } else if (writable.destroyed) {
    fail(new PrematureCloseError());
  } else {
    conn = source.connect(sink);
    conn.resume();
  }

  return {
    promise,
    abort() {
      fail(new StreamDisposedError());
    },
  };
}

/** Connect a source to a writable, returning a promise for completion. */
function sourceIntoWritable<T>(source: Source<T>, writable: Writable): Promise<void> {
  return runIntoWritable(source, writable).promise;
}

class IntoWritableSource<T> extends AbstractSource<Promise<void>> {
  constructor(
    private readonly source: Source<T>,
    private readonly writable: Writable,
  ) {
    super();
  }

  connect(sink: Sink<Promise<void>>): StreamConnection {
    let run: Run | undefined;
    return new SingleValueStream<Promise<void>>(
      sink,
      () => (run = runIntoWritable(this.source, this.writable)).promise,
      () => run?.abort(),
    );
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
 * - Writable `'error'` → source disposed, promise rejects with the error
 * - Writable `'close'` before `'finish'` → source disposed, promise rejects
 *   with `PrematureCloseError` (`code: 'ERR_STREAM_PREMATURE_CLOSE'`)
 * - `write()` throws synchronously (e.g. a `null` chunk) → source disposed,
 *   writable destroyed, promise rejects with the thrown error
 *
 * The consumption starts once, on the first `resume()`. Disposing the
 * connection while it is running disconnects the source and rejects the
 * promise with `StreamDisposedError`; the writable itself is left alone.
 *
 * After the promise settles the adapter detaches from the writable, leaving
 * a no-op `'error'` listener so a late error cannot crash the process.
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
