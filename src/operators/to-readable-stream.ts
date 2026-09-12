// ---------------------------------------------------------------------------
// toReadableStream — kilde Source → Web ReadableStream
//
// Creates a ReadableStream that pulls from a kilde source.
// Backpressure maps 1:1:
//   ReadableStream pull() → resume()   (only while the source is paused)
//   desiredSize ≤ 0       → PAUSE
//   ReadableStream cancel  → [Symbol.dispose]
//
// Zero buffering in the adapter — the ReadableStream's internal queue
// (controlled by highWaterMark) is the only buffer. Because an error on
// the controller discards that queue, an upstream error that arrives
// while chunks are still queued is held until the consumer has drained
// them (mirrors the "terminal after the buffer" rule of the operators).
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream as StreamConnection, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';
import { SingleValueStream } from '../internal/single-value-stream.js';

function sourceToReadableStream<T>(source: Source<T>): ReadableStream<T> {
  let controller!: ReadableStreamDefaultController<T>;
  let highWaterMark = 0;
  let conn: StreamConnection | undefined;
  let pendingPull: (() => void) | null = null;
  let upstreamPaused = true; // not started, or PAUSE returned
  let done = false;
  let pendingError: { error: unknown } | null = null;

  const settlePull = () => {
    const resolve = pendingPull;
    pendingPull = null;
    resolve?.();
  };

  const queueIsEmpty = () =>
    controller.desiredSize === null || controller.desiredSize >= highWaterMark;

  const sink: Sink<T> = {
    next(value: T): undefined | PAUSE {
      if (done) return PAUSE;
      controller.enqueue(value);
      if (controller.desiredSize !== null && controller.desiredSize <= 0) {
        upstreamPaused = true;
        settlePull();
        return PAUSE;
      }
      return undefined;
    },
    complete() {
      if (done) return;
      done = true;
      controller.close(); // queued chunks are still delivered
      settlePull();
    },
    error(err: unknown) {
      if (done) return;
      done = true;
      if (queueIsEmpty()) controller.error(err);
      else pendingError = { error: err }; // after the queue has drained
      settlePull();
    },
  };

  return new ReadableStream<T>({
    start(c) {
      controller = c;
      highWaterMark = c.desiredSize ?? 0;
    },
    pull() {
      if (done) {
        if (pendingError && queueIsEmpty()) {
          const { error } = pendingError;
          pendingError = null;
          controller.error(error);
        }
        return;
      }
      return new Promise<void>((resolve) => {
        pendingPull = resolve;
        conn ??= source.connect(sink);
        if (upstreamPaused) {
          upstreamPaused = false;
          conn.resume();
        }
      });
    },
    cancel() {
      done = true;
      pendingError = null;
      conn?.[Symbol.dispose]();
      settlePull();
    },
  });
}

class ToReadableStreamSource<T> extends AbstractSource<ReadableStream<T>> {
  constructor(private readonly source: Source<T>) {
    super();
  }

  connect(sink: Sink<ReadableStream<T>>): StreamConnection {
    return new SingleValueStream(sink, () => sourceToReadableStream(this.source));
  }
}

/**
 * Convert a kilde source into a Web `ReadableStream<T>`.
 *
 * Returns an operator that emits the ReadableStream as a single value,
 * suitable as a terminal in `stream()`:
 *
 * ```ts
 * const rs = stream(mySource, map(transform), toReadableStream());
 * // rs is ReadableStream<T> — lazy, data flows when consumed
 * ```
 *
 * Backpressure flows end-to-end:
 * - Consumer reads → ReadableStream pull() → resume()
 * - ReadableStream buffer full (desiredSize ≤ 0) → PAUSE
 * - Consumer cancels → [Symbol.dispose]
 *
 * Zero buffering in the adapter — the ReadableStream's internal queue
 * (controlled by `highWaterMark`, default 1) is the only buffer. An
 * upstream error is surfaced to the reader only after queued chunks have
 * been read; completion lets queued chunks drain naturally.
 */
export function toReadableStream<T>(): Operator<T, ReadableStream<T>> {
  return (source) => new ToReadableStreamSource(source);
}
