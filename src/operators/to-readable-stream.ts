// ---------------------------------------------------------------------------
// toReadableStream — kilde Source → Web ReadableStream
//
// Creates a ReadableStream that pulls from a kilde source.
// Backpressure maps 1:1:
//   ReadableStream pull() → resume()
//   desiredSize ≤ 0       → PAUSE
//   ReadableStream cancel  → [Symbol.dispose]
//
// Zero buffering in the adapter — the ReadableStream's internal queue
// (controlled by highWaterMark) is the only buffer.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream as StreamConnection, Operator, PAUSE } from '../types.js';
import { PAUSE as PAUSE_SYM } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

function sourceToReadableStream<T>(source: Source<T>): ReadableStream<T> {
  let conn: StreamConnection | undefined;
  let pendingResolve: (() => void) | null = null;

  const sink: Sink<T> = {
    next(value: T): undefined | PAUSE {
      controller!.enqueue(value);
      if (controller!.desiredSize !== null && controller!.desiredSize <= 0) {
        const r = pendingResolve;
        pendingResolve = null;
        r?.();
        return PAUSE_SYM;
      }
      return undefined;
    },
    complete() {
      controller!.close();
      const r = pendingResolve;
      pendingResolve = null;
      r?.();
    },
    error(err: unknown) {
      controller!.error(err);
      const r = pendingResolve;
      pendingResolve = null;
      r?.();
    },
  };

  let controller: ReadableStreamDefaultController<T> | null = null;

  return new ReadableStream<T>({
    start(c) {
      controller = c;
    },
    pull() {
      return new Promise<void>((resolve) => {
        pendingResolve = resolve;
        if (!conn) {
          conn = source.connect(sink);
        }
        conn.resume();
      });
    },
    cancel() {
      conn?.[Symbol.dispose]();
    },
  });
}

class ToReadableStreamSource<T> extends AbstractSource<ReadableStream<T>> {
  constructor(private readonly source: Source<T>) {
    super();
  }

  connect(sink: Sink<ReadableStream<T>>): StreamConnection {
    const source = this.source;
    return {
      resume() {
        const rs = sourceToReadableStream(source);
        sink.next(rs);
        sink.complete();
      },
      [Symbol.dispose]() {},
    };
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
 * (controlled by `highWaterMark`, default 1) is the only buffer.
 */
export function toReadableStream<T>(): Operator<T, ReadableStream<T>> {
  return (source) => new ToReadableStreamSource(source);
}
