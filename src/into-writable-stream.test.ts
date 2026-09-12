// ---------------------------------------------------------------------------
// intoWritableStream — tests (operator: stream(..., intoWritableStream(ws)))
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { fromArray } from './sources/from-array.js';
import { createRelay } from './relay.js';
import { stream, pipe } from './stream.js';
import { map } from './operators/map.js';
import { filter } from './operators/filter.js';
import { intoWritableStream } from './into-writable-stream.js';
import type { Source, Sink, Stream as StreamConnection } from './types.js';
import { PAUSE } from './types.js';
import { testSink } from './testing/test-sink.js';
import { assertProtocol } from './testing/protocol.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Create a WritableStream that collects values into an array. */
function collectWritableStream<T>(): { writable: WritableStream<T>; chunks: T[] } {
  const chunks: T[] = [];
  const writable = new WritableStream<T>({
    write(chunk) {
      chunks.push(chunk);
    },
  });
  return { writable, chunks };
}

/** A WritableStream whose writes take a macrotask each (exerts backpressure). */
function slowWritableStream<T>(): { writable: WritableStream<T>; chunks: T[] } {
  const chunks: T[] = [];
  const writable = new WritableStream<T>(
    {
      async write(chunk) {
        await new Promise((r) => setTimeout(r, 1));
        chunks.push(chunk);
      },
    },
    { highWaterMark: 1 },
  );
  return { writable, chunks };
}

function tracked<T>(source: Source<T>): Source<T> & { connects: number; disposes: number } {
  const t = {
    connects: 0,
    disposes: 0,
    connect(sink: Sink<T>) {
      t.connects++;
      const s = source.connect(sink);
      return {
        resume: () => s.resume(),
        [Symbol.dispose]: () => {
          t.disposes++;
          s[Symbol.dispose]();
        },
      };
    },
  };
  return t;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('intoWritableStream (operator)', () => {
  it('writes all values to the writable stream', async () => {
    const { writable, chunks } = collectWritableStream<number>();
    await stream(fromArray([1, 2, 3]), intoWritableStream(writable));
    expect(chunks).toEqual([1, 2, 3]);
  });

  it('works in a pipeline', async () => {
    const { writable, chunks } = collectWritableStream<number>();
    await stream(
      fromArray([1, 2, 3, 4, 5]),
      filter((x) => x % 2 === 1),
      map((x) => x * 10),
      intoWritableStream(writable),
    );
    expect(chunks).toEqual([10, 30, 50]);
  });

  it('handles empty source', async () => {
    const { writable, chunks } = collectWritableStream<number>();
    await stream(fromArray<number>([]), intoWritableStream(writable));
    expect(chunks).toEqual([]);
  });

  it('handles string values', async () => {
    const { writable, chunks } = collectWritableStream<string>();
    await stream(fromArray(['hello\n', 'world\n']), intoWritableStream(writable));
    expect(chunks).toEqual(['hello\n', 'world\n']);
  });

  it('propagates source errors', async () => {
    const errorSource: Source<number> = {
      connect(sink: Sink<number>): StreamConnection {
        return {
          resume() {
            sink.next(1);
            sink.error(new Error('source failed'));
          },
          [Symbol.dispose]() {},
        };
      },
    };

    const { writable } = collectWritableStream<number>();
    await expect(stream(errorSource, intoWritableStream(writable))).rejects.toThrow('source failed');
  });

  it('handles many values', async () => {
    const values = Array.from({ length: 100 }, (_, i) => i);
    const { writable, chunks } = collectWritableStream<number>();
    await stream(fromArray(values), intoWritableStream(writable));
    expect(chunks).toEqual(values);
  });

  it('applies backpressure to the source and preserves order', async () => {
    const values = Array.from({ length: 20 }, (_, i) => i);
    const pauses: number[] = [];
    const src: Source<number> = {
      connect(sink: Sink<number>) {
        const s = fromArray(values).connect({
          next(v) {
            const r = sink.next(v);
            if (r === PAUSE) pauses.push(v);
            return r;
          },
          complete: () => sink.complete(),
          error: (e) => sink.error(e),
        });
        return s;
      },
    };
    const { writable, chunks } = slowWritableStream<number>();
    await stream(src, intoWritableStream(writable));
    expect(chunks).toEqual(values);
    expect(pauses.length).toBeGreaterThan(0);
  });

  it('complete() arriving while paused still lands every queued write', async () => {
    const relay = createRelay<number>();
    const { writable, chunks } = slowWritableStream<number>();
    const done = stream(relay, intoWritableStream(writable));
    relay.next(1);
    relay.next(2); // adapter is paused on the writer now; relay buffers 2
    relay.complete(); // terminal while paused
    await done;
    expect(chunks).toEqual([1, 2]);
  });

  it('a failing writable disposes the upstream and rejects', async () => {
    const relay = createRelay<number>();
    const src = tracked(relay);
    const writable = new WritableStream<number>({
      write() {
        throw new Error('sink broke');
      },
    });
    const done = stream(src, intoWritableStream(writable));
    relay.next(1);
    await expect(done).rejects.toThrow('sink broke');
    expect(src.disposes).toBe(1);
  });

  it('bug 12: a second resume() does not open a second upstream connection', () => {
    const src = tracked(fromArray([1]));
    const { writable } = collectWritableStream<number>();
    const sink = testSink<Promise<void>>();
    const s = pipe(src, intoWritableStream(writable), assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    expect(src.connects).toBe(1);
    expect(sink.values).toHaveLength(1);
    expect(sink.completeCount).toBe(1);
  });
});
