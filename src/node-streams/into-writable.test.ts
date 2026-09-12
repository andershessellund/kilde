// ---------------------------------------------------------------------------
// intoWritable — tests (operator: stream(..., intoWritable(writable)))
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { Writable, PassThrough } from 'node:stream';
import { fromArray } from '../sources/from-array.js';
import { stream } from '../stream.js';
import { map } from '../operators/map.js';
import { filter } from '../operators/filter.js';
import { intoWritable, sourceIntoWritable } from './into-writable.js';
import { PrematureCloseError } from './premature-close.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { pipe } from '../stream.js';
import { testSink } from '../testing/test-sink.js';
import { assertProtocol } from '../testing/protocol.js';
import type { Source, Sink, Stream as StreamConnection } from '../types.js';

const tick = () => new Promise<void>((r) => setTimeout(r, 10));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Collect everything written into a Writable. */
function collectWritable(): Writable & { chunks: any[] } {
  const chunks: any[] = [];
  const writable = new Writable({
    objectMode: true,
    write(chunk, _enc, cb) {
      chunks.push(chunk);
      cb();
    },
  });
  (writable as any).chunks = chunks;
  return writable as Writable & { chunks: any[] };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('intoWritable (operator)', () => {
  it('writes all values to the writable', async () => {
    const w = collectWritable();
    await stream(fromArray([1, 2, 3]), intoWritable(w));
    expect(w.chunks).toEqual([1, 2, 3]);
  });

  it('works in a pipeline', async () => {
    const w = collectWritable();
    await stream(
      fromArray([1, 2, 3, 4, 5]),
      filter((x) => x % 2 === 1),
      map((x) => x * 10),
      intoWritable(w),
    );
    expect(w.chunks).toEqual([10, 30, 50]);
  });

  it('handles empty source', async () => {
    const w = collectWritable();
    await stream(fromArray<number>([]), intoWritable(w));
    expect(w.chunks).toEqual([]);
  });

  it('handles string values', async () => {
    const w = collectWritable();
    await stream(fromArray(['hello\n', 'world\n']), intoWritable(w));
    expect(w.chunks).toEqual(['hello\n', 'world\n']);
  });

  it('propagates source errors to writable', async () => {
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

    const w = collectWritable();
    await expect(stream(errorSource, intoWritable(w))).rejects.toThrow('source failed');
  });

  it('handles writable errors', async () => {
    let disposed = false;
    const failingWritable = new Writable({
      objectMode: true,
      write(_chunk, _enc, cb) {
        cb(new Error('write failed'));
      },
    });

    const src: Source<number> = {
      connect(sink: Sink<number>): StreamConnection {
        return {
          resume() {
            sink.next(1);
          },
          [Symbol.dispose]() {
            disposed = true;
          },
        };
      },
    };

    await expect(stream(src, intoWritable(failingWritable))).rejects.toThrow('write failed');
    expect(disposed).toBe(true);
  });

  it('respects backpressure from writable', async () => {
    let drainCount = 0;
    const chunks: number[] = [];

    const w = new Writable({
      objectMode: true,
      highWaterMark: 1,
      write(chunk, _enc, cb) {
        chunks.push(chunk);
        setTimeout(cb, 5);
      },
    });

    w.on('drain', () => drainCount++);

    const values = Array.from({ length: 10 }, (_, i) => i);
    await stream(fromArray(values), intoWritable(w));

    expect(chunks).toEqual(values);
    expect(drainCount).toBeGreaterThan(0);
  });

  it('pipes through a PassThrough stream', async () => {
    const pt = new PassThrough({ objectMode: true });
    const collected: number[] = [];

    pt.on('data', (chunk) => collected.push(chunk));
    const done = new Promise<void>((r) => pt.on('end', r));

    await stream(fromArray([10, 20, 30]), intoWritable(pt));
    await done;

    expect(collected).toEqual([10, 20, 30]);
  });
});

describe('sourceIntoWritable (standalone)', () => {
  it('writes all values to the writable', async () => {
    const w = collectWritable();
    await sourceIntoWritable(fromArray([1, 2, 3]), w);
    expect(w.chunks).toEqual([1, 2, 3]);
  });

  // -------------------------------------------------------------------------
  // Lifecycle edge cases
  // -------------------------------------------------------------------------

  it("'close' before 'finish' rejects with PrematureCloseError and disposes the source", async () => {
    let disposed = false;
    const src: Source<number> = {
      connect(sink) {
        return {
          resume() {
            sink.next(1);
          },
          [Symbol.dispose]() {
            disposed = true;
          },
        };
      },
    };
    const w = collectWritable();
    const promise = sourceIntoWritable(src, w);
    await tick();
    expect(w.chunks).toEqual([1]);
    w.destroy(); // 'close' only, no 'finish'
    await expect(promise).rejects.toBeInstanceOf(PrematureCloseError);
    expect(disposed).toBe(true);
  });

  it('a synchronous throw from write() rejects, disposes the source and detaches', async () => {
    let disposed = false;
    let resumedAfterThrow = false;
    let resumes = 0;
    const src: Source<unknown> = {
      connect(sink) {
        return {
          resume() {
            resumes++;
            if (resumes > 1) resumedAfterThrow = true;
            sink.next(1);
            sink.next(null); // ERR_STREAM_NULL_VALUES — thrown synchronously
            sink.next(2);
          },
          [Symbol.dispose]() {
            disposed = true;
          },
        };
      },
    };
    const w = collectWritable();
    const promise = sourceIntoWritable(src, w);
    await expect(promise).rejects.toMatchObject({ code: 'ERR_STREAM_NULL_VALUES' });
    expect(disposed).toBe(true);
    expect(w.chunks).toEqual([1]); // the value after the throw is refused
    expect(w.destroyed).toBe(true);
    expect(w.listenerCount('drain')).toBe(0);
    expect(w.listenerCount('finish')).toBe(0);
    expect(w.listenerCount('close')).toBe(0);
    await tick();
    expect(resumedAfterThrow).toBe(false);
  });

  it('after settling, a late error on the writable does not crash', async () => {
    const w = collectWritable();
    await sourceIntoWritable(fromArray([1]), w);
    expect(w.listenerCount('finish')).toBe(0);
    expect(w.listenerCount('close')).toBe(0);
    expect(() => w.emit('error', new Error('late'))).not.toThrow();
  });

  it('rejects immediately for an already-destroyed writable', async () => {
    const w = collectWritable();
    w.destroy();
    let connected = false;
    const src: Source<number> = {
      connect() {
        connected = true;
        return { resume() {}, [Symbol.dispose]() {} };
      },
    };
    await expect(sourceIntoWritable(src, w)).rejects.toBeInstanceOf(PrematureCloseError);
    expect(connected).toBe(false);
  });

  it('rejects for an already-ended writable instead of hanging', async () => {
    const w = collectWritable();
    w.end();
    await new Promise<void>((r) => w.once('finish', r));
    await expect(sourceIntoWritable(fromArray<number>([]), w)).rejects.toThrow(/already ended/);
  });
});

describe('intoWritable (operator connection)', () => {
  it('starts once: repeated resume() emits a single promise', async () => {
    const w = collectWritable();
    const sink = testSink<Promise<void>>();
    const conn = pipe(fromArray([1, 2]), intoWritable(w), assertProtocol()).connect(sink);
    conn.resume();
    conn.resume();
    conn.resume();
    expect(sink.values).toHaveLength(1);
    expect(sink.completeCount).toBe(1);
    await sink.values[0];
    expect(w.chunks).toEqual([1, 2]);
  });

  it('dispose while running disconnects the source and rejects with StreamDisposedError', async () => {
    let disposed = false;
    const src: Source<number> = {
      connect(sink) {
        return {
          resume() {
            sink.next(1);
          },
          [Symbol.dispose]() {
            disposed = true;
          },
        };
      },
    };
    const w = collectWritable();
    const sink = testSink<Promise<void>>();
    const conn = pipe(src, intoWritable(w)).connect(sink);
    conn.resume();
    const promise = sink.values[0];
    promise.catch(() => {});
    conn[Symbol.dispose]();
    expect(disposed).toBe(true);
    await expect(promise).rejects.toBeInstanceOf(StreamDisposedError);
    expect(w.destroyed).toBe(false); // the writable is left to its owner
  });

  it('dispose before resume: nothing starts', () => {
    let connected = false;
    const src: Source<number> = {
      connect() {
        connected = true;
        return { resume() {}, [Symbol.dispose]() {} };
      },
    };
    const sink = testSink<Promise<void>>();
    const conn = pipe(src, intoWritable(collectWritable())).connect(sink);
    conn[Symbol.dispose]();
    conn.resume();
    expect(connected).toBe(false);
    expect(sink.values).toHaveLength(0);
  });
});
