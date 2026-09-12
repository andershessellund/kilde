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
import type { Source, Sink, Stream as StreamConnection } from '../types.js';

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
});
