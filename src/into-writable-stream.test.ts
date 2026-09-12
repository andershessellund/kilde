// ---------------------------------------------------------------------------
// intoWritableStream — tests (operator: stream(..., intoWritableStream(ws)))
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { fromArray } from './sources/from-array.js';
import { stream } from './stream.js';
import { map } from './operators/map.js';
import { filter } from './operators/filter.js';
import { intoWritableStream } from './into-writable-stream.js';
import type { Source, Sink, Stream as StreamConnection } from './types.js';

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
    await expect(stream(errorSource, intoWritableStream(writable))).rejects.toThrow();
  });

  it('handles many values', async () => {
    const values = Array.from({ length: 100 }, (_, i) => i);
    const { writable, chunks } = collectWritableStream<number>();
    await stream(fromArray(values), intoWritableStream(writable));
    expect(chunks).toEqual(values);
  });
});
