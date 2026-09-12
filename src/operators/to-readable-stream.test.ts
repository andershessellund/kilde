// ---------------------------------------------------------------------------
// toReadableStream — tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { stream } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { map } from './map.js';
import { toReadableStream } from './to-readable-stream.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Collect all chunks from a ReadableStream into an array. */
async function collectReadableStream<T>(rs: ReadableStream<T>): Promise<T[]> {
  const reader = rs.getReader();
  const result: T[] = [];
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    result.push(value!);
  }
  return result;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('toReadableStream', () => {
  it('converts a source to a ReadableStream', async () => {
    const rs = stream(fromArray([1, 2, 3]), toReadableStream());
    const values = await collectReadableStream(rs);
    expect(values).toEqual([1, 2, 3]);
  });

  it('works in a pipeline with map', async () => {
    const rs = stream(
      fromArray([1, 2, 3]),
      map((x) => x * 10),
      toReadableStream(),
    );
    const values = await collectReadableStream(rs);
    expect(values).toEqual([10, 20, 30]);
  });

  it('handles empty source', async () => {
    const rs = stream(fromArray<number>([]), toReadableStream());
    const values = await collectReadableStream(rs);
    expect(values).toEqual([]);
  });

  it('handles single value', async () => {
    const rs = stream(fromArray([42]), toReadableStream());
    const values = await collectReadableStream(rs);
    expect(values).toEqual([42]);
  });

  it('handles string values', async () => {
    const rs = stream(fromArray(['hello', 'world']), toReadableStream());
    const values = await collectReadableStream(rs);
    expect(values).toEqual(['hello', 'world']);
  });

  it('cancellation disposes the upstream', async () => {
    let disposed = false;
    const src = fromArray([1, 2, 3, 4, 5]);
    // Wrap to detect dispose
    const wrappedSrc = {
      connect(sink: any) {
        const s = src.connect(sink);
        return {
          resume: () => s.resume(),
          [Symbol.dispose]() {
            disposed = true;
            s[Symbol.dispose]();
          },
        };
      },
    };

    const rs = stream(wrappedSrc as any, toReadableStream());
    const reader = rs.getReader();
    await reader.read(); // read first value
    await reader.cancel();

    expect(disposed).toBe(true);
  });

  it('handles many values with natural backpressure', async () => {
    const values = Array.from({ length: 100 }, (_, i) => i);
    const rs = stream(fromArray(values), toReadableStream());
    const result = await collectReadableStream(rs);
    expect(result).toEqual(values);
  });
});
