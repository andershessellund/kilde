// ---------------------------------------------------------------------------
// toReadable — tests (kilde Source → Node.js Readable)
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { Readable } from 'node:stream';
import { stream } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { map } from '../operators/map.js';
import { toReadable, sourceToReadable } from './to-readable.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Collect all chunks from a Node Readable into an array. */
async function collectReadable<T>(readable: Readable): Promise<T[]> {
  const result: T[] = [];
  for await (const chunk of readable) {
    result.push(chunk as T);
  }
  return result;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('toReadable', () => {
  it('converts a source to a Node Readable (operator)', async () => {
    const readable = stream(fromArray([1, 2, 3]), toReadable());
    const values = await collectReadable<number>(readable);
    expect(values).toEqual([1, 2, 3]);
  });

  it('works in a pipeline with map', async () => {
    const readable = stream(
      fromArray([1, 2, 3]),
      map((x) => x * 10),
      toReadable(),
    );
    const values = await collectReadable<number>(readable);
    expect(values).toEqual([10, 20, 30]);
  });

  it('handles empty source', async () => {
    const readable = stream(fromArray<number>([]), toReadable());
    const values = await collectReadable<number>(readable);
    expect(values).toEqual([]);
  });

  it('handles single value', async () => {
    const readable = stream(fromArray([42]), toReadable());
    const values = await collectReadable<number>(readable);
    expect(values).toEqual([42]);
  });

  it('handles string values', async () => {
    const readable = stream(fromArray(['hello', 'world']), toReadable());
    const values = await collectReadable<string>(readable);
    expect(values).toEqual(['hello', 'world']);
  });

  it('handles many values', async () => {
    const input = Array.from({ length: 100 }, (_, i) => i);
    const readable = stream(fromArray(input), toReadable());
    const values = await collectReadable<number>(readable);
    expect(values).toEqual(input);
  });

  it('destroy disposes the upstream', async () => {
    const readable = stream(fromArray([1, 2, 3, 4, 5]), toReadable());

    // Read one value then destroy
    const reader = readable[Symbol.asyncIterator]();
    const first = await reader.next();
    expect(first.value).toBe(1);
    readable.destroy();

    // Give time for cleanup
    await new Promise((r) => setTimeout(r, 20));
    expect(readable.destroyed).toBe(true);
  });
});

describe('sourceToReadable (standalone)', () => {
  it('converts a source directly', async () => {
    const readable = sourceToReadable(fromArray([10, 20, 30]));
    const values = await collectReadable<number>(readable);
    expect(values).toEqual([10, 20, 30]);
  });

  it('is a proper Node Readable (object mode)', async () => {
    const readable = sourceToReadable(fromArray([1, 2]));
    expect(readable).toBeInstanceOf(Readable);
    expect(readable.readableObjectMode).toBe(true);
  });
});
