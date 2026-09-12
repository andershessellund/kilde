// ---------------------------------------------------------------------------
// toReadable — tests (kilde Source → Node.js Readable)
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { Readable } from 'node:stream';
import { stream } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { map } from '../operators/map.js';
import { toReadable, sourceToReadable } from './to-readable.js';
import { pipe } from '../stream.js';
import { fromSignal } from '../sources/from-signal.js';
import { createSignal } from '../signal.js';
import { PAUSE } from '../types.js';
import type { Source } from '../types.js';
import { testSink } from '../testing/test-sink.js';
import { assertProtocol } from '../testing/protocol.js';

const tick = () => new Promise<void>((r) => setTimeout(r, 10));

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

  it('a null value destroys the readable with a TypeError and disposes the upstream', async () => {
    let disposed = false;
    const src: Source<number | null> = {
      connect(sink) {
        return {
          resume() {
            if (disposed) return;
            sink.next(1);
            sink.next(null);
          },
          [Symbol.dispose]() {
            disposed = true;
          },
        };
      },
    };
    const readable = sourceToReadable(src);
    const collected: unknown[] = [];
    let caught: unknown;
    try {
      for await (const chunk of readable) collected.push(chunk);
    } catch (err) {
      caught = err;
    }
    expect(collected).toEqual([1]);
    expect(caught).toBeInstanceOf(TypeError);
    expect((caught as Error).message).toMatch(/null/);
    expect(disposed).toBe(true);
    expect(readable.destroyed).toBe(true);
  });

  it('does not loop forever on a source whose resume() is a no-op while active (fromSignal)', async () => {
    const sig = createSignal(0);
    const readable = sourceToReadable(fromSignal(sig));
    const collected: number[] = [];
    readable.on('data', (v: number) => collected.push(v));
    await tick();
    sig.set(1);
    sig.set(2);
    await tick();
    expect(collected).toEqual([0, 1, 2]);
    readable.destroy();
    await tick();
    expect(sig.observed).toBe(false);
    sig.set(3);
    expect(collected).toEqual([0, 1, 2]);
  });

  it('source error destroys the readable with that error', async () => {
    const src: Source<number> = {
      connect(sink) {
        return {
          resume() {
            sink.next(1);
            sink.error(new Error('upstream failed'));
          },
          [Symbol.dispose]() {},
        };
      },
    };
    const readable = sourceToReadable(src);
    await expect(collectReadable(readable)).rejects.toThrow('upstream failed');
  });

  it('applies backpressure: the source is paused when the buffer is full', async () => {
    let resumes = 0;
    let index = 0;
    const src: Source<number> = {
      connect(sink) {
        return {
          resume() {
            resumes++;
            while (index < 100) {
              if (sink.next(index++) === PAUSE) return;
            }
            sink.complete();
          },
          [Symbol.dispose]() {},
        };
      },
    };
    const readable = sourceToReadable(src);
    const values = await collectReadable<number>(readable);
    expect(values).toEqual(Array.from({ length: 100 }, (_, i) => i));
    expect(resumes).toBeGreaterThan(1); // it did pause along the way
  });
});

describe('toReadable (operator connection)', () => {
  it('starts once: repeated resume() emits a single Readable', async () => {
    const sink = testSink<Readable>();
    const conn = pipe(fromArray([1, 2]), toReadable(), assertProtocol()).connect(sink);
    conn.resume();
    conn.resume();
    conn.resume();
    expect(sink.values).toHaveLength(1);
    expect(sink.completeCount).toBe(1);
    expect(await collectReadable<number>(sink.values[0])).toEqual([1, 2]);
  });

  it('dispose destroys the Readable and with it the upstream connection', async () => {
    let disposed = false;
    const src: Source<number> = {
      connect() {
        return {
          resume() {},
          [Symbol.dispose]() {
            disposed = true;
          },
        };
      },
    };
    const sink = testSink<Readable>();
    const conn = pipe(src, toReadable()).connect(sink);
    conn.resume();
    const readable = sink.values[0];
    readable.on('error', () => {});
    conn[Symbol.dispose]();
    expect(readable.destroyed).toBe(true);
    expect(disposed).toBe(true);
    conn[Symbol.dispose](); // idempotent
  });

  it('dispose before resume: nothing is created', () => {
    let connected = false;
    const src: Source<number> = {
      connect() {
        connected = true;
        return { resume() {}, [Symbol.dispose]() {} };
      },
    };
    const sink = testSink<Readable>();
    const conn = pipe(src, toReadable()).connect(sink);
    conn[Symbol.dispose]();
    conn.resume();
    expect(connected).toBe(false);
    expect(sink.values).toHaveLength(0);
  });
});
