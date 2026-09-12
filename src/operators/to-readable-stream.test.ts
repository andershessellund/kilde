// ---------------------------------------------------------------------------
// toReadableStream — tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source, Sink } from '../types.js';
import { stream, pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { createRelay } from '../relay.js';
import { map } from './map.js';
import { toReadableStream } from './to-readable-stream.js';
import { testSink } from '../testing/test-sink.js';
import { assertProtocol } from '../testing/protocol.js';

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

const tick = () => new Promise((r) => setTimeout(r, 0));

function tracked<T>(
  source: Source<T>,
): Source<T> & { connects: number; disposes: number; resumes: number; pauses: number } {
  const t = {
    connects: 0,
    disposes: 0,
    resumes: 0,
    pauses: 0,
    connect(sink: Sink<T>) {
      t.connects++;
      const s = source.connect({
        next(v: T) {
          const r = sink.next(v);
          if (r !== undefined) t.pauses++;
          return r;
        },
        complete: () => sink.complete(),
        error: (e: unknown) => sink.error(e),
      });
      return {
        resume: () => {
          t.resumes++;
          s.resume();
        },
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
    const src = tracked(fromArray([1, 2, 3, 4, 5]));
    const rs = stream(src, toReadableStream());
    const reader = rs.getReader();
    await reader.read(); // read first value
    await reader.cancel();

    expect(src.disposes).toBe(1);
  });

  it('handles many values with natural backpressure', async () => {
    const values = Array.from({ length: 100 }, (_, i) => i);
    const rs = stream(fromArray(values), toReadableStream());
    const result = await collectReadableStream(rs);
    expect(result).toEqual(values);
  });

  it('connects the upstream once and resumes it only while paused', async () => {
    const src = tracked(fromArray([1, 2, 3]));
    const rs = stream(src, toReadableStream());
    expect(src.connects).toBe(0); // lazy: nothing until the first pull
    const values = await collectReadableStream(rs);
    expect(values).toEqual([1, 2, 3]);
    expect(src.connects).toBe(1);
    // Backpressure engaged, and the upstream was never resumed while it
    // was still running (one resume to start, then one per PAUSE).
    expect(src.pauses).toBeGreaterThan(0);
    expect(src.resumes).toBeLessThanOrEqual(src.pauses + 1);
  });

  it('relay source: values pushed asynchronously are read in order', async () => {
    const relay = createRelay<number>();
    const rs = stream(relay, toReadableStream());
    const reader = rs.getReader();
    const first = reader.read();
    await tick(); // the adapter connects lazily, on the first pull
    relay.next(1);
    expect(await first).toEqual({ value: 1, done: false });
    relay.next(2);
    relay.next(3);
    relay.complete(); // arrives while the adapter is paused — queued chunks survive
    expect(await reader.read()).toEqual({ value: 2, done: false });
    expect(await reader.read()).toEqual({ value: 3, done: false });
    expect(await reader.read()).toEqual({ value: undefined, done: true });
  });

  it('an error arriving while chunks are queued surfaces after they are read', async () => {
    const src: Source<number> = {
      connect(sink: Sink<number>) {
        return {
          resume() {
            sink.next(1); // fills the queue (hwm 1) → PAUSE
            sink.error(new Error('late')); // arrives while paused
          },
          [Symbol.dispose]() {},
        };
      },
    };
    const rs = stream(src, toReadableStream());
    const reader = rs.getReader();
    expect(await reader.read()).toEqual({ value: 1, done: false });
    await expect(reader.read()).rejects.toThrow('late');
  });

  it('bug 12: a second resume() emits only one ReadableStream and one complete()', () => {
    const sink = testSink<ReadableStream<number>>();
    const s = pipe(fromArray([1]), toReadableStream(), assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    expect(sink.values).toHaveLength(1);
    expect(sink.completeCount).toBe(1);
  });
});
