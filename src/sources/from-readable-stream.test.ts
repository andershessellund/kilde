// ---------------------------------------------------------------------------
// fromReadableStream — tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { fromReadableStream } from './from-readable-stream.js';
import { pipe } from '../stream.js';
import { toArray } from '../operators/to-array.js';
import { toPromise } from '../operators/to-promise.js';
import { lines } from '../operators/lines.js';
import { stream } from '../stream.js';
import { PAUSE } from '../types.js';
import type { Sink, PAUSE as PAUSETYPE } from '../types.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Create a ReadableStream from an array of values. */
function readableStreamFrom<T>(values: T[]): ReadableStream<T> {
  let index = 0;
  return new ReadableStream<T>({
    pull(controller) {
      if (index < values.length) {
        controller.enqueue(values[index++]);
      } else {
        controller.close();
      }
    },
  });
}

function collectSink<T>(): Sink<T> & { values: T[]; completed: boolean; errors: unknown[] } {
  const result = {
    values: [] as T[],
    completed: false,
    errors: [] as unknown[],
    next(value: T) {
      result.values.push(value);
      return undefined as undefined;
    },
    complete() {
      result.completed = true;
    },
    error(err: unknown) {
      result.errors.push(err);
    },
  };
  return result;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('fromReadableStream', () => {
  it('reads all values from a ReadableStream', async () => {
    const rs = readableStreamFrom([1, 2, 3]);
    const src = fromReadableStream(rs);
    const sink = collectSink<number>();
    const s = src.connect(sink);

    // The source is async — need to await values
    s.resume();
    // Wait for the async pull loop to complete
    await new Promise((r) => setTimeout(r, 50));

    expect(sink.values).toEqual([1, 2, 3]);
    expect(sink.completed).toBe(true);
  });

  it('respects PAUSE and resumes correctly', async () => {
    const rs = readableStreamFrom([10, 20, 30, 40]);
    const src = fromReadableStream(rs);

    let pauseCount = 0;
    const values: number[] = [];
    let completed = false;

    const sink: Sink<number> = {
      next(value: number): undefined | PAUSETYPE {
        values.push(value);
        if (values.length % 2 === 0) {
          pauseCount++;
          return PAUSE;
        }
        return undefined;
      },
      complete() {
        completed = true;
      },
      error() {},
    };

    const s = src.connect(sink);

    // First resume — should get 10 (no pause), 20 (pause)
    s.resume();
    await new Promise((r) => setTimeout(r, 50));
    expect(values).toEqual([10, 20]);
    expect(pauseCount).toBe(1);

    // Second resume — should get 30 (no pause), 40 (pause)
    s.resume();
    await new Promise((r) => setTimeout(r, 50));
    expect(values).toEqual([10, 20, 30, 40]);
    expect(pauseCount).toBe(2);

    // Third resume — source exhausted, should complete
    s.resume();
    await new Promise((r) => setTimeout(r, 50));
    expect(completed).toBe(true);
  });

  it('propagates errors from the ReadableStream', async () => {
    let pulls = 0;
    const rs = new ReadableStream<number>({
      pull(controller) {
        pulls++;
        if (pulls === 1) {
          controller.enqueue(1);
        } else {
          controller.error(new Error('stream error'));
        }
      },
    });

    const src = fromReadableStream(rs);
    const sink = collectSink<number>();
    const s = src.connect(sink);
    s.resume();

    await new Promise((r) => setTimeout(r, 50));
    expect(sink.values).toEqual([1]);
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('stream error');
  });

  it('dispose cancels the reader', async () => {
    let cancelled = false;
    let pullCount = 0;
    const rs = new ReadableStream<number>({
      async pull(controller) {
        pullCount++;
        if (pullCount === 1) {
          controller.enqueue(1);
        } else {
          // Simulate waiting for next chunk — hangs until cancelled
          await new Promise<void>(() => {});
        }
      },
      cancel() {
        cancelled = true;
      },
    });

    const src = fromReadableStream(rs);
    const sink = collectSink<number>();
    const s = src.connect(sink);

    s.resume();
    await new Promise((r) => setTimeout(r, 50));
    expect(sink.values).toEqual([1]);

    s[Symbol.dispose]();
    await new Promise((r) => setTimeout(r, 50));

    expect(cancelled).toBe(true);
  });

  it('works in a pipeline with lines()', async () => {
    const rs = readableStreamFrom(['hello\nworld\n', 'foo\nbar\n']);
    const src = fromReadableStream(rs);
    const result = stream(pipe(src, lines(), toArray()), toPromise());
    const linesArr = await result;

    expect(linesArr).toEqual(['hello', 'world', 'foo', 'bar']);
  });

  it('empty ReadableStream completes immediately', async () => {
    const rs = readableStreamFrom<string>([]);
    const src = fromReadableStream(rs);
    const sink = collectSink<string>();
    const s = src.connect(sink);
    s.resume();

    await new Promise((r) => setTimeout(r, 50));
    expect(sink.values).toEqual([]);
    expect(sink.completed).toBe(true);
  });
});
