// ---------------------------------------------------------------------------
// fromReadable — tests (Node.js Readable → kilde Source)
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { Readable } from 'node:stream';
import { fromReadable } from './from-readable.js';
import { pipe } from '../stream.js';
import { lines } from '../operators/lines.js';
import { toArray } from '../operators/to-array.js';
import { toPromise } from '../operators/to-promise.js';
import { stream } from '../stream.js';
import { PAUSE } from '../types.js';
import type { Sink, PAUSE as PAUSETYPE } from '../types.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Create a Node Readable from an array of values (object mode). */
function readableFrom<T>(values: T[]): Readable {
  let index = 0;
  return new Readable({
    objectMode: true,
    read() {
      if (index < values.length) {
        this.push(values[index++]);
      } else {
        this.push(null);
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

describe('fromReadable', () => {
  it('reads all values from a Node Readable', async () => {
    const readable = readableFrom([1, 2, 3]);
    const src = fromReadable<number>(readable);
    const sink = collectSink<number>();
    const s = src.connect(sink);
    s.resume();

    await new Promise((r) => setTimeout(r, 50));
    expect(sink.values).toEqual([1, 2, 3]);
    expect(sink.completed).toBe(true);
  });

  it('respects PAUSE and resumes correctly', async () => {
    const readable = readableFrom([10, 20, 30, 40, 50]);
    const src = fromReadable<number>(readable);

    const values: number[] = [];
    let completed = false;

    const sink: Sink<number> = {
      next(value: number): undefined | PAUSETYPE {
        values.push(value);
        // Pause after every value
        return PAUSE;
      },
      complete() {
        completed = true;
      },
      error() {},
    };

    const s = src.connect(sink);

    // Each resume should yield one value then PAUSE
    for (let i = 0; i < 5; i++) {
      s.resume();
      await new Promise((r) => setTimeout(r, 20));
      expect(values.length).toBe(i + 1);
    }

    // One more resume to get the end
    s.resume();
    await new Promise((r) => setTimeout(r, 50));
    expect(values).toEqual([10, 20, 30, 40, 50]);
    expect(completed).toBe(true);
  });

  it('propagates errors from the Readable', async () => {
    let pushed = false;
    const readable = new Readable({
      objectMode: true,
      read() {
        if (!pushed) {
          pushed = true;
          this.push(1);
          process.nextTick(() => this.destroy(new Error('readable error')));
        }
        // After the first push, do nothing — wait for the destroy to fire
      },
    });

    const src = fromReadable<number>(readable);
    const sink = collectSink<number>();
    const s = src.connect(sink);
    s.resume();

    await new Promise((r) => setTimeout(r, 50));
    expect(sink.values).toEqual([1]);
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('readable error');
  });

  it('dispose destroys the readable', async () => {
    const readable = readableFrom([1, 2, 3]);
    const src = fromReadable<number>(readable);
    const sink = collectSink<number>();
    const s = src.connect(sink);
    s.resume();

    await new Promise((r) => setTimeout(r, 20));
    s[Symbol.dispose]();

    expect(readable.destroyed).toBe(true);
  });

  it('works with Buffer data and lines()', async () => {
    const readable = new Readable({
      read() {
        this.push(Buffer.from('hello\nworld\n'));
        this.push(null);
      },
    });

    const src = fromReadable<Buffer>(readable);
    const result = stream(pipe(src, lines(), toArray()), toPromise());
    const linesArr = await result;

    expect(linesArr).toEqual(['hello', 'world']);
  });

  it('empty Readable completes immediately', async () => {
    const readable = readableFrom<number>([]);
    const src = fromReadable<number>(readable);
    const sink = collectSink<number>();
    const s = src.connect(sink);
    s.resume();

    await new Promise((r) => setTimeout(r, 50));
    expect(sink.values).toEqual([]);
    expect(sink.completed).toBe(true);
  });

  it('string mode Readable', async () => {
    const readable = new Readable({
      encoding: 'utf-8',
      read() {
        this.push('hello ');
        this.push('world');
        this.push(null);
      },
    });

    const src = fromReadable<string>(readable);
    const sink = collectSink<string>();
    const s = src.connect(sink);
    s.resume();

    await new Promise((r) => setTimeout(r, 50));
    expect(sink.values).toEqual(['hello ', 'world']);
    expect(sink.completed).toBe(true);
  });
});
