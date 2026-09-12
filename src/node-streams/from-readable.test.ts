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
import { PrematureCloseError } from './premature-close.js';
import { testSink } from '../testing/test-sink.js';
import { assertProtocol } from '../testing/protocol.js';

const tick = () => new Promise<void>((r) => setTimeout(r, 10));

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

  // -------------------------------------------------------------------------
  // Lifecycle edge cases
  // -------------------------------------------------------------------------

  it('attaches nothing before the first resume()', () => {
    const readable = readableFrom([1]);
    const sink = testSink<number>();
    pipe(fromReadable<number>(readable), assertProtocol()).connect(sink);
    expect(readable.listenerCount('data')).toBe(0);
    expect(readable.listenerCount('end')).toBe(0);
    expect(readable.readableFlowing).toBeNull();
  });

  it('premature destroy() (close before end) is an error', async () => {
    const readable = new Readable({ objectMode: true, read() {} });
    const sink = testSink<number>();
    const s = pipe(fromReadable<number>(readable), assertProtocol()).connect(sink);
    s.resume();
    readable.push(1);
    await tick();
    readable.destroy();
    await tick();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(0);
    expect(sink.errors).toHaveLength(1);
    expect(sink.errors[0]).toBeInstanceOf(PrematureCloseError);
    expect((sink.errors[0] as PrematureCloseError).code).toBe('ERR_STREAM_PREMATURE_CLOSE');
  });

  it('destroy(err) delivers that error, not a premature-close error', async () => {
    const readable = new Readable({ objectMode: true, read() {} });
    const sink = testSink<number>();
    const s = pipe(fromReadable<number>(readable), assertProtocol()).connect(sink);
    s.resume();
    readable.destroy(new Error('boom'));
    await tick();
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('boom');
  });

  it('connecting to an already-ended readable completes on resume', async () => {
    const readable = readableFrom([1, 2]);
    readable.resume(); // drain it
    await new Promise<void>((r) => readable.once('end', r));
    expect(readable.readableEnded).toBe(true);

    const sink = testSink<number>();
    const s = pipe(fromReadable<number>(readable), assertProtocol()).connect(sink);
    expect(sink.completeCount).toBe(0);
    s.resume();
    expect(sink.completeCount).toBe(1);
    s.resume();
    expect(sink.completeCount).toBe(1);
  });

  it('connecting to an already-destroyed readable errors on resume', () => {
    const readable = new Readable({ objectMode: true, read() {} });
    readable.destroy();
    const sink = testSink<number>();
    const s = pipe(fromReadable<number>(readable), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.errors).toHaveLength(1);
    expect(sink.errors[0]).toBeInstanceOf(PrematureCloseError);

    const errored = new Readable({ objectMode: true, read() {} });
    errored.on('error', () => {});
    errored.destroy(new Error('already broken'));
    const sink2 = testSink<number>();
    pipe(fromReadable<number>(errored), assertProtocol()).connect(sink2).resume();
    expect((sink2.errors[0] as Error).message).toBe('already broken');
  });

  it('a late error after end is not delivered and does not crash', async () => {
    const readable = readableFrom([1]);
    const sink = testSink<number>();
    const s = pipe(fromReadable<number>(readable), assertProtocol()).connect(sink);
    s.resume();
    await tick();
    expect(sink.completeCount).toBe(1);
    expect(readable.listenerCount('data')).toBe(0);
    expect(readable.listenerCount('end')).toBe(0);
    expect(readable.listenerCount('close')).toBe(0);
    // Would throw ERR_UNHANDLED_ERROR without a listener left behind.
    expect(() => readable.emit('error', new Error('late'))).not.toThrow();
    expect(sink.errors).toEqual([]);
  });

  it('repeated resume() while flowing does not duplicate values', async () => {
    const readable = readableFrom([1, 2, 3]);
    const sink = testSink<number>();
    const s = pipe(fromReadable<number>(readable), assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    await tick();
    s.resume();
    await tick();
    expect(sink.values).toEqual([1, 2, 3]);
    expect(sink.completeCount).toBe(1);
  });

  it('dispose removes all listeners and destroys the readable', async () => {
    const readable = readableFrom([1, 2, 3]);
    const sink = testSink<number>({ oracle: { integer: () => 1 } }); // always PAUSE
    const s = pipe(fromReadable<number>(readable), assertProtocol()).connect(sink);
    s.resume();
    await tick();
    expect(sink.values).toEqual([1]);
    s[Symbol.dispose]();
    expect(readable.listenerCount('data')).toBe(0);
    expect(readable.listenerCount('end')).toBe(0);
    expect(readable.listenerCount('close')).toBe(0);
    expect(readable.destroyed).toBe(true);
    await tick();
    s.resume();
    await tick();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(0);
    expect(sink.errors).toEqual([]); // the 'close' from our own destroy is not an error
  });

  it('dispose before resume never touches the readable', () => {
    const readable = readableFrom([1]);
    const s = fromReadable<number>(readable).connect(testSink<number>());
    s[Symbol.dispose]();
    expect(readable.destroyed).toBe(true);
    expect(readable.listenerCount('data')).toBe(0);
  });
});
