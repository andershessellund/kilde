// ---------------------------------------------------------------------------
// connect.test.ts — Tests for tracked stream connections
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source, Sink } from './types.js';
import type { Stream } from './types.js';
import { connect, StreamDisposedError } from './connect.js';
import { fromArray } from './sources/from-array.js';
import { createOwner, withOwner } from './owner.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

interface LiveSource<T> extends Source<T> {
  push(value: T): void;
  complete(): void;
  error(err: unknown): void;
}

function createLiveSource<T>(): LiveSource<T> {
  let sink: Sink<T> | null = null;
  let resumed = false;

  return {
    connect(s: Sink<T>): Stream {
      sink = s;
      return {
        resume() { resumed = true; },
        [Symbol.dispose]() { sink = null; resumed = false; },
      };
    },
    push(value: T) { if (sink && resumed) sink.next(value); },
    complete() { if (sink && resumed) sink.complete(); },
    error(err: unknown) { if (sink && resumed) sink.error(err); },
  };
}

function collectSink<T>() {
  const values: T[] = [];
  let completed = false;
  let error: unknown = null;
  const sink: Sink<T> = {
    next(v) { values.push(v); return undefined; },
    complete() { completed = true; },
    error(err) { error = err; },
  };
  return {
    sink,
    values,
    isCompleted() { return completed; },
    getError() { return error; },
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('connect()', () => {
  it('works without an owner (untracked)', () => {
    const c = collectSink<number>();
    const conn = connect(fromArray([1, 2, 3]), c.sink);
    conn.resume();

    expect(c.values).toEqual([1, 2, 3]);
    expect(c.isCompleted()).toBe(true);
  });

  it('passes values and completion through normally', async () => {
    const owner = createOwner('test-connect');

    withOwner(owner, () => {
      const c = collectSink<number>();
      const conn = connect(fromArray([1, 2, 3]), c.sink);
      conn.resume();

      expect(c.values).toEqual([1, 2, 3]);
      expect(c.isCompleted()).toBe(true);
    });

    await owner.dispose();
  });

  it('passes errors through normally', async () => {
    const owner = createOwner('test-connect-error');

    withOwner(owner, () => {
      const live = createLiveSource<number>();
      const c = collectSink<number>();
      const conn = connect(live, c.sink);
      conn.resume();

      live.push(1);
      live.error(new Error('boom'));

      expect(c.values).toEqual([1]);
      expect(c.getError()).toBeInstanceOf(Error);
      expect((c.getError() as Error).message).toBe('boom');
    });

    await owner.dispose();
  });

  it('errors the sink with StreamDisposedError when the owner is disposed', async () => {
    const owner = createOwner('test-dispose');
    const live = createLiveSource<number>();
    const c = collectSink<number>();
    let error: unknown = null;

    withOwner(owner, () => {
      connect(live, {
        next(v) { return c.sink.next(v); },
        complete() { c.sink.complete(); },
        error(err) { error = err; },
      }).resume();
    });

    live.push(42);
    expect(c.values).toEqual([42]);
    expect(owner.size).toBe(1);

    await owner.dispose();

    expect(error).toBeInstanceOf(StreamDisposedError);
    // The upstream was torn down: further pushes go nowhere
    live.push(43);
    expect(c.values).toEqual([42]);
  });

  it('accepts an explicit owner option', async () => {
    const owner = createOwner('explicit');
    const live = createLiveSource<number>();
    let error: unknown = null;

    connect(live, {
      next() { return undefined; },
      complete() {},
      error(err) { error = err; },
    }, { owner }).resume();

    expect(owner.size).toBe(1);
    await owner.dispose();
    expect(error).toBeInstanceOf(StreamDisposedError);
  });

  it('explicit owner wins over the ambient owner', () => {
    const ambient = createOwner('ambient');
    const explicit = createOwner('explicit');
    const live = createLiveSource<number>();

    withOwner(ambient, () => {
      connect(live, collectSink<number>().sink, { owner: explicit }).resume();
    });

    expect(ambient.size).toBe(0);
    expect(explicit.size).toBe(1);
  });

  it('explicitly disposing the connection errors an unsettled sink', async () => {
    const owner = createOwner('test-explicit-dispose');

    withOwner(owner, () => {
      const live = createLiveSource<number>();
      let error: unknown = null;

      const c = collectSink<number>();
      const conn = connect(live, {
        next(v) { return c.sink.next(v); },
        complete() { c.sink.complete(); },
        error(err) { error = err; },
      });
      conn.resume();

      live.push(1);

      // Explicitly dispose (simulates what owner disposal does)
      conn[Symbol.dispose]();

      expect(error).toBeInstanceOf(StreamDisposedError);
    });

    await owner.dispose();
  });

  it('does not error the sink if stream completed before disposal', async () => {
    const owner = createOwner('test-no-double-error');

    withOwner(owner, () => {
      const live = createLiveSource<number>();
      let errorCount = 0;

      const conn = connect(live, {
        next() { return undefined; },
        complete() {},
        error() { errorCount++; },
      });
      conn.resume();

      // Complete normally first
      live.complete();

      // Then dispose — should be a no-op on the sink
      conn[Symbol.dispose]();

      expect(errorCount).toBe(0);
    });

    await owner.dispose();
  });

  it('unregisters from the owner automatically on complete', async () => {
    const owner = createOwner('test-auto-unregister');
    let disposeCount = 0;

    withOwner(owner, () => {
      const live = createLiveSource<number>();
      const origConnect = live.connect.bind(live);

      // Wrap to count dispose calls
      live.connect = (sink: Sink<number>) => {
        const conn = origConnect(sink);
        return {
          resume() { conn.resume(); },
          [Symbol.dispose]() { disposeCount++; conn[Symbol.dispose](); },
        };
      };

      const c = collectSink<number>();
      const conn = connect(live, c.sink);
      conn.resume();

      live.push(1);
      live.complete(); // Should unregister the resource
    });

    expect(owner.size).toBe(0);
    await owner.dispose();

    // Owner disposal must not dispose the (already settled) connection again
    expect(disposeCount).toBe(0);
  });
});
