// ---------------------------------------------------------------------------
// deferred — protocol tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { deferred } from './deferred.js';
import { pipe } from '../stream.js';
import { PAUSE } from '../types.js';
import type { Sink, PAUSE as PAUSETYPE } from '../types.js';
import { testSink } from '../testing/test-sink.js';
import { assertProtocol } from '../testing/protocol.js';

/** A sink that pauses on every value. */
function pausingSink<T>(): Sink<T> & { values: T[]; completeCount: number } {
  const sink = {
    values: [] as T[],
    completeCount: 0,
    next(v: T): undefined | PAUSETYPE {
      sink.values.push(v);
      return PAUSE;
    },
    complete() {
      sink.completeCount++;
    },
    error() {},
  };
  return sink;
}

describe('deferred', () => {
  it('resolve after resume delivers value then complete', () => {
    const d = deferred<number>();
    const sink = testSink<number>();
    const s = pipe(d, assertProtocol()).connect(sink);
    s.resume();
    expect(sink.values).toEqual([]);
    d.resolve(42);
    expect(sink.values).toEqual([42]);
    expect(sink.completeCount).toBe(1);
  });

  it('resolve before resume: delivered on the first resume', () => {
    const d = deferred<number>();
    const sink = testSink<number>();
    const s = pipe(d, assertProtocol()).connect(sink);
    d.resolve(1);
    expect(sink.values).toEqual([]);
    s.resume();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(1);
  });

  it('resolve before connect: delivered on the first resume', () => {
    const d = deferred<number>();
    d.resolve(1);
    const sink = testSink<number>();
    const s = pipe(d, assertProtocol()).connect(sink);
    expect(sink.values).toEqual([]);
    s.resume();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(1);
  });

  it('resume() is idempotent — never re-delivers', () => {
    const d = deferred<number>();
    const sink = testSink<number>();
    const s = pipe(d, assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    d.resolve(1);
    s.resume();
    s.resume();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(1);
  });

  it('PAUSE on the value: complete arrives once, no later than the next resume()', () => {
    const a = pausingSink<number>();
    const da = deferred<number>();
    const sa = pipe(da, assertProtocol()).connect(a);
    sa.resume();
    da.resolve(1); // pushed directly
    expect(a.values).toEqual([1]);
    sa.resume();
    expect(a.completeCount).toBe(1);
    sa.resume();
    expect(a.values).toEqual([1]);
    expect(a.completeCount).toBe(1);

    const b = pausingSink<number>();
    const db = deferred<number>();
    db.resolve(2); // queued
    const sb = pipe(db, assertProtocol()).connect(b);
    sb.resume();
    expect(b.values).toEqual([2]);
    sb.resume();
    expect(b.completeCount).toBe(1);
    sb.resume();
    expect(b.values).toEqual([2]);
    expect(b.completeCount).toBe(1);
  });

  it('reject delivers error() and rejects the promise', async () => {
    const d = deferred<number>();
    const sink = testSink<number>();
    const s = pipe(d, assertProtocol()).connect(sink);
    s.resume();
    d.reject(new Error('bad'));
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('bad');
    await expect(d.promise).rejects.toThrow('bad');
    s.resume(); // no-op
    expect(sink.errors).toHaveLength(1);
  });

  it('promise resolves with the value', async () => {
    const d = deferred<string>();
    d.resolve('ok');
    await expect(d.promise).resolves.toBe('ok');
  });

  it('only the first settlement counts', async () => {
    const d = deferred<number>();
    const sink = testSink<number>();
    pipe(d, assertProtocol()).connect(sink).resume();
    d.resolve(1);
    d.resolve(2);
    d.reject(new Error('late'));
    expect(sink.values).toEqual([1]);
    expect(sink.errors).toEqual([]);
    await expect(d.promise).resolves.toBe(1);
  });

  it('multiple subscribers each get the value', () => {
    const d = deferred<number>();
    const a = testSink<number>();
    const b = testSink<number>();
    d.connect(a).resume();
    d.connect(b).resume();
    d.resolve(5);
    expect(a.values).toEqual([5]);
    expect(b.values).toEqual([5]);
    expect(a.completeCount).toBe(1);
    expect(b.completeCount).toBe(1);
  });

  it('dispose before resolution: that sink never hears anything', () => {
    const d = deferred<number>();
    const a = testSink<number>();
    const b = testSink<number>();
    const sa = pipe(d, assertProtocol()).connect(a);
    d.connect(b).resume();
    sa.resume();
    sa[Symbol.dispose]();
    d.resolve(1);
    sa.resume();
    expect(a.values).toEqual([]);
    expect(a.completeCount).toBe(0);
    expect(b.values).toEqual([1]);
  });

  it('dispose without ever resuming does not leak a waiting stream', () => {
    const d = deferred<number>();
    const sink = testSink<number>();
    const s = d.connect(sink);
    s[Symbol.dispose]();
    d.resolve(1);
    expect(sink.values).toEqual([]);
  });

  it('a throwing sink does not prevent the others from being notified', () => {
    const d = deferred<number>();
    const good1 = testSink<number>();
    const good2 = testSink<number>();
    const bad: Sink<number> = {
      next() {
        throw new Error('sink threw');
      },
      complete() {},
      error() {},
    };
    d.connect(good1).resume();
    d.connect(bad).resume();
    d.connect(good2).resume();

    expect(() => d.resolve(7)).toThrow('sink threw');
    expect(good1.values).toEqual([7]);
    expect(good2.values).toEqual([7]);
    expect(good1.completeCount).toBe(1);
    expect(good2.completeCount).toBe(1);
  });

  it('several throwing sinks are reported as one AggregateError', () => {
    const d = deferred<number>();
    const thrower = (msg: string): Sink<number> => ({
      next() {
        throw new Error(msg);
      },
      complete() {},
      error() {},
    });
    const good = testSink<number>();
    d.connect(thrower('one')).resume();
    d.connect(good).resume();
    d.connect(thrower('two')).resume();

    let caught: unknown;
    try {
      d.resolve(1);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(AggregateError);
    expect((caught as AggregateError).errors.map((e) => (e as Error).message)).toEqual([
      'one',
      'two',
    ]);
    expect(good.values).toEqual([1]);
  });

  it('a throwing sink in reject() does not prevent the others from being notified', () => {
    const d = deferred<number>();
    const good = testSink<number>();
    const bad: Sink<number> = {
      next: () => undefined,
      complete() {},
      error() {
        throw new Error('error handler threw');
      },
    };
    d.connect(bad).resume();
    d.connect(good).resume();
    d.promise.catch(() => {});
    expect(() => d.reject(new Error('x'))).toThrow('error handler threw');
    expect(good.errors).toHaveLength(1);
  });
});
