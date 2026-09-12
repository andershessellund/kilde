// ---------------------------------------------------------------------------
// fromPromise / fromAsyncFn — protocol tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { fromPromise, fromAsyncFn } from './from-promise.js';
import { pipe, stream } from '../stream.js';
import { toPromise } from '../operators/to-promise.js';
import { PAUSE } from '../types.js';
import type { Sink, PAUSE as PAUSETYPE } from '../types.js';
import { testSink } from '../testing/test-sink.js';
import { assertProtocol } from '../testing/protocol.js';

const tick = () => new Promise<void>((r) => setTimeout(r, 0));

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

describe('fromPromise', () => {
  it('delivers the value then completes (resume before settlement)', async () => {
    const sink = testSink<number>();
    const s = pipe(fromPromise(Promise.resolve(42)), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.values).toEqual([]);
    await tick();
    expect(sink.values).toEqual([42]);
    expect(sink.completeCount).toBe(1);
  });

  it('holds the settlement until the first resume()', async () => {
    const sink = testSink<number>();
    const s = pipe(fromPromise(Promise.resolve(42)), assertProtocol()).connect(sink);
    await tick();
    expect(sink.values).toEqual([]);
    expect(sink.completeCount).toBe(0);
    s.resume();
    expect(sink.values).toEqual([42]);
    expect(sink.completeCount).toBe(1);
  });

  it('resume() is idempotent — never re-delivers', async () => {
    const sink = testSink<number>();
    const s = pipe(fromPromise(Promise.resolve(1)), assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    await tick();
    s.resume();
    s.resume();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(1);
  });

  it('PAUSE on the value: completion arrives once, no later than the next resume()', async () => {
    // Settled before resume — value is drained on resume().
    const a = pausingSink<number>();
    const sa = pipe(fromPromise(Promise.resolve(1)), assertProtocol()).connect(a);
    await tick();
    sa.resume();
    expect(a.values).toEqual([1]);
    sa.resume();
    expect(a.completeCount).toBe(1);
    sa.resume();
    expect(a.values).toEqual([1]);
    expect(a.completeCount).toBe(1);

    // Settled after resume — value is pushed directly.
    const b = pausingSink<number>();
    const sb = pipe(fromPromise(Promise.resolve(2)), assertProtocol()).connect(b);
    sb.resume();
    await tick();
    expect(b.values).toEqual([2]);
    sb.resume();
    expect(b.completeCount).toBe(1);
    sb.resume();
    expect(b.values).toEqual([2]);
    expect(b.completeCount).toBe(1);
  });

  it('delivers a rejection as error()', async () => {
    const sink = testSink<number>();
    const s = pipe(fromPromise(Promise.reject(new Error('nope'))), assertProtocol()).connect(sink);
    s.resume();
    await tick();
    expect(sink.values).toEqual([]);
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('nope');
    s.resume(); // no-op after error
    expect(sink.errors).toHaveLength(1);
  });

  it('dispose before settlement: nothing is delivered', async () => {
    const sink = testSink<number>();
    const s = pipe(fromPromise(Promise.resolve(1)), assertProtocol()).connect(sink);
    s.resume();
    s[Symbol.dispose]();
    await tick();
    s.resume();
    expect(sink.values).toEqual([]);
    expect(sink.completeCount).toBe(0);
  });

  it('dispose after settlement but before resume: nothing is delivered', async () => {
    const sink = testSink<number>();
    const s = pipe(fromPromise(Promise.resolve(1)), assertProtocol()).connect(sink);
    await tick();
    s[Symbol.dispose]();
    s.resume();
    expect(sink.values).toEqual([]);
  });

  it('each connect() shares the same settlement', async () => {
    const src = fromPromise(Promise.resolve('x'));
    const a = testSink<string>();
    const b = testSink<string>();
    src.connect(a).resume();
    src.connect(b).resume();
    await tick();
    expect(a.values).toEqual(['x']);
    expect(b.values).toEqual(['x']);
  });

  it('works with toPromise()', async () => {
    await expect(stream(fromPromise(Promise.resolve(7)), toPromise())).resolves.toBe(7);
  });

  it('accepts a bare thenable', async () => {
    const thenable: PromiseLike<number> = {
      then(onFulfilled) {
        queueMicrotask(() => onFulfilled?.(9));
        return undefined as any;
      },
    };
    const sink = testSink<number>();
    pipe(fromPromise(thenable), assertProtocol()).connect(sink).resume();
    await tick();
    expect(sink.values).toEqual([9]);
    expect(sink.completeCount).toBe(1);
  });
});

describe('fromAsyncFn', () => {
  it('calls fn once per connect()', async () => {
    let calls = 0;
    const src = fromAsyncFn(async () => ++calls);
    const a = testSink<number>();
    const b = testSink<number>();
    src.connect(a).resume();
    src.connect(b).resume();
    await tick();
    expect(calls).toBe(2);
    expect(a.values).toEqual([1]);
    expect(b.values).toEqual([2]);
    expect(a.completeCount).toBe(1);
    expect(b.completeCount).toBe(1);
  });

  it('a synchronous throw from fn is delivered as error()', async () => {
    const src = fromAsyncFn<number>(() => {
      throw new Error('sync boom');
    });
    const sink = testSink<number>();
    pipe(src, assertProtocol()).connect(sink).resume();
    await tick();
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('sync boom');
  });

  it('resume() is idempotent and PAUSE is honoured', async () => {
    const sink = pausingSink<number>();
    const s = pipe(
      fromAsyncFn(() => Promise.resolve(3)),
      assertProtocol(),
    ).connect(sink);
    s.resume();
    s.resume();
    await tick();
    expect(sink.values).toEqual([3]);
    s.resume();
    s.resume();
    expect(sink.values).toEqual([3]);
    expect(sink.completeCount).toBe(1);
  });
});
