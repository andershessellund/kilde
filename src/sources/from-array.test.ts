// ---------------------------------------------------------------------------
// fromArray / of — exhaustive protocol tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { fromArray, of } from './from-array.js';
import { pipe, stream } from '../stream.js';
import { toArray } from '../operators/to-array.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

function drive(s: { resume(): void }, sink: { terminated: boolean }, max = 40) {
  for (let i = 0; i < max && !sink.terminated; i++) s.resume();
}

describe('fromArray', () => {
  it('emits all values and completes', () => {
    expect(stream(fromArray([1, 2, 3]), toArray())).toEqual([1, 2, 3]);
    expect(stream(of('a', 'b'), toArray())).toEqual(['a', 'b']);
    expect(stream(fromArray<number>([]), toArray())).toEqual([]);
  });

  it('accepts any ArrayLike', () => {
    expect(stream(fromArray('abc'), toArray())).toEqual(['a', 'b', 'c']);
    expect(stream(fromArray({ length: 2, 0: 'x', 1: 'y' }), toArray())).toEqual(['x', 'y']);
  });

  it('respects PAUSE and completes exactly once under every pause pattern', async () => {
    const runs = await exhaustiveTest((oracle) => {
      const sink = testSink<number>({ oracle });
      const s = pipe(fromArray([1, 2, 3]), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
      // Extra resume() calls after completion are no-ops.
      s.resume();
      s.resume();
      expect(sink.completeCount).toBe(1);
    });
    expect(runs.runs).toBe(8);
  });

  it('resume() after completion of an empty array does not re-complete', () => {
    const sink = testSink<number>();
    const s = pipe(fromArray<number>([]), assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    expect(sink.completeCount).toBe(1);
  });

  it('dispose stops delivery mid-array', () => {
    const values: number[] = [];
    let completed = false;
    const s: { resume(): void; [Symbol.dispose](): void } = fromArray([1, 2, 3]).connect({
      next(v: number) {
        values.push(v);
        if (v === 2) s[Symbol.dispose]();
        return undefined;
      },
      complete() {
        completed = true;
      },
      error() {},
    });
    s.resume();
    expect(values).toEqual([1, 2]);
    expect(completed).toBe(false);
    s.resume(); // no-op after dispose
    expect(values).toEqual([1, 2]);
  });

  it('each connect() is independent', () => {
    const src = fromArray([1, 2]);
    const a = testSink<number>();
    const b = testSink<number>();
    src.connect(a).resume();
    src.connect(b).resume();
    expect(a.values).toEqual([1, 2]);
    expect(b.values).toEqual([1, 2]);
  });
});

describe('fromArray re-entrant resume', () => {
  it('resume() called from inside next() is a no-op and completion happens once', () => {
    const got: number[] = [];
    let completes = 0;
    const handle: { stream?: { resume(): void } } = {};
    handle.stream = fromArray([1, 2, 3]).connect({
      next(v) { got.push(v); handle.stream?.resume(); return undefined; },
      complete() { completes++; },
      error() {},
    });
    handle.stream.resume();
    expect(got).toEqual([1, 2, 3]);
    expect(completes).toBe(1);
  });
});
