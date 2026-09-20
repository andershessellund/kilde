// ---------------------------------------------------------------------------
// take — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source, Sink } from '../types.js';
import { pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { take } from './take.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('take (exhaustive)', () => {
  it('take 3 from 5 — all pause orderings', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3, 4, 5], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, take(3), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('take 1 — early termination', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([10, 20, 30], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, take(1), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([10]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('take 0 — immediate complete', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, take(0), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('take more than available', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, take(10), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('take all — same as passthrough', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, take(3), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
    });
  });
});

describe('take (bug 7)', () => {
  function spySource<T>(values: T[]): Source<T> & { connects: number; disposes: number } {
    const spy = {
      connects: 0,
      disposes: 0,
      connect(sink: Sink<T>) {
        spy.connects++;
        const s = fromArray(values).connect(sink);
        return {
          resume: () => s.resume(),
          [Symbol.dispose]: () => {
            spy.disposes++;
            s[Symbol.dispose]();
          },
        };
      },
    };
    return spy;
  }

  it('take(0) never connects upstream and completes exactly once', () => {
    const src = spySource([1, 2, 3]);
    const sink = testSink<number>();
    const s = pipe(src, take(0)).connect(sink);
    expect(src.connects).toBe(0);
    s.resume();
    expect(sink.completeCount).toBe(1);
    // A second (and third) resume() must not complete again — testSink
    // throws on a second terminal event.
    s.resume();
    s.resume();
    expect(sink.completeCount).toBe(1);
    expect(src.connects).toBe(0);
  });

  it('take(n) disposes the upstream and completes once; resume() afterwards is a no-op', () => {
    const src = spySource([1, 2, 3]);
    const sink = testSink<number>();
    const s = pipe(src, take(2)).connect(sink);
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    expect(sink.completeCount).toBe(1);
    expect(src.disposes).toBe(1);
    s.resume();
    expect(sink.completeCount).toBe(1);
  });

  it('take(n) completes once when the downstream pauses on the nth value', () => {
    const sink = testSink<number>({ oracle: { integer: () => 1 } }); // always PAUSE
    const s = pipe(fromArray([1, 2, 3]), take(1), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(1);
    s.resume();
    expect(sink.completeCount).toBe(1);
  });
});
