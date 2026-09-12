// ---------------------------------------------------------------------------
// take — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe } from '../stream.js';
import { take } from './take.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('take (exhaustive)', () => {
  it('take 3 from 5 — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3, 4, 5], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, take(3)).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('take 1 — early termination', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([10, 20, 30], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, take(1)).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([10]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('take 0 — immediate complete', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, take(0)).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('take more than available', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, take(10)).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('take all — same as passthrough', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, take(3)).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
    });
  });
});
