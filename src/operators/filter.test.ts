// ---------------------------------------------------------------------------
// filter — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe } from '../stream.js';
import { filter } from './filter.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('filter (exhaustive)', () => {
  it('even numbers — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3, 4, 5, 6], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        filter((x) => x % 2 === 0),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([2, 4, 6]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('all pass — identity', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        filter(() => true),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('none pass — empty output', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        filter(() => false),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('error in predicate — reports error', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        filter((x) => {
          if (x === 2) throw new Error('pred');
          return true;
        }),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1]);
      expect(sink.errors).toHaveLength(1);
    });
  });
});
