// ---------------------------------------------------------------------------
// map — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe } from '../stream.js';
import { map } from './map.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('map (exhaustive)', () => {
  it('double — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        map((x) => x * 2),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([2, 4, 6]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('identity — preserves values', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([10, 20, 30, 40], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        map((x) => x),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([10, 20, 30, 40]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('type change — number to string', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(
        src,
        map((x) => `v${x}`),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['v1', 'v2', 'v3']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('error in fn — reports error on all orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        map((x) => {
          if (x === 2) throw new Error('bad');
          return x;
        }),
      ).connect(sink);
      drive(s, sink);
      // Got value 1 then error on 2
      expect(sink.values).toEqual([1]);
      expect(sink.errors).toHaveLength(1);
      expect((sink.errors[0] as Error).message).toBe('bad');
    });
  });
});
