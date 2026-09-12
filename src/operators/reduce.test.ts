// ---------------------------------------------------------------------------
// reduce — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe } from '../stream.js';
import { reduce } from './reduce.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';

const add = (a: number, b: number) => a + b;

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('reduce (exhaustive)', () => {
  it('sum — all source pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, reduce(add, 0)).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([6]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('product', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([2, 3, 4], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        reduce((a, b) => a * b, 1),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([24]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('single value', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([42], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, reduce(add, 0)).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([42]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty source — emits initial', () => {
    exhaustiveTest((oracle) => {
      const src = testSource<number>([], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, reduce(add, 0)).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([0]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('string concatenation', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(['a', 'b', 'c'], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(
        src,
        reduce((a, b) => a + b, ''),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['abc']);
      expect(sink.completeCount).toBe(1);
    });
  });
});
