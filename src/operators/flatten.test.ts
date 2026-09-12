// ---------------------------------------------------------------------------
// flatten — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source } from '../types.js';
import { pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { empty } from '../sources/empty.js';
import { flatten } from './flatten.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('flatten (exhaustive)', () => {
  it('two inner arrays — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([fromArray([1, 2]), fromArray([3, 4])], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3, 4]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('three inner arrays', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([fromArray([1]), fromArray([2, 3]), fromArray([4])], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3, 4]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty inner first', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([empty<number>(), fromArray([1, 2])], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty inner last', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([fromArray([1, 2]), empty<number>()], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('all empty inners', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([empty<number>(), empty<number>(), empty<number>()], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('single inner with many values', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([fromArray([1, 2, 3, 4, 5])], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3, 4, 5]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty outer', () => {
    exhaustiveTest((oracle) => {
      const src = testSource<Source<number>>([], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });
});
