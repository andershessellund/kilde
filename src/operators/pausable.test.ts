// ---------------------------------------------------------------------------
// pausable — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe } from '../stream.js';
import { pausable } from './pausable.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('pausable (exhaustive)', () => {
  it('three values — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, pausable()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('five values — buffer drain orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3, 4, 5], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, pausable()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3, 4, 5]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('single value', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([42], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, pausable()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([42]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty source', () => {
    exhaustiveTest((oracle) => {
      const src = testSource<number>([], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, pausable()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });
});
