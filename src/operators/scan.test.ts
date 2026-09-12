// ---------------------------------------------------------------------------
// scan — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe } from '../stream.js';
import { scan } from './scan.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';

const add = (a: number, b: number) => a + b;

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('scan (exhaustive)', () => {
  it('running sum — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, scan(add, 0)).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 3, 6]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('running product', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([2, 3, 4], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        scan((a, b) => a * b, 1),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([2, 6, 24]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('single value', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([5], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, scan(add, 10)).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([15]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty source — no emissions', () => {
    exhaustiveTest((oracle) => {
      const src = testSource<number>([], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, scan(add, 0)).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });
});
