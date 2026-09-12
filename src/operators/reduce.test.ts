// ---------------------------------------------------------------------------
// reduce — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe, stream } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { reduce } from './reduce.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('reduce (exhaustive)', () => {
  it('sum — all source pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3, 4], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        reduce((acc, x) => acc + x, 0),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([10]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('product', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([2, 3, 4], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        reduce((acc, x) => acc * x, 1),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([24]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('single value', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([7], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        reduce((acc, x) => acc + x, 0),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([7]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty source — emits initial', () => {
    exhaustiveTest((oracle) => {
      const src = testSource<number>([], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        reduce((acc, x) => acc + x, 42),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([42]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('string concatenation', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(['a', 'b', 'c'], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(
        src,
        reduce((acc, x) => acc + x, ''),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['abc']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('error in fn — reports error, no result', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        reduce((acc, x) => {
          if (x === 2) throw new Error('reduce-fail');
          return acc + x;
        }, 0),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink, 10);
      expect(sink.values).toEqual([]);
      expect(sink.errors).toHaveLength(1);
    });
  });
});

describe('reduce (collector)', () => {
  it('works as the last argument to stream()', () => {
    expect(stream(fromArray([1, 2, 3]), reduce((a, b) => a + b, 0))).toBe(6);
  });

  it('completes exactly once even when the downstream pauses on the result', () => {
    const sink = testSink<number>({ oracle: { integer: () => 1 } }); // always PAUSE
    const s = pipe(fromArray([1, 2]), reduce((a, b) => a + b, 0), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.values).toEqual([3]);
    expect(sink.completeCount).toBe(1);
    s.resume();
    expect(sink.completeCount).toBe(1);
  });
});
