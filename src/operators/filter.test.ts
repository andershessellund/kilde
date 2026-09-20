// ---------------------------------------------------------------------------
// filter — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Sink } from '../types.js';
import { pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { filter } from './filter.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('filter (exhaustive)', () => {
  it('even numbers — all pause orderings', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3, 4, 5, 6], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        filter((x) => x % 2 === 0),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([2, 4, 6]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('all pass — identity', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        filter(() => true),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('none pass — empty output', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        filter(() => false),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('error in predicate — reports error', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        filter((x) => {
          if (x === 2) throw new Error('pred-fail');
          return true;
        }),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink, 10);
      expect(sink.values).toEqual([1]);
      expect(sink.errors).toHaveLength(1);
      expect((sink.errors[0] as Error).message).toBe('pred-fail');
      s.resume(); // no-op after terminal
    });
  });
});

describe('filter (protocol)', () => {
  it('bug 8: a throwing downstream sink propagates to the producer', () => {
    const errors: unknown[] = [];
    const throwing: Sink<number> = {
      next() {
        throw new Error('sink boom');
      },
      complete() {},
      error(err) {
        errors.push(err);
      },
    };
    const s = pipe(fromArray([1, 2]), filter(() => true)).connect(throwing);
    expect(() => s.resume()).toThrow('sink boom');
    expect(errors).toEqual([]);
  });

  it('resume() after completion is a no-op', () => {
    const sink = testSink<number>();
    const s = pipe(fromArray([1, 2]), filter((x) => x > 1), assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    expect(sink.values).toEqual([2]);
    expect(sink.completeCount).toBe(1);
  });
});
