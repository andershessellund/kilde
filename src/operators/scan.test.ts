// ---------------------------------------------------------------------------
// scan — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Sink } from '../types.js';
import { pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { scan } from './scan.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('scan (exhaustive)', () => {
  it('running sum — all pause orderings', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3, 4], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        scan((acc, x) => acc + x, 0),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 3, 6, 10]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('running product', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([2, 3, 4], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        scan((acc, x) => acc * x, 1),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([2, 6, 24]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('single value', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([5], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        scan((acc, x) => acc + x, 10),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([15]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty source — no emissions', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource<number>([], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        scan((acc, x) => acc + x, 0),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('error in fn — reports error', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        scan((acc, x) => {
          if (x === 3) throw new Error('scan-fail');
          return acc + x;
        }, 0),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink, 10);
      expect(sink.values).toEqual([1, 3]);
      expect(sink.errors).toHaveLength(1);
      s.resume(); // no-op after terminal
    });
  });
});

describe('scan (protocol)', () => {
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
    const s = pipe(fromArray([1, 2]), scan((a, b) => a + b, 0)).connect(throwing);
    expect(() => s.resume()).toThrow('sink boom');
    expect(errors).toEqual([]);
  });
});
