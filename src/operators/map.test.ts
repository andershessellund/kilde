// ---------------------------------------------------------------------------
// map — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Sink } from '../types.js';
import { pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { map } from './map.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

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
        assertProtocol(),
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
        assertProtocol(),
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
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['v1', 'v2', 'v3']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty source', () => {
    exhaustiveTest((oracle) => {
      const src = testSource<number>([], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        map((x) => x),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('error in fn — reports error on all orderings, nothing afterwards', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        map((x) => {
          if (x === 2) throw new Error('bad');
          return x;
        }),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink, 10);
      // Got value 1 then error on 2
      expect(sink.values).toEqual([1]);
      expect(sink.errors).toHaveLength(1);
      expect((sink.errors[0] as Error).message).toBe('bad');
      // resume() after the terminal is a no-op (testSink throws otherwise)
      s.resume();
    });
  });
});

describe('map (protocol)', () => {
  it('resume() after completion is a no-op', () => {
    const sink = testSink<number>();
    const s = pipe(fromArray([1]), map((x) => x), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.completeCount).toBe(1);
    s.resume();
    s.resume();
    expect(sink.completeCount).toBe(1);
  });

  it('nothing is delivered after dispose', () => {
    let upstreamDisposed = false;
    const sink = testSink<number>();
    const s = pipe(
      {
        connect(inner: Sink<number>) {
          return {
            resume() {
              inner.next(1);
            },
            [Symbol.dispose]() {
              upstreamDisposed = true;
            },
          };
        },
      },
      map((x: number) => x),
    ).connect(sink);
    s.resume();
    s[Symbol.dispose]();
    expect(upstreamDisposed).toBe(true);
    // A misbehaving upstream after dispose is swallowed
    s.resume();
    expect(sink.values).toEqual([1]);
  });

  it('bug 8: a throwing downstream sink propagates to the producer, not into sink.error()', () => {
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
    const s = pipe(fromArray([1, 2]), map((x) => x * 2)).connect(throwing);
    expect(() => s.resume()).toThrow('sink boom');
    expect(errors).toEqual([]);
  });
});
