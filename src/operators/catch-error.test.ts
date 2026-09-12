// ---------------------------------------------------------------------------
// catchError — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source } from '../types.js';
import { pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { catchError } from './catch-error.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';

/** Source that emits `values` then errors with `err`. */
function failAfter<T>(values: T[], err: unknown = new Error('fail')): Source<T> {
  return {
    connect(sink) {
      let index = 0;
      let disposed = false;
      return {
        resume() {
          if (disposed) return;
          while (index < values.length) {
            const result = sink.next(values[index++]);
            if (result) return; // PAUSE
          }
          sink.error(err);
        },
        [Symbol.dispose]() {
          disposed = true;
        },
      };
    },
  };
}

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('catchError (exhaustive)', () => {
  it('error after one value — recover with array', () => {
    exhaustiveTest((oracle) => {
      const sink = testSink<number>({ oracle });
      const s = pipe(
        failAfter([1]),
        catchError(() => fromArray([99])),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 99]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('error after two values — longer fallback', () => {
    exhaustiveTest((oracle) => {
      const sink = testSink<number>({ oracle });
      const s = pipe(
        failAfter([1, 2]),
        catchError(() => fromArray([8, 9])),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 8, 9]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('immediate error — full fallback', () => {
    exhaustiveTest((oracle) => {
      const sink = testSink<number>({ oracle });
      const s = pipe(
        failAfter<number>([]),
        catchError(() => fromArray([5, 6, 7])),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([5, 6, 7]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('no error — passthrough', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        catchError(() => fromArray([99])),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('error handler receives original error', () => {
    const captured: unknown[] = [];
    exhaustiveTest((oracle) => {
      captured.length = 0;
      const sink = testSink<number>({ oracle });
      const s = pipe(
        failAfter([1], new Error('oops')),
        catchError((err) => {
          captured.push(err);
          return fromArray([42]);
        }),
      ).connect(sink);
      drive(s, sink);
      expect(captured).toHaveLength(1);
      expect((captured[0] as Error).message).toBe('oops');
    });
  });
});
