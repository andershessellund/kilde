// ---------------------------------------------------------------------------
// toArray — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe, stream } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { toArray } from './to-array.js';
import { toPromise } from './to-promise.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('toArray (exhaustive)', () => {
  it('collects everything — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number[]>({ oracle });
      const s = pipe(src, toArray(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([[1, 2, 3]]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty source — emits []', () => {
    exhaustiveTest((oracle) => {
      const src = testSource<number>([], { oracle });
      const sink = testSink<number[]>({ oracle });
      const s = pipe(src, toArray(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([[]]);
      expect(sink.completeCount).toBe(1);
    });
  });
});

describe('toArray (protocol)', () => {
  it('works as a collector for stream()', () => {
    expect(stream(fromArray([1, 2, 3]), toArray())).toEqual([1, 2, 3]);
  });

  it('the emitted array survives the downstream disposing us synchronously', async () => {
    // toPromise disposes its upstream inside next(), while still holding
    // the array we just handed it.
    const result = await stream(pipe(fromArray([1, 2]), toArray()), toPromise());
    expect(result).toEqual([1, 2]);
  });

  it('forwards errors without emitting a partial array', () => {
    const sink = testSink<number[]>();
    const s = pipe(
      {
        connect(inner: { next(v: number): unknown; error(e: unknown): void }) {
          return {
            resume() {
              inner.next(1);
              inner.error(new Error('boom'));
            },
            [Symbol.dispose]() {},
          };
        },
      },
      toArray<number>(),
      assertProtocol(),
    ).connect(sink);
    s.resume();
    expect(sink.values).toEqual([]);
    expect(sink.errors).toHaveLength(1);
  });
});
