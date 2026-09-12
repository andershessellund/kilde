// ---------------------------------------------------------------------------
// merge — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source } from '../types.js';
import { stream, pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { empty } from '../sources/empty.js';
import { merge } from './merge.js';
import { toArray } from './to-array.js';
import { map } from './map.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 60) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('merge', () => {
  // ---------------------------------------------------------------------------
  // Basic functional tests
  // ---------------------------------------------------------------------------

  it('merges two inner arrays', () => {
    const result = stream(
      fromArray([fromArray([1, 2]), fromArray([3, 4])]),
      merge(),
      toArray(),
    );
    expect(result).toEqual([1, 2, 3, 4]);
  });

  it('merges three inner arrays', () => {
    const result = stream(
      fromArray([fromArray([1]), fromArray([2, 3]), fromArray([4])]),
      merge(),
      toArray(),
    );
    expect(result).toEqual([1, 2, 3, 4]);
  });

  it('handles empty outer', () => {
    const result = stream(fromArray<Source<number>>([]), merge(), toArray());
    expect(result).toEqual([]);
  });

  it('handles empty inner sources', () => {
    const result = stream(
      fromArray([empty<number>(), fromArray([1, 2]), empty<number>()]),
      merge(),
      toArray(),
    );
    expect(result).toEqual([1, 2]);
  });

  it('handles single inner source', () => {
    const result = stream(fromArray([fromArray([1, 2, 3])]), merge(), toArray());
    expect(result).toEqual([1, 2, 3]);
  });

  it('handles all empty inners', () => {
    const result = stream(
      fromArray([empty<number>(), empty<number>()]),
      merge(),
      toArray(),
    );
    expect(result).toEqual([]);
  });

  it('works with map downstream', () => {
    const result = stream(
      fromArray([fromArray([1, 2]), fromArray([3, 4])]),
      merge(),
      map((x) => x * 10),
      toArray(),
    );
    expect(result).toEqual([10, 20, 30, 40]);
  });

  it('forwards inner errors', () => {
    const errorSource: Source<number> = {
      connect(sink) {
        return {
          resume() {
            sink.error(new Error('inner boom'));
          },
          [Symbol.dispose]() {},
        };
      },
    };

    const sink = testSink<number>({});
    const s = pipe(fromArray<Source<number>>([errorSource]), merge()).connect(sink);
    s.resume();

    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('inner boom');
  });

  it('forwards outer errors', () => {
    const errorOuter: Source<Source<number>> = {
      connect(sink) {
        return {
          resume() {
            sink.error(new Error('outer boom'));
          },
          [Symbol.dispose]() {},
        };
      },
    };

    const sink = testSink<number>({});
    const s = pipe(errorOuter, merge()).connect(sink);
    s.resume();

    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('outer boom');
  });

  it('dispose stops all streams', () => {
    let innerDisposed = 0;
    const neverComplete: Source<number> = {
      connect(sink) {
        return {
          resume() {
            sink.next(1);
          },
          [Symbol.dispose]() {
            innerDisposed++;
          },
        };
      },
    };

    const sink = testSink<number>({});
    const s = pipe(fromArray([neverComplete, neverComplete]), merge()).connect(sink);
    s.resume();
    s[Symbol.dispose]();

    expect(innerDisposed).toBe(2);
  });

  // ---------------------------------------------------------------------------
  // Exhaustive tests (all pause/resume interleavings)
  // ---------------------------------------------------------------------------

  it('two inner arrays — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([fromArray([1, 2]), fromArray([3, 4])], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, merge()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3, 4]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('three inner arrays — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(
        [fromArray([1]), fromArray([2, 3]), fromArray([4])],
        { oracle },
      );
      const sink = testSink<number>({ oracle });
      const s = pipe(src, merge()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3, 4]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty inner first — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([empty<number>(), fromArray([1, 2])], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, merge()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty inner last — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([fromArray([1, 2]), empty<number>()], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, merge()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('all empty inners — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(
        [empty<number>(), empty<number>(), empty<number>()],
        { oracle },
      );
      const sink = testSink<number>({ oracle });
      const s = pipe(src, merge()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('single inner with many values — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([fromArray([1, 2, 3, 4, 5])], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, merge()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3, 4, 5]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty outer — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource<Source<number>>([], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, merge()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });
});
