// ---------------------------------------------------------------------------
// merge — tests
//
// merge interleaves inner sources. The exhaustive tests therefore check
// that every value arrives exactly once and that each inner's values keep
// their relative order — not one fixed global order.
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source, Sink } from '../types.js';
import { stream, pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { empty } from '../sources/empty.js';
import { createRelay } from '../relay.js';
import { merge } from './merge.js';
import { toArray } from './to-array.js';
import { map } from './map.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 60) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

/** Every value once, and each group's values in their original order. */
function expectInterleaving<T>(values: T[], groups: T[][]) {
  const all = groups.flat();
  expect([...values].sort()).toEqual([...all].sort());
  for (const group of groups) {
    const seen = values.filter((v) => group.includes(v));
    expect(seen).toEqual(group);
  }
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
    const s = pipe(fromArray<Source<number>>([errorSource]), merge(), assertProtocol()).connect(sink);
    s.resume();

    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('inner boom');
    s.resume(); // no-op after terminal
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
    const s = pipe(errorOuter, merge(), assertProtocol()).connect(sink);
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
      const s = pipe(src, merge(), assertProtocol()).connect(sink);
      drive(s, sink);
      expectInterleaving(sink.values, [[1, 2], [3, 4]]);
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
      const s = pipe(src, merge(), assertProtocol()).connect(sink);
      drive(s, sink);
      expectInterleaving(sink.values, [[1], [2, 3], [4]]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('two self-pausing inners — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(
        [testSource([1, 2], { oracle }), testSource([3, 4], { oracle })],
        { oracle },
      );
      const sink = testSink<number>({ oracle });
      const s = pipe(src, merge(), assertProtocol()).connect(sink);
      drive(s, sink);
      expectInterleaving(sink.values, [[1, 2], [3, 4]]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty inner first — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([empty<number>(), fromArray([1, 2])], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, merge(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty inner last — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([fromArray([1, 2]), empty<number>()], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, merge(), assertProtocol()).connect(sink);
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
      const s = pipe(src, merge(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('single inner with many values — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([fromArray([1, 2, 3, 4, 5])], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, merge(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3, 4, 5]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty outer — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource<Source<number>>([], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, merge(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });
});

// ---------------------------------------------------------------------------
// Bug 2 — asynchronous inners, downstream pause, fairness
// ---------------------------------------------------------------------------

describe('merge (bug 2 — relay outer)', () => {
  it('inners that arrive asynchronously are started', () => {
    const outer = createRelay<Source<number>>();
    const sink = testSink<number>();
    const s = pipe(outer, merge(), assertProtocol()).connect(sink);
    s.resume();

    outer.next(fromArray([1, 2]));
    expect(sink.values).toEqual([1, 2]);
    outer.next(fromArray([3]));
    expect(sink.values).toEqual([1, 2, 3]);
    expect(sink.completeCount).toBe(0);

    outer.complete();
    expect(sink.completeCount).toBe(1);
  });

  it('downstream PAUSE stops every inner; resume drains held values then continues', () => {
    const outer = createRelay<Source<number>>();
    const a = createRelay<number>();
    const b = createRelay<number>();
    let pauseNext = true;
    const sink = testSink<number>({ oracle: { integer: () => (pauseNext ? 1 : 0) } });
    const s = pipe(outer, merge(), assertProtocol()).connect(sink);
    s.resume();
    outer.next(a);
    outer.next(b);

    a.next(1); // delivered, downstream pauses
    b.next(2); // held by merge, b's subscription receives PAUSE
    a.next(3); // a's subscription is paused → buffered in the relay
    b.next(4); // buffered in the relay
    expect(sink.values).toEqual([1]);

    pauseNext = false;
    s.resume(); // delivers the held 2, then resumes inners
    expect([...sink.values].sort()).toEqual([1, 2, 3, 4]);
    expect(sink.values.indexOf(1)).toBeLessThan(sink.values.indexOf(3));
    expect(sink.values.indexOf(2)).toBeLessThan(sink.values.indexOf(4));

    a.complete();
    b.complete();
    expect(sink.completeCount).toBe(0);
    outer.complete();
    expect(sink.completeCount).toBe(1);
  });

  it('an inner arriving while the downstream is paused waits for resume()', () => {
    const outer = createRelay<Source<number>>();
    let resumed = 0;
    const lateInner: Source<number> = {
      connect(inner: Sink<number>) {
        return {
          resume() {
            resumed++;
            inner.next(7);
            inner.complete();
          },
          [Symbol.dispose]() {},
        };
      },
    };
    let pauseNext = true;
    const sink = testSink<number>({ oracle: { integer: () => (pauseNext ? 1 : 0) } });
    const s = pipe(outer, merge(), assertProtocol()).connect(sink);
    s.resume();
    outer.next(fromArray([1])); // downstream pauses on 1
    outer.next(lateInner);
    expect(resumed).toBe(0);

    pauseNext = false;
    s.resume();
    expect(resumed).toBe(1);
    expect(sink.values).toEqual([1, 7]);
  });

  it('resumes inners round-robin from where it left off (fairness)', () => {
    // Each inner delivers one value per resume and never completes.
    const counter = (name: string): Source<string> => ({
      connect(inner: Sink<string>) {
        let n = 0;
        return {
          resume() {
            inner.next(`${name}${++n}`);
          },
          [Symbol.dispose]() {},
        };
      },
    });
    const sink = testSink<string>({ oracle: { integer: () => 1 } }); // always PAUSE
    const s = pipe(fromArray([counter('a'), counter('b')]), merge(), assertProtocol()).connect(sink);
    s.resume(); // a starts (pauses the sink); b is connected but waits
    for (let i = 0; i < 5; i++) s.resume();
    expect(sink.values).toEqual(['a1', 'b1', 'a2', 'b2', 'a3', 'b3']);
  });

  it('inner error is delivered after held values', () => {
    const outer = createRelay<Source<number>>();
    const a = createRelay<number>();
    const b = createRelay<number>();
    let pauseNext = true;
    const sink = testSink<number>({ oracle: { integer: () => (pauseNext ? 1 : 0) } });
    const s = pipe(outer, merge(), assertProtocol()).connect(sink);
    s.resume();
    outer.next(a);
    outer.next(b);
    a.next(1); // pauses
    b.next(2); // held
    a.error(new Error('boom')); // held behind 2
    expect(sink.errors).toHaveLength(0);

    pauseNext = false;
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    expect(sink.errors).toHaveLength(1);
    s.resume(); // no-op
  });

  it('completes only after the last inner completes', () => {
    const outer = createRelay<Source<number>>();
    const a = createRelay<number>();
    const sink = testSink<number>();
    const s = pipe(outer, merge(), assertProtocol()).connect(sink);
    s.resume();
    outer.next(a);
    outer.complete();
    expect(sink.completeCount).toBe(0);
    a.next(1);
    a.complete();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(1);
  });
});
