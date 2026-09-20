// ---------------------------------------------------------------------------
// flatten — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source, Sink } from '../types.js';
import { pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { empty } from '../sources/empty.js';
import { createRelay } from '../relay.js';
import { flatten } from './flatten.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('flatten (exhaustive)', () => {
  it('two inner arrays — all pause orderings', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([fromArray([1, 2]), fromArray([3, 4])], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3, 4]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('two self-pausing inners — all pause orderings', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource(
        [testSource([1, 2], { oracle }), testSource([3, 4], { oracle })],
        { oracle },
      );
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3, 4]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('three inner arrays', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([fromArray([1]), fromArray([2, 3]), fromArray([4])], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3, 4]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty inner first', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([empty<number>(), fromArray([1, 2])], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty inner last', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([fromArray([1, 2]), empty<number>()], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('all empty inners', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([empty<number>(), empty<number>(), empty<number>()], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('single inner with many values', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([fromArray([1, 2, 3, 4, 5])], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3, 4, 5]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty outer', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource<Source<number>>([], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, flatten(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });
});

describe('flatten (bug 3 — outer error with an active inner)', () => {
  function tracked<T>(source: Source<T>): Source<T> & { disposed: number } {
    const t = {
      disposed: 0,
      connect(sink: Sink<T>) {
        const s = source.connect(sink);
        return {
          resume: () => s.resume(),
          [Symbol.dispose]: () => {
            t.disposed++;
            s[Symbol.dispose]();
          },
        };
      },
    };
    return t;
  }

  it('disposes the active inner and ignores its later values', () => {
    const outer = createRelay<Source<number>>();
    const innerRelay = createRelay<number>();
    const inner = tracked(innerRelay);
    const sink = testSink<number>();
    const s = pipe(outer, flatten(), assertProtocol()).connect(sink);
    s.resume();
    outer.next(inner);
    innerRelay.next(1);
    expect(sink.values).toEqual([1]);

    outer.error(new Error('outer boom'));
    expect(sink.errors).toHaveLength(1);
    expect(inner.disposed).toBe(1);

    innerRelay.next(2); // the inner is gone; nothing reaches the sink
    innerRelay.complete();
    expect(sink.values).toEqual([1]);
    s.resume(); // no-op after terminal
  });

  it('inner error disposes the outer', () => {
    const outerRelay = createRelay<Source<number>>();
    const outer = tracked(outerRelay);
    const innerRelay = createRelay<number>();
    const sink = testSink<number>();
    const s = pipe(outer, flatten(), assertProtocol()).connect(sink);
    s.resume();
    outerRelay.next(innerRelay);
    innerRelay.error(new Error('inner boom'));
    expect(sink.errors).toHaveLength(1);
    expect(outer.disposed).toBe(1);
    outerRelay.next(fromArray([9])); // ignored
    expect(sink.values).toEqual([]);
  });
});

describe('flatten (relay outer)', () => {
  it('serializes inners that arrive asynchronously', () => {
    const outer = createRelay<Source<number>>();
    const sink = testSink<number>();
    const s = pipe(outer, flatten(), assertProtocol()).connect(sink);
    s.resume();
    outer.next(fromArray([1, 2]));
    expect(sink.values).toEqual([1, 2]);

    const slow = createRelay<number>();
    outer.next(slow);
    outer.next(fromArray([5])); // queued in the relay: outer is paused while `slow` is active
    expect(sink.values).toEqual([1, 2]);
    slow.next(3);
    slow.next(4);
    slow.complete(); // inner done → outer resumed → [5] delivered
    expect(sink.values).toEqual([1, 2, 3, 4, 5]);

    outer.complete();
    expect(sink.completeCount).toBe(1);
  });

  it('does not start the next inner while the downstream is paused', () => {
    let bResumed = 0;
    const b: Source<number> = {
      connect(inner: Sink<number>) {
        return {
          resume() {
            bResumed++;
            inner.next(2);
            inner.complete();
          },
          [Symbol.dispose]() {},
        };
      },
    };
    let pauseNext = true;
    const sink = testSink<number>({ oracle: { integer: () => (pauseNext ? 1 : 0) } });
    const s = pipe(fromArray([fromArray([1]), b]), flatten(), assertProtocol()).connect(sink);
    s.resume(); // 1 delivered, sink pauses, inner A completes synchronously
    expect(sink.values).toEqual([1]);
    expect(bResumed).toBe(0);

    pauseNext = false;
    s.resume();
    expect(bResumed).toBe(1);
    expect(sink.values).toEqual([1, 2]);
    expect(sink.completeCount).toBe(1);
  });

  it('an inner completing asynchronously while the downstream is paused waits for resume()', () => {
    const outer = createRelay<Source<number>>();
    const a = createRelay<number>();
    let bResumed = 0;
    const b: Source<number> = {
      connect(inner: Sink<number>) {
        return {
          resume() {
            bResumed++;
            inner.next(2);
            inner.complete();
          },
          [Symbol.dispose]() {},
        };
      },
    };
    let pauseNext = true;
    const sink = testSink<number>({ oracle: { integer: () => (pauseNext ? 1 : 0) } });
    const s = pipe(outer, flatten(), assertProtocol()).connect(sink);
    s.resume();
    outer.next(a);
    outer.next(b);
    a.next(1); // sink pauses
    a.complete(); // outer must not be resumed yet
    expect(bResumed).toBe(0);

    pauseNext = false;
    s.resume();
    expect(bResumed).toBe(1);
    expect(sink.values).toEqual([1, 2]);
  });
});
