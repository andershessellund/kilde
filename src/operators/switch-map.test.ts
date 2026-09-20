// ---------------------------------------------------------------------------
// switchMap — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source, Sink, Stream } from '../types.js';
import { PAUSE } from '../types.js';
import { pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { empty } from '../sources/empty.js';
import { createRelay } from '../relay.js';
import { switchMap } from './switch-map.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('switchMap (exhaustive)', () => {
  it('single outer value — maps to inner array', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([10], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, switchMap((x) => fromArray([x, x + 1])), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([10, 11]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('single outer value — self-pausing inner', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([10], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        switchMap((x) => testSource([x, x + 1, x + 2], { oracle })),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([10, 11, 12]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('two outer values — each inner completes before next arrives', async () => {
    // With fromArray inners and sequential outer, each inner completes
    // synchronously before the next outer value — unless the downstream
    // paused, in which case the pending inner is switched away.
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, switchMap((x) => fromArray([x * 10, x * 10 + 1])), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.completeCount).toBe(1);
      // The last inner always runs to completion.
      expect(sink.values.slice(-2)).toEqual([20, 21]);
    });
  });

  it('switch disposes previous inner', () => {
    let disposed = false;

    const slowInner: Source<number> = {
      connect(sink: Sink<number>): Stream {
        // Never completes — simulates a long-running source
        return {
          resume() { sink.next(42); },
          [Symbol.dispose]() { disposed = true; },
        };
      },
    };

    const src = fromArray([1, 2]);
    const sink = testSink<number>();
    const s = pipe(
      src,
      switchMap((x) => (x === 1 ? slowInner : fromArray([99]))),
      assertProtocol(),
    ).connect(sink);
    s.resume();
    // First outer value connects slowInner. Second outer value switches
    // away — slowInner should be disposed.
    expect(disposed).toBe(true);
    expect(sink.values).toContain(99);
  });

  it('empty outer — completes immediately', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource<number>([], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, switchMap((x) => fromArray([x])), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('outer with empty inner', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, switchMap(() => empty<number>()), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('mixed empty and non-empty inners', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        switchMap((x) => (x === 2 ? empty<number>() : fromArray([x * 10]))),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.completeCount).toBe(1);
      expect(sink.values[sink.values.length - 1]).toBe(30);
    });
  });

  it('three outer values with multi-value inners', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        switchMap((x) => fromArray([x, x + 10])),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.completeCount).toBe(1);
      expect(sink.values.slice(-2)).toEqual([3, 13]);
    });
  });

  it('dispose stops both outer and inner', () => {
    let innerDisposed = false;
    let outerDisposed = false;

    const neverComplete: Source<number> = {
      connect(sink: Sink<number>): Stream {
        return {
          resume() { sink.next(1); },
          [Symbol.dispose]() { innerDisposed = true; },
        };
      },
    };

    const outerSource: Source<number> = {
      connect(sink: Sink<number>): Stream {
        return {
          resume() { sink.next(1); },
          [Symbol.dispose]() { outerDisposed = true; },
        };
      },
    };

    const sink = testSink<number>();
    const s = pipe(outerSource, switchMap(() => neverComplete)).connect(sink);
    s.resume();
    s[Symbol.dispose]();
    expect(innerDisposed).toBe(true);
    expect(outerDisposed).toBe(true);
  });

  it('inner error propagates to downstream', () => {
    const failingInner: Source<number> = {
      connect(sink: Sink<number>): Stream {
        return {
          resume() { sink.error(new Error('boom')); },
          [Symbol.dispose]() {},
        };
      },
    };

    const src = fromArray([1]);
    const sink = testSink<number>();
    const s = pipe(src, switchMap(() => failingInner), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('boom');
  });

  it('outer error propagates to downstream', () => {
    const failingOuter: Source<number> = {
      connect(sink: Sink<number>): Stream {
        return {
          resume() { sink.error(new Error('outer-boom')); },
          [Symbol.dispose]() {},
        };
      },
    };

    const sink = testSink<number>();
    const s = pipe(failingOuter, switchMap((x) => fromArray([x])), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('outer-boom');
  });

  it('downstream pause is respected across switch', () => {
    // Manual test: downstream pauses, outer switches, new inner should
    // not deliver until resume.
    const values: number[] = [];
    let innerStream1: Stream | undefined;
    let innerStream2: Stream | undefined;

    const inner1: Source<number> = {
      connect(sink: Sink<number>): Stream {
        innerStream1 = {
          resume() { sink.next(1); },
          [Symbol.dispose]() {},
        };
        return innerStream1;
      },
    };

    const inner2: Source<number> = {
      connect(sink: Sink<number>): Stream {
        innerStream2 = {
          resume() {
            sink.next(2);
            sink.complete();
          },
          [Symbol.dispose]() {},
        };
        return innerStream2;
      },
    };

    let outerSink: Sink<number> | undefined;
    const outer: Source<number> = {
      connect(sink: Sink<number>): Stream {
        outerSink = sink;
        return {
          resume() { sink.next(1); },
          [Symbol.dispose]() {},
        };
      },
    };

    let pauseNext = false;
    const downstream: Sink<number> = {
      next(v) {
        values.push(v);
        if (pauseNext) return PAUSE;
        return undefined;
      },
      complete() {},
      error() {},
    };

    const s = pipe(outer, switchMap((x) => (x === 1 ? inner1 : inner2))).connect(downstream);

    // Start — outer emits 1, inner1 connected but not resumed (downstream paused)
    // resume → inner1 resumes, delivers 1
    pauseNext = true;
    s.resume();
    expect(values).toEqual([1]);

    // Downstream is paused. Now outer emits 2 — inner1 disposed, inner2 connected
    outerSink!.next(2);

    // inner2 should NOT have delivered yet (downstream still paused)
    expect(values).toEqual([1]);

    // Resume — inner2 delivers
    pauseNext = false;
    s.resume();
    expect(values).toEqual([1, 2]);
  });
});

describe('switchMap (bug 3 — outer error / throwing project fn)', () => {
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

  it('outer error disposes the active inner and ignores its later values', () => {
    const outer = createRelay<number>();
    const innerRelay = createRelay<number>();
    const inner = tracked(innerRelay);
    const sink = testSink<number>();
    const s = pipe(outer, switchMap(() => inner), assertProtocol()).connect(sink);
    s.resume();
    outer.next(1);
    innerRelay.next(10);
    expect(sink.values).toEqual([10]);

    outer.error(new Error('outer boom'));
    expect(sink.errors).toHaveLength(1);
    expect(inner.disposed).toBe(1);

    innerRelay.next(11);
    innerRelay.complete();
    expect(sink.values).toEqual([10]);
    s.resume(); // no-op after terminal
  });

  it('a throwing project fn errors the downstream and disposes the outer', () => {
    const outerRelay = createRelay<number>();
    const outer = tracked(outerRelay);
    const sink = testSink<number>();
    const s = pipe(
      outer,
      switchMap((x: number) => {
        if (x === 2) throw new Error('project-fail');
        return fromArray([x]);
      }),
      assertProtocol(),
    ).connect(sink);
    s.resume();
    outerRelay.next(1);
    expect(sink.values).toEqual([1]);
    expect(() => outerRelay.next(2)).not.toThrow();
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('project-fail');
    expect(outer.disposed).toBe(1);
    outerRelay.next(3); // ignored
    expect(sink.values).toEqual([1]);
  });

  it('inner error disposes the outer', () => {
    const outerRelay = createRelay<number>();
    const outer = tracked(outerRelay);
    const innerRelay = createRelay<number>();
    const sink = testSink<number>();
    const s = pipe(outer, switchMap(() => innerRelay), assertProtocol()).connect(sink);
    s.resume();
    outerRelay.next(1);
    innerRelay.error(new Error('inner boom'));
    expect(sink.errors).toHaveLength(1);
    expect(outer.disposed).toBe(1);
  });
});

describe('switchMap (relay outer)', () => {
  it('switches between asynchronous inners', () => {
    const outer = createRelay<number>();
    const inners = [createRelay<number>(), createRelay<number>()];
    const sink = testSink<number>();
    const s = pipe(outer, switchMap((i: number) => inners[i]), assertProtocol()).connect(sink);
    s.resume();

    outer.next(0);
    inners[0].next(1);
    outer.next(1); // switch: inners[0] is disposed
    inners[0].next(99); // dropped
    inners[1].next(2);
    expect(sink.values).toEqual([1, 2]);

    outer.complete();
    expect(sink.completeCount).toBe(0); // inner still active
    inners[1].next(3);
    inners[1].complete();
    expect(sink.values).toEqual([1, 2, 3]);
    expect(sink.completeCount).toBe(1);
  });

  it('an inner arriving while the downstream is paused waits for resume()', () => {
    const outer = createRelay<number>();
    let pauseNext = true;
    const sink = testSink<number>({ oracle: { integer: () => (pauseNext ? 1 : 0) } });
    const s = pipe(outer, switchMap((x: number) => fromArray([x, x + 1])), assertProtocol()).connect(sink);
    s.resume();
    outer.next(10); // 10 delivered, sink pauses, inner paused
    outer.next(20); // switch; new inner not resumed
    expect(sink.values).toEqual([10]);

    pauseNext = false;
    s.resume();
    expect(sink.values).toEqual([10, 20, 21]);
  });
});
