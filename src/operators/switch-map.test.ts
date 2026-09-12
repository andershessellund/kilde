// ---------------------------------------------------------------------------
// switchMap — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source, Sink, Stream } from '../types.js';
import { PAUSE } from '../types.js';
import { pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { empty } from '../sources/empty.js';
import { switchMap } from './switch-map.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('switchMap (exhaustive)', () => {
  it('single outer value — maps to inner array', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([10], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, switchMap((x) => fromArray([x, x + 1]))).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([10, 11]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('two outer values — each inner completes before next arrives', () => {
    // With fromArray inners and sequential outer, each inner completes
    // synchronously before the next outer value. All values are collected.
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, switchMap((x) => fromArray([x * 10, x * 10 + 1]))).connect(sink);
      drive(s, sink);
      // Outer emits 1 → inner [10, 11]. Then outer emits 2 → disposes
      // inner (already complete), new inner [20, 21].
      // With synchronous inners, both complete before being switched.
      expect(sink.completeCount).toBe(1);
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
    ).connect(sink);
    s.resume();
    // First outer value connects slowInner. Second outer value switches
    // away — slowInner should be disposed.
    expect(disposed).toBe(true);
    expect(sink.values).toContain(99);
  });

  it('empty outer — completes immediately', () => {
    exhaustiveTest((oracle) => {
      const src = testSource<number>([], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, switchMap((x) => fromArray([x]))).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('outer with empty inner', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, switchMap(() => empty<number>())).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('mixed empty and non-empty inners', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        switchMap((x) => (x === 2 ? empty<number>() : fromArray([x * 10]))),
      ).connect(sink);
      drive(s, sink);
      // Value 1 → [10], value 2 → empty (disposes [10] if still active),
      // value 3 → [30]. With sync inners, all complete before switch.
      expect(sink.completeCount).toBe(1);
    });
  });

  it('three outer values with multi-value inners', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        switchMap((x) => fromArray([x, x + 10])),
      ).connect(sink);
      drive(s, sink);
      expect(sink.completeCount).toBe(1);
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
    const s = pipe(src, switchMap(() => failingInner)).connect(sink);
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
    const s = pipe(failingOuter, switchMap((x) => fromArray([x]))).connect(sink);
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
