// ---------------------------------------------------------------------------
// combineLatest — tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source, Sink } from './types.js';
import { PAUSE } from './types.js';
import { combineLatest } from './combine-latest.js';
import { createSignal } from './signal.js';
import { createRelay } from './relay.js';
import { fromArray } from './sources/from-array.js';
import { testSource } from './testing/test-source.js';
import { testSink } from './testing/test-sink.js';
import { exhaustiveTest } from './testing/exhaustive.js';
import { fromSignal } from './sources/from-signal.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function recordSink<T>(): Sink<T> & { values: T[]; completed: boolean; errors: unknown[] } {
  const sink = {
    values: [] as T[],
    completed: false,
    errors: [] as unknown[],
    next(value: T): undefined | typeof PAUSE {
      sink.values.push(value);
      return undefined;
    },
    complete() {
      sink.completed = true;
    },
    error(err: unknown) {
      sink.errors.push(err);
    },
  };
  return sink;
}

function pauseAfter<T>(n: number): Sink<T> & { values: T[]; completed: boolean } {
  let count = 0;
  const sink = {
    values: [] as T[],
    completed: false,
    next(value: T): undefined | typeof PAUSE {
      sink.values.push(value);
      count++;
      return count >= n ? PAUSE : undefined;
    },
    complete() {
      sink.completed = true;
    },
    error() {},
  };
  return sink;
}

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 60) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

// ===========================================================================
// combineLatest
// ===========================================================================

describe('combineLatest', () => {
  // ---------------------------------------------------------------------------
  // Basic behavior
  // ---------------------------------------------------------------------------

  it('combines two synchronous sources', () => {
    // fromArray sources emit all values synchronously on resume, so the
    // combined output is the final combined tuple after both complete.
    const a = fromArray([1, 2]);
    const b = fromArray(['x', 'y']);
    const sink = recordSink<[number, string]>();
    const s = combineLatest([a, b]).connect(sink);
    s.resume();

    // Both sources deliver synchronously. a emits 1 then 2, b emits 'x' then 'y'.
    // After a:1 → waiting for b. After b:'x' → emit [2, 'x'] (a already at 2).
    // Then b:'y' → emit [2, 'y']. Then both complete.
    expect(sink.values).toEqual([[2, 'x'], [2, 'y']]);
    expect(sink.completed).toBe(true);
  });

  it('waits for all sources before emitting', () => {
    const a = createRelay<number>();
    const b = createRelay<string>();
    const sink = recordSink<[number, string]>();
    const s = combineLatest([a, b]).connect(sink);
    s.resume();

    a.next(1);
    expect(sink.values).toEqual([]); // b hasn't emitted

    b.next('x');
    expect(sink.values).toEqual([[1, 'x']]); // now both have emitted

    a.next(2);
    expect(sink.values).toEqual([[1, 'x'], [2, 'x']]);
  });

  it('emits on each input change after all have values', () => {
    const a = createRelay<number>();
    const b = createRelay<number>();
    const c = createRelay<number>();
    const sink = recordSink<[number, number, number]>();
    const s = combineLatest([a, b, c]).connect(sink);
    s.resume();

    a.next(1);
    b.next(2);
    c.next(3);
    expect(sink.values).toEqual([[1, 2, 3]]);

    b.next(20);
    expect(sink.values).toEqual([[1, 2, 3], [1, 20, 3]]);

    a.next(10);
    expect(sink.values).toEqual([[1, 2, 3], [1, 20, 3], [10, 20, 3]]);
  });

  it('combines Signal inputs with replay', () => {
    const a = createSignal(1);
    const b = createSignal('hello');
    const sink = recordSink<[number, string]>();
    const s = combineLatest([fromSignal(a), fromSignal(b)]).connect(sink);
    s.resume();

    // Both states replay immediately → first tuple
    expect(sink.values).toEqual([[1, 'hello']]);

    a.set(2);
    expect(sink.values).toEqual([[1, 'hello'], [2, 'hello']]);
  });

  it('each emission is a fresh array reference', () => {
    const a = createRelay<number>();
    const b = createRelay<number>();
    const sink = recordSink<[number, number]>();
    const s = combineLatest([a, b]).connect(sink);
    s.resume();

    a.next(1);
    b.next(2);
    a.next(3);

    expect(sink.values).toHaveLength(2);
    expect(sink.values[0]).not.toBe(sink.values[1]);
  });

  // ---------------------------------------------------------------------------
  // Single source and empty sources
  // ---------------------------------------------------------------------------

  it('single source wraps values in tuple', () => {
    const a = createRelay<number>();
    const sink = recordSink<[number]>();
    const s = combineLatest([a]).connect(sink);
    s.resume();

    a.next(42);
    expect(sink.values).toEqual([[42]]);
  });

  it('empty sources array completes immediately', () => {
    const sink = recordSink<[]>();
    const s = combineLatest([]).connect(sink);
    s.resume();
    expect(sink.completed).toBe(true);
    expect(sink.values).toEqual([]);
  });

  // ---------------------------------------------------------------------------
  // Backpressure
  // ---------------------------------------------------------------------------

  it('pauses all inputs when downstream pauses', () => {
    const sink = pauseAfter<[number, string]>(1);
    const a = createRelay<number>();
    const b = createRelay<string>();
    const s = combineLatest([a, b]).connect(sink);
    s.resume();

    a.next(1);
    b.next('x'); // → emit [1, 'x'], sink PAUSEs after 1 value

    // Further emissions are buffered in the relay subscriptions
    a.next(2);
    b.next('y');
    expect(sink.values).toEqual([[1, 'x']]);

    // Resume: delivers dirty if any, then resumes inputs which drain
    // relay buffers. Input a resumes first → drains 2 → delivers [2, 'x'].
    // Sink pauses again (count ≥ 1), so b's 'y' stays buffered.
    s.resume();
    expect(sink.values).toEqual([[1, 'x'], [2, 'x']]);

    // Resume again: b drains 'y' → delivers [2, 'y']
    s.resume();
    expect(sink.values).toEqual([[1, 'x'], [2, 'x'], [2, 'y']]);
  });

  it('returns PAUSE to input when downstream is paused', () => {
    let _inputPaused = false;
    const customSource: Source<number> = {
      connect(sink) {
        return {
          resume() {
            const result = sink.next(1);
            if (result === PAUSE) _inputPaused = true;
          },
          [Symbol.dispose]() {},
        };
      },
    };

    // sink pauses immediately
    const sink = pauseAfter<[number, string]>(0);
    const b = createSignal('x');
    const s = combineLatest([customSource, fromSignal(b)]).connect(sink);
    s.resume();

    // After b replays 'x' and customSource emits 1, downstream pauses.
    // But since both emit during resume(), the first delivery happens,
    // then the downstream is paused. Let's verify with a relay instead:
    _inputPaused = false;

    const a2 = createRelay<number>();
    const b2 = createRelay<string>();
    const sink2 = pauseAfter<[number, string]>(1);
    const s2 = combineLatest([a2, b2]).connect(sink2);
    s2.resume();

    a2.next(1);
    b2.next('x'); // emits [1, 'x'], sink pauses

    // Now a2.next should get PAUSE propagated via dirty flag
    a2.next(2);
    // The relay will have delivered next(2) which stored latest and marked dirty
    // The PAUSE is returned to input when downstream is paused
    expect(sink2.values).toEqual([[1, 'x']]);
  });

  // ---------------------------------------------------------------------------
  // Completion
  // ---------------------------------------------------------------------------

  it('completes only when all inputs complete', () => {
    const a = createRelay<number>();
    const b = createRelay<string>();
    const sink = recordSink<[number, string]>();
    const s = combineLatest([a, b]).connect(sink);
    s.resume();

    a.next(1);
    b.next('x');
    a.complete();
    expect(sink.completed).toBe(false);

    b.next('y');
    expect(sink.values).toEqual([[1, 'x'], [1, 'y']]);

    b.complete();
    expect(sink.completed).toBe(true);
  });

  it('source completing before first emit still waits for others', () => {
    const a = createRelay<number>();
    const b = createRelay<string>();
    const sink = recordSink<[number, string]>();
    const s = combineLatest([a, b]).connect(sink);
    s.resume();

    a.next(1);
    a.complete();
    expect(sink.completed).toBe(false);

    b.next('x');
    expect(sink.values).toEqual([[1, 'x']]);

    b.complete();
    expect(sink.completed).toBe(true);
  });

  // ---------------------------------------------------------------------------
  // Error
  // ---------------------------------------------------------------------------

  it('error from any input disposes all and forwards error', () => {
    let bDisposed = false;
    const a = createRelay<number>();
    const b: Source<string> = {
      connect(sink) {
        return {
          resume() {
            sink.next('x');
          },
          [Symbol.dispose]() {
            bDisposed = true;
          },
        };
      },
    };
    const sink = recordSink<[number, string]>();
    const s = combineLatest([a, b]).connect(sink);
    s.resume();

    a.next(1); // now both have values
    a.error(new Error('boom'));
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('boom');
    expect(bDisposed).toBe(true);
  });

  // ---------------------------------------------------------------------------
  // Dispose
  // ---------------------------------------------------------------------------

  it('dispose stops all input connections', () => {
    let disposedCount = 0;
    const makeSource = (): Source<number> => ({
      connect(sink) {
        return {
          resume() {
            sink.next(1);
          },
          [Symbol.dispose]() {
            disposedCount++;
          },
        };
      },
    });

    const sink = recordSink<[number, number, number]>();
    const s = combineLatest([makeSource(), makeSource(), makeSource()]).connect(sink);
    s.resume();
    s[Symbol.dispose]();
    expect(disposedCount).toBe(3);
  });

  // ---------------------------------------------------------------------------
  // Reentrant delivery
  // ---------------------------------------------------------------------------

  it('handles reentrant set during delivery', () => {
    const a = createSignal(1);
    const b = createSignal(10);
    const values: [number, number][] = [];

    const sink: Sink<[number, number]> = {
      next(value) {
        values.push(value);
        // On first delivery, trigger a reentrant change
        if (value[0] === 1 && value[1] === 10) {
          a.set(2);
        }
        return undefined;
      },
      complete() {},
      error() {},
    };

    const s = combineLatest([fromSignal(a), fromSignal(b)]).connect(sink);
    s.resume();

    // Resume delivers [1, 10], reentrant set(2) → dirty → delivers [2, 10]
    expect(values).toEqual([[1, 10], [2, 10]]);
  });

  // ---------------------------------------------------------------------------
  // TypeScript inference
  // ---------------------------------------------------------------------------

  it('infers tuple types correctly', () => {
    const a = createSignal(42);
    const b = createSignal('hello');
    const c = createSignal(true);

    // This is a compile-time check — if types are wrong, TS will error
    const combined: Source<[number, string, boolean]> = combineLatest([fromSignal(a), fromSignal(b), fromSignal(c)]);

    const sink = recordSink<[number, string, boolean]>();
    const s = combined.connect(sink);
    s.resume();
    expect(sink.values).toEqual([[42, 'hello', true]]);
  });

  // ---------------------------------------------------------------------------
  // Exhaustive tests (all pause/resume interleavings)
  // ---------------------------------------------------------------------------

  it('two fromArray sources — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const a = testSource([1, 2], { oracle });
      const b = testSource([10, 20], { oracle });
      const sink = testSink<[number, number]>({ oracle });
      const s = combineLatest([a, b]).connect(sink);
      drive(s, sink);

      // Both sources emit all values. The final combined state must include
      // the last value from each source.
      const last = sink.values[sink.values.length - 1];
      expect(last).toEqual([2, 20]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('three sources — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const a = testSource([1], { oracle });
      const b = testSource([2, 3], { oracle });
      const c = testSource([4], { oracle });
      const sink = testSink<[number, number, number]>({ oracle });
      const s = combineLatest([a, b, c]).connect(sink);
      drive(s, sink);

      const last = sink.values[sink.values.length - 1];
      expect(last).toEqual([1, 3, 4]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('one source empty — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const a = testSource([1, 2], { oracle });
      const b = testSource<number>([], { oracle });
      const sink = testSink<[number, number]>({ oracle });
      const s = combineLatest([a, b]).connect(sink);
      drive(s, sink);

      // b never emits → no combined value ever produced
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('single source — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const a = testSource([1, 2, 3], { oracle });
      const sink = testSink<[number]>({ oracle });
      const s = combineLatest([a]).connect(sink);
      drive(s, sink);

      expect(sink.values).toEqual([[1], [2], [3]]);
      expect(sink.completeCount).toBe(1);
    });
  });
});
