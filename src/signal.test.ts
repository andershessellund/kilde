// ---------------------------------------------------------------------------
// Signal — tests for createSignal and toSignal
// ---------------------------------------------------------------------------

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Sink, Source } from './types.js';
import { PAUSE } from './types.js';
import { createSignal, toSignal, computed, microtaskScheduler } from './signal.js';
import { fromArray } from './sources/from-array.js';
import type { Scheduler } from './types.js';
import { pipe } from './stream.js';
import { scan } from './operators/scan.js';
import { createRelay } from './relay.js';
import { fromSignal } from './sources/from-signal.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Simple recording sink. */
function recordSink<T>(): Sink<T> & { values: T[]; pauses: number } {
  const sink = {
    values: [] as T[],
    pauses: 0,
    next(value: T): undefined | typeof PAUSE {
      sink.values.push(value);
      return undefined;
    },
    complete() {},
    error() {},
  };
  return sink;
}

/** Recording sink that PAUSEs after N values. */
function pauseAfter<T>(n: number): Sink<T> & { values: T[] } {
  let count = 0;
  const sink = {
    values: [] as T[],
    next(value: T): undefined | typeof PAUSE {
      sink.values.push(value);
      count++;
      return count >= n ? PAUSE : undefined;
    },
    complete() {},
    error() {},
  };
  return sink;
}

// ===========================================================================
// createSignal
// ===========================================================================

describe('createSignal', () => {
  it('has initial value', () => {
    const s = createSignal(42);
    expect(s()).toBe(42);
  });

  it('set updates value', () => {
    const s = createSignal(0);
    s.set(1);
    expect(s()).toBe(1);
  });

  it('update transforms value', () => {
    const s = createSignal(10);
    s.update((n) => n + 5);
    expect(s()).toBe(15);
  });

  it('set skips deeply equal values', () => {
    const s = createSignal({ a: 1 });
    const sink = recordSink<{ a: number }>();
    const stream = fromSignal(s).connect(sink);
    stream.resume(); // gets { a: 1 }

    s.set({ a: 1 }); // deeply equal — should skip
    s.set({ a: 2 }); // different — should emit

    expect(sink.values).toEqual([{ a: 1 }, { a: 2 }]);
  });

  it('replays current value on first resume', () => {
    const s = createSignal('hello');
    s.set('world');

    const sink = recordSink<string>();
    const stream = fromSignal(s).connect(sink);
    expect(sink.values).toEqual([]); // nothing before resume

    stream.resume();
    expect(sink.values).toEqual(['world']);
  });

  it('notifies multiple subscribers', () => {
    const s = createSignal(0);

    const sink1 = recordSink<number>();
    const sink2 = recordSink<number>();
    const s1 = fromSignal(s).connect(sink1);
    const s2 = fromSignal(s).connect(sink2);
    s1.resume();
    s2.resume();

    s.set(1);
    s.set(2);

    expect(sink1.values).toEqual([0, 1, 2]);
    expect(sink2.values).toEqual([0, 1, 2]);
  });

  it('paused subscriber gets latest on resume (no intermediates)', () => {
    const s = createSignal(0);

    const sink = pauseAfter<number>(1); // PAUSE after first value
    const stream = fromSignal(s).connect(sink);
    stream.resume(); // delivers 0, then PAUSE

    s.set(1); // subscriber paused — marks dirty
    s.set(2); // still paused — marks dirty again
    s.set(3);

    expect(sink.values).toEqual([0]); // only initial

    // Allow unlimited values now
    sink.next = (v) => {
      sink.values.push(v);
      return undefined;
    };
    stream.resume();

    expect(sink.values).toEqual([0, 3]); // got latest, not 1 or 2
  });

  it('dispose removes subscriber', () => {
    const s = createSignal(0);
    const sink = recordSink<number>();
    const stream = fromSignal(s).connect(sink);
    stream.resume();

    stream[Symbol.dispose]();
    s.set(1);

    expect(sink.values).toEqual([0]); // no update after dispose
  });

  it('custom equals option', () => {
    // Reference equality — objects with same content are different
    const s = createSignal({ a: 1 }, { equals: Object.is });
    const sink = recordSink<{ a: number }>();
    const stream = fromSignal(s).connect(sink);
    stream.resume();

    s.set({ a: 1 }); // different reference → should emit
    expect(sink.values).toEqual([{ a: 1 }, { a: 1 }]);
  });

  it('reentrant set inside sink.next is handled correctly', () => {
    const s = createSignal(0);
    const values: number[] = [];

    const sink: Sink<number> = {
      next(value) {
        values.push(value);
        // Reentrant: when we receive 1, set to 2
        if (value === 1) s.set(2);
        return undefined;
      },
      complete() {},
      error() {},
    };

    const stream = fromSignal(s).connect(sink);
    stream.resume(); // delivers 0

    s.set(1); // delivers 1 → triggers set(2) → coalesced

    // Should have received 0, 1, 2 — no duplicates
    expect(values).toEqual([0, 1, 2]);
    expect(s()).toBe(2);
  });

  it('set during initial replay is handled', () => {
    const s = createSignal('a');
    const values: string[] = [];

    const sink: Sink<string> = {
      next(value) {
        values.push(value);
        if (value === 'a') s.set('b');
        return undefined;
      },
      complete() {},
      error() {},
    };

    const stream = fromSignal(s).connect(sink);
    stream.resume();

    // resume delivers 'a', which triggers set('b')
    // The notification for 'b' is coalesced via pendingNotify
    expect(values).toEqual(['a', 'b']);
    expect(s()).toBe('b');
  });

  it('throws when reentrant set causes infinite loop', () => {
    const s = createSignal(0);

    const sink: Sink<number> = {
      next(value) {
        // Always increments → never settles → should hit cap
        s.set(value + 1);
        return undefined;
      },
      complete() {},
      error() {},
    };

    const stream = fromSignal(s).connect(sink);
    expect(() => stream.resume()).toThrow('Signal update loop detected');
  });
});

// ===========================================================================
// toSignal
// ===========================================================================

describe('toSignal', () => {
  it('has initial value before upstream emits', () => {
    const relay = createRelay<number>();
    const sig = toSignal({ initial: 0 })(relay);
    expect(sig()).toBe(0);
  });

  it('replays initial value on first resume', () => {
    const relay = createRelay<number>();
    const sig = toSignal({ initial: 0 })(relay);

    const sink = recordSink<number>();
    const stream = fromSignal(sig).connect(sink);
    stream.resume();

    expect(sink.values).toEqual([0]);
  });

  it('upstream values propagate to subscribers', () => {
    const relay = createRelay<number>();
    const sig = toSignal({ initial: 0 })(relay);

    const sink = recordSink<number>();
    const stream = fromSignal(sig).connect(sink);
    stream.resume();

    relay.next(1);
    relay.next(2);

    expect(sink.values).toEqual([0, 1, 2]);
    expect(sig()).toBe(2);
  });

  it('deduplicates upstream values', () => {
    const relay = createRelay<number>();
    const sig = toSignal({ initial: 0 })(relay);

    const sink = recordSink<number>();
    const stream = fromSignal(sig).connect(sink);
    stream.resume();

    relay.next(1);
    relay.next(1); // duplicate — skipped
    relay.next(2);

    expect(sink.values).toEqual([0, 1, 2]);
  });

  it('deep equality on upstream objects', () => {
    const relay = createRelay<{ x: number }>();
    const sig = toSignal({ initial: { x: 0 } })(relay);

    const sink = recordSink<{ x: number }>();
    const stream = fromSignal(sig).connect(sink);
    stream.resume();

    relay.next({ x: 1 });
    relay.next({ x: 1 }); // deeply equal — skipped

    expect(sink.values).toEqual([{ x: 0 }, { x: 1 }]);
  });

  it('connects to upstream on first subscriber', () => {
    let connected = false;
    const source: Source<number> = {
      connect(sink) {
        connected = true;
        sink.next(1);
        sink.complete();
        return { resume() {}, [Symbol.dispose]() {} };
      },
    };

    const sig = toSignal({ initial: 0 })(source);
    expect(connected).toBe(false);

    const sink = recordSink<number>();
    const stream = fromSignal(sig).connect(sink);
    stream.resume();
    expect(connected).toBe(true);
  });

  it('disconnects upstream when last subscriber disposes', () => {
    let disposed = false;
    const source: Source<number> = {
      connect() {
        return {
          resume() {},
          [Symbol.dispose]() {
            disposed = true;
          },
        };
      },
    };

    const sig = toSignal({ initial: 0 })(source);
    const sink = recordSink<number>();
    const stream = fromSignal(sig).connect(sink);
    stream.resume();

    expect(disposed).toBe(false);
    stream[Symbol.dispose]();
    expect(disposed).toBe(true);
  });

  it('reconnects on new subscriber after disconnect', () => {
    let connectCount = 0;
    const source: Source<number> = {
      connect(_sink) {
        connectCount++;
        return { resume() {}, [Symbol.dispose]() {} };
      },
    };

    const sig = toSignal({ initial: 0 })(source);

    // First subscriber
    const s1 = fromSignal(sig).connect(recordSink());
    s1.resume();
    expect(connectCount).toBe(1);
    s1[Symbol.dispose](); // disconnect

    // Second subscriber triggers reconnect
    const s2 = fromSignal(sig).connect(recordSink());
    s2.resume();
    expect(connectCount).toBe(2);
    s2[Symbol.dispose]();
  });

  it('upstream completion keeps last value', () => {
    const relay = createRelay<number>();
    const sig = toSignal({ initial: 0 })(relay);

    const sink = recordSink<number>();
    const stream = fromSignal(sig).connect(sink);
    stream.resume();

    relay.next(42);
    relay.complete();

    expect(sig()).toBe(42);

    // New subscriber still gets the last value
    const sink2 = recordSink<number>();
    const s2 = fromSignal(sig).connect(sink2);
    s2.resume();
    expect(sink2.values).toEqual([42]);
    s2[Symbol.dispose]();
    stream[Symbol.dispose]();
  });

  it('multiple subscribers share upstream', () => {
    let connectCount = 0;
    const relay = createRelay<number>();
    const wrappedSource: Source<number> = {
      connect(sink) {
        connectCount++;
        return relay.connect(sink);
      },
    };

    const sig = toSignal({ initial: 0 })(wrappedSource);

    const sink1 = recordSink<number>();
    const sink2 = recordSink<number>();
    const s1 = fromSignal(sig).connect(sink1);
    const s2 = fromSignal(sig).connect(sink2);
    s1.resume();
    s2.resume();

    expect(connectCount).toBe(1); // shared

    relay.next(1);
    expect(sink1.values).toEqual([0, 1]);
    expect(sink2.values).toEqual([0, 1]);

    s1[Symbol.dispose]();
    s2[Symbol.dispose]();
  });

  it('works with pipe + scan', () => {
    const relay = createRelay<number>();
    const sig = toSignal({ initial: 0 })(
      pipe(relay, scan((acc, n) => acc + n, 0)),
    );

    const sink = recordSink<number>();
    const stream = fromSignal(sig).connect(sink);
    stream.resume();

    relay.next(1); // acc = 1
    relay.next(2); // acc = 3
    relay.next(3); // acc = 6

    expect(sink.values).toEqual([0, 1, 3, 6]);
    expect(sig()).toBe(6);
    stream[Symbol.dispose]();
  });

  it('custom equals option', () => {
    const relay = createRelay<{ x: number }>();
    // Reference equality — same-content objects are different
    const sig = toSignal({ initial: { x: 0 }, equals: Object.is })(relay);

    const sink = recordSink<{ x: number }>();
    const stream = fromSignal(sig).connect(sink);
    stream.resume();

    relay.next({ x: 1 });
    relay.next({ x: 1 }); // different reference → emits

    expect(sink.values).toEqual([{ x: 0 }, { x: 1 }, { x: 1 }]);
    stream[Symbol.dispose]();
  });

  // --- keepAlive ---

  describe('keepAlive', () => {
    beforeEach(() => vi.useFakeTimers());
    afterEach(() => vi.useRealTimers());

    it('keepAlive: number delays disconnect', () => {
      let disposed = false;
      const source: Source<number> = {
        connect() {
          return {
            resume() {},
            [Symbol.dispose]() {
              disposed = true;
            },
          };
        },
      };

      const sig = toSignal({ initial: 0, keepAlive: 1000 })(source);
      const sink = recordSink<number>();
      const stream = fromSignal(sig).connect(sink);
      stream.resume();

      stream[Symbol.dispose]();
      expect(disposed).toBe(false); // grace period

      vi.advanceTimersByTime(500);
      expect(disposed).toBe(false);

      vi.advanceTimersByTime(500);
      expect(disposed).toBe(true);
    });

    it('keepAlive: new subscriber during grace period cancels disconnect', () => {
      let connectCount = 0;
      let disposed = false;
      const source: Source<number> = {
        connect() {
          connectCount++;
          return {
            resume() {},
            [Symbol.dispose]() {
              disposed = true;
            },
          };
        },
      };

      const sig = toSignal({ initial: 0, keepAlive: 1000 })(source);

      const s1 = fromSignal(sig).connect(recordSink());
      s1.resume();
      s1[Symbol.dispose](); // starts grace period

      vi.advanceTimersByTime(500);
      expect(disposed).toBe(false);

      const s2 = fromSignal(sig).connect(recordSink());
      s2.resume();

      vi.advanceTimersByTime(1000); // grace period would have expired
      expect(disposed).toBe(false); // but was cancelled
      expect(connectCount).toBe(1); // still same upstream connection

      s2[Symbol.dispose]();
      vi.advanceTimersByTime(1000);
      expect(disposed).toBe(true); // now disconnects
    });

    it('keepAlive: true never disconnects', () => {
      let disposed = false;
      const source: Source<number> = {
        connect() {
          return {
            resume() {},
            [Symbol.dispose]() {
              disposed = true;
            },
          };
        },
      };

      const sig = toSignal({ initial: 0, keepAlive: true })(source);
      const s1 = fromSignal(sig).connect(recordSink());
      s1.resume();
      s1[Symbol.dispose]();

      vi.advanceTimersByTime(100_000);
      expect(disposed).toBe(false);
    });
  });
});

// ---------------------------------------------------------------------------
// Cross-scheduler flush — reproduces the bug where scheduler B's subscribers
// are never notified when scheduler A's flush bumps globalEpoch via a side-effect
// ---------------------------------------------------------------------------

describe('cross-scheduler flush', () => {
  it('second scheduler delivers when first flush callback bumps globalEpoch', () => {
    // Controllable schedulers: capture the flush callback so we decide order
    let flushX: (() => void) | null = null;
    let flushY: (() => void) | null = null;
    const schedulerX: Scheduler = { schedule(cb) { flushX = cb; } };
    const schedulerY: Scheduler = { schedule(cb) { flushY = cb; } };

    const a = createSignal(1);
    const b = computed(() => a() * 2);
    // Unrelated signal — its set() bumps globalEpoch without affecting b
    const unrelated = createSignal(0);

    const xValues: number[] = [];
    const yValues: number[] = [];

    b.observe('value', (v) => {
      xValues.push(v);
      // Side-effect during X's delivery: bump globalEpoch
      unrelated.set(unrelated() + 1);
    }, schedulerX);

    b.observe('value', (v) => {
      yValues.push(v);
    }, schedulerY);

    // Initial delivery is synchronous for both observers
    expect(xValues).toEqual([2]);
    expect(yValues).toEqual([2]);

    // Mutate the source signal
    a.set(2);

    // Flush scheduler X first (simulates microtask firing before rAF)
    expect(flushX).not.toBeNull();
    flushX!();
    expect(xValues).toEqual([2, 4]);

    // Flush scheduler Y second — this is where the cross-scheduler bug was:
    // old code saw version already bumped by X's flush → stopped propagation
    expect(flushY).not.toBeNull();
    flushY!();
    expect(yValues).toEqual([2, 4]);
  });

  it('second scheduler delivers even without epoch-bumping side-effects', () => {
    let flushA: (() => void) | null = null;
    let flushB: (() => void) | null = null;
    const schedA: Scheduler = { schedule(cb) { flushA = cb; } };
    const schedB: Scheduler = { schedule(cb) { flushB = cb; } };

    const src = createSignal(10);
    const derived = computed(() => src() + 1);

    const aValues: number[] = [];
    const bValues: number[] = [];

    derived.observe('value', (v) => aValues.push(v), schedA);
    derived.observe('value', (v) => bValues.push(v), schedB);

    expect(aValues).toEqual([11]);
    expect(bValues).toEqual([11]);

    src.set(20);
    flushA!();
    expect(aValues).toEqual([11, 21]);

    flushB!();
    expect(bValues).toEqual([11, 21]);
  });

  it('multiple changes: each scheduler sees every committed value', () => {
    let flushA: (() => void) | null = null;
    let flushB: (() => void) | null = null;
    const schedA: Scheduler = { schedule(cb) { flushA = cb; } };
    const schedB: Scheduler = { schedule(cb) { flushB = cb; } };

    const src = createSignal(0);
    const derived = computed(() => src() * 3);
    const unrelated = createSignal(0);

    const aValues: number[] = [];
    const bValues: number[] = [];

    derived.observe('value', (v) => {
      aValues.push(v);
      unrelated.set(unrelated() + 1); // bump epoch on each delivery
    }, schedA);

    derived.observe('value', (v) => bValues.push(v), schedB);

    expect(aValues).toEqual([0]);
    expect(bValues).toEqual([0]);

    // First change
    src.set(1);
    flushA!();
    flushB!();
    expect(aValues).toEqual([0, 3]);
    expect(bValues).toEqual([0, 3]);

    // Second change
    src.set(2);
    flushA!();
    flushB!();
    expect(aValues).toEqual([0, 3, 6]);
    expect(bValues).toEqual([0, 3, 6]);
  });
});

// ---------------------------------------------------------------------------
// Liveness: a plain read registers nothing
// ---------------------------------------------------------------------------

describe('unobserved computed reads', () => {
  it('a plain read of a computed does not make its dependencies observed', () => {
    const a = createSignal(1);
    let activations = 0;
    a.observe('activate', () => activations++);
    const c = computed(() => a() * 2);

    expect(c()).toBe(2);
    expect(a.observed).toBe(false);
    expect(activations).toBe(0);

    // Still correct on the pull path
    a.set(5);
    expect(c()).toBe(10);
  });

  it('a plain read of a computed does not connect a toSignal upstream', () => {
    let connects = 0;
    let disposes = 0;
    const src = {
      connect(sink: { next(v: number): unknown }) {
        connects++;
        return { resume() { sink.next(5); }, [Symbol.dispose]() { disposes++; } };
      },
    };
    const s = toSignal<number>({ initial: 0 })(src);
    const c = computed(() => s() + 1);
    expect(c()).toBe(1);
    expect(connects).toBe(0);

    const stop = c.observe('value', () => {});
    expect(connects).toBe(1);
    expect(c()).toBe(6);
    stop();
    expect(disposes).toBe(1);
  });

  it('observing a computed chain activates every level and deactivates on stop', () => {
    const a = createSignal(1);
    const b = computed(() => a() + 1);
    const c = computed(() => b() + 1);
    const seen: number[] = [];
    expect(c()).toBe(3);
    expect(a.observed).toBe(false);
    expect(b.observed).toBe(false);

    const stop = c.observe('value', (v) => seen.push(v));
    expect(a.observed).toBe(true);
    expect(b.observed).toBe(true);
    a.set(10);
    expect(seen).toEqual([3, 12]);

    stop();
    expect(a.observed).toBe(false);
    expect(b.observed).toBe(false);
    a.set(20);
    expect(seen).toEqual([3, 12]);
    expect(c()).toBe(22);
  });
});

// ---------------------------------------------------------------------------
// Flush robustness
// ---------------------------------------------------------------------------

describe('errors during delivery', () => {
  it('a throwing observer does not starve its siblings', () => {
    const a = createSignal(0);
    const seen: number[] = [];
    a.observe('value', (v) => { if (v === 1) throw new Error('boom'); });
    a.observe('value', (v) => seen.push(v));

    expect(() => a.set(1)).toThrow('boom');
    expect(seen).toEqual([0, 1]);
  });

  it('several throwing observers are reported together', () => {
    const a = createSignal(0);
    a.observe('value', (v) => { if (v) throw new Error('one'); });
    a.observe('value', (v) => { if (v) throw new Error('two'); });
    let caught: unknown;
    try { a.set(1); } catch (e) { caught = e; }
    expect(caught).toBeInstanceOf(AggregateError);
    expect((caught as AggregateError).errors.map((e) => (e as Error).message)).toEqual(['one', 'two']);
  });

  it('a throwing computed does not block delivery to other subscribers, and is re-evaluated on read', () => {
    const a = createSignal(0);
    const bad = computed(() => { if (a() === 1) throw new Error('compute boom'); return a(); });
    const good = computed(() => a() * 10);
    const seenGood: number[] = [];
    bad.observe('value', () => {});
    good.observe('value', (v) => seenGood.push(v));

    expect(() => a.set(1)).toThrow('compute boom');
    expect(seenGood).toEqual([0, 10]);
    expect(() => bad()).toThrow('compute boom');

    a.set(2);
    expect(bad()).toBe(2);
    expect(seenGood).toEqual([0, 10, 20]);
  });
});

// ---------------------------------------------------------------------------
// Re-entrancy during activation
// ---------------------------------------------------------------------------

describe('set() inside activate callbacks', () => {
  it('a set() during activation is reflected in the computed that triggered it', () => {
    const a = createSignal(1);
    const b = createSignal(10);
    a.observe('activate', () => b.set(20));
    const c = computed(() => a() + b());
    const seen: number[] = [];
    c.observe('value', (v) => seen.push(v));
    expect(c()).toBe(21);
    expect(seen[seen.length - 1]).toBe(21);
  });

  it('an observed computed over a toSignal that emits synchronously on connect sees the final value', () => {
    const s = toSignal<number>({ initial: 0 })(fromArray([1, 2, 3]));
    const c = computed(() => s() * 2);
    const seen: number[] = [];
    const stop = c.observe('value', (v) => seen.push(v));
    expect(c()).toBe(6);
    expect(seen[seen.length - 1]).toBe(6);
    stop();
    expect(s.observed).toBe(false);
  });

  it('does not leak scheduler refs when activation re-enters evaluation', () => {
    const a = createSignal(1);
    const b = createSignal(1);
    a.observe('activate', () => b.set(2));
    const c = computed(() => a() + b());
    const stop = c.observe('value', () => {});
    stop();
    // With everything unsubscribed, a write must not reach any observer
    let delivered = 0;
    const stop2 = c.observe('value', () => delivered++);
    stop2();
    a.set(5);
    b.set(5);
    expect(delivered).toBe(1);
    expect(a.observed).toBe(false);
    expect(b.observed).toBe(false);
  });
});

describe('throwing computed on the read path', () => {
  it('throws again on the next read instead of handing out an unset value', () => {
    let fail = true;
    const c = computed(() => { if (fail) throw new Error('nope'); return 1; });
    expect(() => c()).toThrow('nope');
    expect(() => c()).toThrow('nope');
    fail = false;
    expect(c()).toBe(1);
  });
});

describe('scheduled delivery', () => {
  it('a coalescing scheduler never delivers a value equal to the last delivered one', async () => {
    const b = createSignal(0);
    const seen: number[] = [];
    b.observe('value', (v) => seen.push(v), microtaskScheduler);
    b.set(1);
    b.set(0);
    await Promise.resolve();
    expect(seen).toEqual([0]);
  });

  it('microtaskScheduler runs every callback scheduled in a tick', async () => {
    const order: string[] = [];
    microtaskScheduler.schedule(() => order.push('a'));
    microtaskScheduler.schedule(() => order.push('b'));
    await Promise.resolve();
    expect(order).toEqual(['a', 'b']);
  });
});
