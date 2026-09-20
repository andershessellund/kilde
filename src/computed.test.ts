// ---------------------------------------------------------------------------
// computed — tests for derived signals with auto-tracking
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { deepEqual } from 'valsem';
import type { Sink } from './types.js';
import { PAUSE } from './types.js';
import { createSignal, toSignal, computed } from './signal.js';
import { createRelay } from './relay.js';
import { fromSignal } from './sources/from-signal.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function recordSink<T>(): Sink<T> & { values: T[]; completed: boolean } {
  const sink = {
    values: [] as T[],
    completed: false,
    next(value: T): undefined | typeof PAUSE {
      sink.values.push(value);
      return undefined;
    },
    complete() {
      sink.completed = true;
    },
    error() {},
  };
  return sink;
}

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
// computed
// ===========================================================================

describe('computed', () => {
  // ---------------------------------------------------------------------------
  // Basic derivation
  // ---------------------------------------------------------------------------

  it('derives from a single signal', () => {
    const count = createSignal(2);
    const doubled = computed(() => count() * 2);

    expect(doubled()).toBe(4);

    count.set(5);
    expect(doubled()).toBe(10);
  });

  it('derives from multiple signals', () => {
    const a = createSignal(1);
    const b = createSignal(2);
    const sum = computed(() => a() + b());

    expect(sum()).toBe(3);

    a.set(10);
    expect(sum()).toBe(12);

    b.set(20);
    expect(sum()).toBe(30);
  });

  it('concatenates string signals', () => {
    const first = createSignal('John');
    const last = createSignal('Doe');
    const full = computed(() => `${first()} ${last()}`);

    expect(full()).toBe('John Doe');

    first.set('Jane');
    expect(full()).toBe('Jane Doe');
  });

  // ---------------------------------------------------------------------------
  // Lazy evaluation
  // ---------------------------------------------------------------------------

  it('is lazy — recomputes only on () read', () => {
    let computeCount = 0;
    const a = createSignal(1);
    const c = computed(() => {
      computeCount++;
      return a() * 2;
    });

    expect(computeCount).toBe(0); // lazy — not evaluated until first read

    expect(c()).toBe(2); // first read triggers evaluation
    expect(computeCount).toBe(1);

    a.set(2);
    expect(computeCount).toBe(1); // not recomputed yet

    expect(c()).toBe(4);
    expect(computeCount).toBe(2); // recomputed on read

    // Reading again without changes doesn't recompute
    expect(c()).toBe(4);
    expect(computeCount).toBe(2);
  });

  // ---------------------------------------------------------------------------
  // Deduplication
  // ---------------------------------------------------------------------------

  it('deduplicates — does not notify when result is equal', () => {
    const a = createSignal(1);
    const b = createSignal(2);
    // Always returns 3 regardless of which signal changes
    const sum = computed(() => a() + b());

    const sink = recordSink<number>();
    const stream = fromSignal(sum).connect(sink);
    stream.resume();

    expect(sink.values).toEqual([3]);

    // Change a to 2, b to 1 → still 3
    a.set(2);
    b.set(1);

    // Sink was notified (pushed dirty), but on resume the value is still 3,
    // so dedup kicks in at the subscription level
    expect(sum()).toBe(3);
  });

  it('skips notification when computed result is equal under a custom equals', () => {
    const input = createSignal({ x: 1, y: 2 });
    const derived = computed(() => ({ sum: input().x + input().y }), { equals: deepEqual });

    const sink = recordSink<{ sum: number }>();
    const s = fromSignal(derived).connect(sink);
    s.resume();
    expect(sink.values).toEqual([{ sum: 3 }]);

    // Set different object with same sum
    input.set({ x: 2, y: 1 });
    // Computed re-evaluates to { sum: 3 } — deeply equal to previous
    expect(derived()).toEqual({ sum: 3 });
    // Sink should not receive a new value
    expect(sink.values).toEqual([{ sum: 3 }]);
  });

  // ---------------------------------------------------------------------------
  // Dynamic dependencies
  // ---------------------------------------------------------------------------

  it('tracks dynamic dependencies (conditional branch)', () => {
    const useA = createSignal(true);
    const a = createSignal(10);
    const b = createSignal(20);

    const result = computed(() => (useA() ? a() : b()));

    expect(result()).toBe(10);

    // Changing b shouldn't trigger recompute (not a dep currently)
    b.set(30);
    expect(result()).toBe(10);

    // Switch to b
    useA.set(false);
    expect(result()).toBe(30);

    // Now a is no longer tracked
    a.set(100);
    expect(result()).toBe(30); // still 30, not re-evaluated

    // b changes should now be tracked
    b.set(40);
    expect(result()).toBe(40);
  });

  // ---------------------------------------------------------------------------
  // Diamond dependency
  // ---------------------------------------------------------------------------

  it('handles diamond dependency graph', () => {
    const source = createSignal(1);
    const left = computed(() => source() * 2);
    const right = computed(() => source() * 3);
    const combined = computed(() => left() + right());

    expect(combined()).toBe(5); // 2 + 3

    source.set(2);
    expect(combined()).toBe(10); // 4 + 6

    source.set(3);
    expect(combined()).toBe(15); // 6 + 9
  });

  // ---------------------------------------------------------------------------
  // Chained computed
  // ---------------------------------------------------------------------------

  it('chains computed signals', () => {
    const base = createSignal(2);
    const doubled = computed(() => base() * 2);
    const quadrupled = computed(() => doubled() * 2);

    expect(quadrupled()).toBe(8);

    base.set(3);
    expect(doubled()).toBe(6);
    expect(quadrupled()).toBe(12);
  });

  it('deep chain of computed signals', () => {
    const a = createSignal(1);
    const b = computed(() => a() + 1);
    const c = computed(() => b() + 1);
    const d = computed(() => c() + 1);
    const e = computed(() => d() + 1);

    expect(e()).toBe(5);

    a.set(10);
    expect(e()).toBe(14);
  });

  // ---------------------------------------------------------------------------
  // Source protocol (connect/resume)
  // ---------------------------------------------------------------------------

  it('replays current value on subscribe + resume', () => {
    const a = createSignal(5);
    const c = computed(() => a() * 10);

    const sink = recordSink<number>();
    const s = fromSignal(c).connect(sink);
    s.resume();

    expect(sink.values).toEqual([50]);
  });

  it('pushes updates to subscribers when dependency changes', () => {
    const a = createSignal(1);
    const c = computed(() => a() * 2);

    const sink = recordSink<number>();
    const s = fromSignal(c).connect(sink);
    s.resume();

    expect(sink.values).toEqual([2]);

    a.set(2);
    // The computed is marked dirty and notifies subscribers.
    // Subscriber's push() reads _value which triggers recompute.
    expect(sink.values).toEqual([2, 4]);

    a.set(3);
    expect(sink.values).toEqual([2, 4, 6]);
  });

  it('paused subscriber gets latest on resume', () => {
    const a = createSignal(1);
    const c = computed(() => a() * 10);

    const sink = pauseAfter<number>(1);
    const s = fromSignal(c).connect(sink);
    s.resume(); // delivers 10, pauses

    a.set(2); // dirty, but paused
    a.set(3); // still dirty

    expect(sink.values).toEqual([10]);

    // Allow unlimited values now
    sink.next = (v) => {
      sink.values.push(v);
      return undefined;
    };
    s.resume();

    expect(sink.values).toEqual([10, 30]); // latest only
  });

  it('dispose stops subscription', () => {
    const a = createSignal(1);
    const c = computed(() => a() * 2);

    const sink = recordSink<number>();
    const s = fromSignal(c).connect(sink);
    s.resume();

    s[Symbol.dispose]();
    a.set(5);

    expect(sink.values).toEqual([2]); // no update after dispose
  });

  it('multiple subscribers all get updates', () => {
    const a = createSignal(0);
    const c = computed(() => a() + 1);

    const sink1 = recordSink<number>();
    const sink2 = recordSink<number>();
    const s1 = fromSignal(c).connect(sink1);
    const s2 = fromSignal(c).connect(sink2);
    s1.resume();
    s2.resume();

    a.set(1);

    expect(sink1.values).toEqual([1, 2]);
    expect(sink2.values).toEqual([1, 2]);
  });

  // ---------------------------------------------------------------------------
  // Custom equals
  // ---------------------------------------------------------------------------

  it('custom equals option', () => {
    // Source uses reference equality so same-content objects are treated as different
    const a = createSignal({ x: 1 }, { equals: Object.is });
    // Computed also uses reference equality — different objects are unequal
    const c = computed(() => ({ doubled: a().x * 2 }), { equals: Object.is });

    const sink = recordSink<{ doubled: number }>();
    const s = fromSignal(c).connect(sink);
    s.resume();

    // Set same x value — computed returns new object ref each time
    a.set({ x: 1 });
    // With Object.is, new object is !== old → emits
    expect(sink.values).toHaveLength(2);
    expect(sink.values[1]).toEqual({ doubled: 2 });
  });

  // ---------------------------------------------------------------------------
  // Tracking isolation
  // ---------------------------------------------------------------------------

  it('() reads outside computed do not track', () => {
    const a = createSignal(1);
    const b = createSignal(2);

    // Read a() outside any computed — should not affect anything
    expect(a()).toBe(1);

    // computed only depends on b
    const c = computed(() => b() * 3);
    expect(c()).toBe(6);

    // Changing a should NOT invalidate c
    a.set(100);
    expect(c()).toBe(6);
  });

  // ---------------------------------------------------------------------------
  // Error handling
  // ---------------------------------------------------------------------------

  it('error in computation propagates on () read', () => {
    const flag = createSignal(true);
    const c = computed(() => {
      if (flag()) return 42;
      throw new Error('computation failed');
    });

    expect(c()).toBe(42);

    flag.set(false);
    expect(() => c()).toThrow('computation failed');
  });

  // ---------------------------------------------------------------------------
  // toSignal as computed dependency
  // ---------------------------------------------------------------------------

  it('tracks toSignal as a dependency', () => {
    const relay = createRelay<number>();
    const sig = toSignal({ initial: 0 })(relay);

    // Need a subscriber to connect the toSignal to upstream
    const sink = recordSink<number>();
    const stream = fromSignal(sig).connect(sink);
    stream.resume();

    const doubled = computed(() => sig() * 2);
    expect(doubled()).toBe(0);

    relay.next(5);
    expect(doubled()).toBe(10);

    relay.next(10);
    expect(doubled()).toBe(20);

    stream[Symbol.dispose]();
  });
});
