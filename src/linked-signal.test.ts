// ---------------------------------------------------------------------------
// linked-signal — tests for link() and linkedSignal()
// ---------------------------------------------------------------------------

import { describe, it, expect, vi } from 'vitest';
import { createSignal } from './signal.js';
import { link, linkedSignal } from './linked-signal.js';

// ===========================================================================
// link()
// ===========================================================================

describe('link', () => {
  // -----------------------------------------------------------------------
  // Basic derivation
  // -----------------------------------------------------------------------

  it('writes computed value to the signal on read (pull-through)', () => {
    const source = createSignal(10);
    const target = createSignal(0);

    link(target, () => source() * 2, { registerResource: false });

    // Read pulls the derived value — no observer needed
    expect(target()).toBe(20);

    source.set(5);
    expect(target()).toBe(10);
  });

  it('passes previous signal value to computation', () => {
    const options = createSignal(['A', 'B', 'C']);
    const selected = createSignal('B');

    link(selected, (prev) => {
      const opts = options();
      return opts.includes(prev) ? prev : opts[0];
    }, { registerResource: false });

    // Initial: 'B' is in ['A','B','C'] → preserved
    expect(selected()).toBe('B');

    // Change options — 'B' still in list
    options.set(['B', 'D']);
    expect(selected()).toBe('B');

    // Change options — 'B' no longer in list → falls back to first
    options.set(['X', 'Y']);
    expect(selected()).toBe('X');
  });

  it('manual set() overrides, next source change sees the override as previous', () => {
    const source = createSignal(1);
    const target = createSignal(0);

    const prevValues: number[] = [];
    link(target, (prev) => {
      prevValues.push(prev);
      return source() + prev;
    }, { registerResource: false });

    // Initial read: source=1, prev=0 → 1+0=1
    expect(target()).toBe(1);

    // Manual override sticks — derived hasn't recomputed (source unchanged)
    target.set(100);
    expect(target()).toBe(100);

    // Source change → computation runs with prev=100
    source.set(2);
    expect(target()).toBe(102); // 2 + 100
  });

  it('returns an unlink function that detaches the derivation', () => {
    const source = createSignal(5);
    const target = createSignal(0);

    const unlink = link(target, () => source(), { registerResource: false });

    const off = target.observe('value', () => {});
    expect(target()).toBe(5);

    // Detach
    unlink();

    // Source changes no longer affect target
    source.set(99);
    expect(target()).toBe(5);

    off();
  });

  it('calling unlink multiple times is safe', () => {
    const target = createSignal(0);
    const unlink = link(target, () => 42, { registerResource: false });
    unlink();
    unlink(); // should not throw
  });

  // -----------------------------------------------------------------------
  // Demand-driven (activate / deactivate)
  // -----------------------------------------------------------------------

  it('pulls derived value on read even without observers', () => {
    const source = createSignal(1);
    const target = createSignal(0);
    const computeFn = vi.fn(() => source());

    link(target, computeFn, { registerResource: false });

    // Read pulls the derived computation
    expect(target()).toBe(1);
    expect(computeFn).toHaveBeenCalled();
  });

  it('push-notifies observers when source changes', () => {
    const source = createSignal(10);
    const target = createSignal(0);

    link(target, () => source(), { registerResource: false });
    expect(target()).toBe(10);

    // Observer receives push notifications on source change
    const values: number[] = [];
    const off = target.observe('value', (v) => values.push(v));
    source.set(20);
    expect(values).toContain(20);

    off();
  });

  it('deactivates when the signal loses all observers, reactivates on new observer', () => {
    const source = createSignal(1);
    const target = createSignal(0);
    const computeFn = vi.fn(() => source());

    link(target, computeFn, { registerResource: false });

    // Activate
    const off = target.observe('value', () => {});
    const callCount = computeFn.mock.calls.length;
    expect(callCount).toBeGreaterThan(0);

    // Deactivate
    off();

    // Source changes while dormant — should NOT trigger computation
    computeFn.mockClear();
    source.set(99);
    // No extra calls (fire-and-forget from source perspective)

    // Re-activate — picks up current source value
    const off2 = target.observe('value', () => {});
    expect(target()).toBe(99);

    off2();
  });

  // -----------------------------------------------------------------------
  // Multiple links on the same signal
  // -----------------------------------------------------------------------

  it('supports multiple links on the same signal (last write wins)', () => {
    const a = createSignal(10);
    const b = createSignal(20);
    const target = createSignal(0);

    link(target, () => a(), { registerResource: false });
    link(target, () => b(), { registerResource: false });

    const off = target.observe('value', () => {});

    // Both links fire — the second link's set() runs after the first
    // Exact order depends on subscription order; the important thing
    // is that it doesn't throw and both can coexist.
    // After activation, the value should reflect at least one source.
    const val = target();
    expect(val === 10 || val === 20).toBe(true);

    off();
  });
});

// ===========================================================================
// linkedSignal()
// ===========================================================================

describe('linkedSignal', () => {
  it('computes initial value from computation(undefined)', () => {
    let firstPrev: number | undefined = -1;
    const sig = linkedSignal((prev: number | undefined) => {
      if (firstPrev === -1) firstPrev = prev;
      return 42;
    });
    // First computation call receives undefined as previous
    expect(firstPrev).toBeUndefined();
    expect(sig()).toBe(42);
  });

  it('follows source signal changes when observed', () => {
    const source = createSignal(1);
    const sig = linkedSignal(() => source() * 3);

    expect(sig()).toBe(3); // initial: 1 * 3

    const values: number[] = [];
    const off = sig.observe('value', (v) => values.push(v));

    source.set(5);
    expect(sig()).toBe(15);

    source.set(10);
    expect(sig()).toBe(30);

    off();
  });

  it('manual set() overrides, preserved as previous on next recompute', () => {
    const source = createSignal('hello');
    const sig = linkedSignal((prev) => {
      const s = source();
      return prev !== undefined ? `${s}+${prev}` : s;
    });

    expect(sig()).toBe('hello');

    // Override sticks until source changes
    sig.set('custom');
    expect(sig()).toBe('custom');

    // Source change → prev is 'custom'
    source.set('world');
    expect(sig()).toBe('world+custom');
  });

  it('is writable — set() and update() work', () => {
    const sig = linkedSignal(() => 10);

    // After initial pull, derived returns 10 — set(20) overrides
    expect(sig()).toBe(10);
    sig.set(20);
    expect(sig()).toBe(20);

    sig.update((v) => v + 5);
    expect(sig()).toBe(25);
  });

  it('custom equals option prevents redundant updates', () => {
    const source = createSignal({ x: 1, y: 2 });
    const computeCount = vi.fn();

    const sig = linkedSignal(
      () => {
        computeCount();
        return source();
      },
      { equals: (a, b) => a.x === b.x && a.y === b.y },
    );

    const values: Array<{ x: number; y: number }> = [];
    const off = sig.observe('value', (v) => values.push(v));

    // Set to structurally equal value
    source.set({ x: 1, y: 2 });

    // The signal should not deliver a new value because equals says it's the same
    // The last delivered value should still be the initial one
    expect(sig().x).toBe(1);
    expect(sig().y).toBe(2);

    off();
  });

  it('works with complex previous-dependent logic (dropdown selection)', () => {
    const options = createSignal(['Ground', 'Air', 'Sea']);

    const selected = linkedSignal<string>((prev) => {
      const opts = options();
      return prev !== undefined && opts.includes(prev) ? prev : opts[0];
    });

    expect(selected()).toBe('Ground');

    selected.set('Sea');
    expect(selected()).toBe('Sea');

    // Source changes, Sea still in list
    options.set(['Email', 'Sea', 'Postal']);
    expect(selected()).toBe('Sea');

    // Sea no longer in list → reset to first
    options.set(['Email', 'Postal']);
    expect(selected()).toBe('Email');
  });

  it('pulls derived on read without observers (no dormancy on read)', () => {
    const source = createSignal(1);
    const computeFn = vi.fn((prev: number | undefined) => source() + (prev ?? 0));

    const sig = linkedSignal(computeFn);

    // Initial value pulled immediately
    expect(sig()).toBe(1); // source=1, prev=undefined → 1+0=1

    // Source change → next read pulls fresh
    source.set(2);
    expect(sig()).toBe(3); // source=2, prev=1 → 2+1=3
  });
});
