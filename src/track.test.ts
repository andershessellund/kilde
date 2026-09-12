// ---------------------------------------------------------------------------
// track() — tests for dependency tracking + dirty notification
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Sink } from './types.js';
import { createSignal, computed, toSignal, track } from './signal.js';
import { createRelay } from './relay.js';
import { fromSignal } from './sources/from-signal.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Creates a Dependent that counts _markDirty() calls. */
function dirtyCounter() {
  const dep = {
    count: 0,
    _markDirty() { dep.count++; },
  };
  return dep;
}

function recordSink<T>(): Sink<T> & { values: T[]; completed: boolean } {
  return {
    values: [] as T[],
    completed: false,
    next(value: T): undefined {
      this.values.push(value);
      return undefined;
    },
    complete() {
      this.completed = true;
    },
    error() {},
  };
}

// ===========================================================================
// track()
// ===========================================================================

describe('track', () => {
  it('runIfDirty() captures deps and returns the result', () => {
    const a = createSignal(1);
    const b = createSignal(2);
    const ctrl = track(() => a() + b());

    expect(ctrl.runIfDirty()).toBe(3);
  });

  it('runIfDirty() returns cached result when deps unchanged', () => {
    let callCount = 0;
    const a = createSignal(1);
    const ctrl = track(() => {
      callCount++;
      return a() * 2;
    });

    ctrl.runIfDirty(); // first evaluation
    callCount = 0;

    const result = ctrl.runIfDirty(); // should skip via version check
    expect(result).toBe(2);
    expect(callCount).toBe(0);
  });

  it('runIfDirty() re-evaluates when deps changed', () => {
    let callCount = 0;
    const a = createSignal(1);
    const ctrl = track(() => {
      callCount++;
      return a() * 2;
    });

    ctrl.runIfDirty();
    callCount = 0;

    a.set(5);
    const result = ctrl.runIfDirty();
    expect(result).toBe(10);
    expect(callCount).toBe(1);
  });

  it('notifies dependents when a dependency changes', () => {
    const a = createSignal(1);
    const ctrl = track(() => a() * 2);
    ctrl.runIfDirty(); // capture deps

    const dep = dirtyCounter();
    ctrl._addDependent(dep);

    a.set(2);
    expect(dep.count).toBe(1);

    ctrl.runIfDirty(); // clear dirty flag so next change can re-notify
    a.set(3);
    expect(dep.count).toBe(2);

    ctrl._removeDependent(dep);
  });

  it('does not notify when value is set to same (equality check)', () => {
    const a = createSignal(1);
    const ctrl = track(() => a());
    ctrl.runIfDirty();

    const dep = dirtyCounter();
    ctrl._addDependent(dep);

    a.set(1); // same value — signal's own equality check prevents _version bump
    expect(dep.count).toBe(0);

    ctrl._removeDependent(dep);
  });

  it('does not register on deps until it has a dependent', () => {
    const a = createSignal(1);
    const ctrl = track(() => a());

    expect(a.observed).toBe(false);
    ctrl.runIfDirty();

    // A plain evaluation pulls; it does not register (nothing is watching)
    expect(a.observed).toBe(false);

    const dep = dirtyCounter();
    ctrl._addDependent(dep);
    expect(a.observed).toBe(true);
    ctrl._removeDependent(dep);
    expect(a.observed).toBe(false);
  });

  it('registers as dependent when it has dependents', () => {
    const a = createSignal(1);
    const ctrl = track(() => a());
    ctrl.runIfDirty();

    const dep = dirtyCounter();
    ctrl._addDependent(dep);

    expect(a.observed).toBe(true);

    ctrl._removeDependent(dep);
    expect(a.observed).toBe(false);
  });

  it('fires dirty on re-activate if deps changed while deactivated', () => {
    const a = createSignal(1);
    const ctrl = track(() => a());
    ctrl.runIfDirty(); // evaluate + register on deps

    // Activate then deactivate to unregister from deps
    const tempDep = dirtyCounter();
    ctrl._addDependent(tempDep);
    ctrl._removeDependent(tempDep); // deactivate → unregisters

    // Change dep while deactivated (no notification received)
    a.set(42);

    // Re-activate — should fire dirty since dep changed
    const dep = dirtyCounter();
    ctrl._addDependent(dep);
    expect(dep.count).toBe(1);

    ctrl._removeDependent(dep);
  });

  it('tracks dynamic dependencies across runIfDirty calls', () => {
    const flag = createSignal(true);
    const a = createSignal(1);
    const b = createSignal(2);
    const ctrl = track(() => flag() ? a() : b());

    ctrl.runIfDirty(); // deps: [flag, a]

    const dep = dirtyCounter();
    ctrl._addDependent(dep);

    // b changes — should NOT fire (not tracked)
    b.set(99);
    expect(dep.count).toBe(0);

    // a changes — should fire
    a.set(10);
    expect(dep.count).toBe(1);

    ctrl.runIfDirty(); // clear dirty so next change can re-notify

    // Switch branch
    flag.set(false);
    expect(dep.count).toBe(2); // flag changed

    ctrl.runIfDirty(); // now deps: [flag, b]

    // a changes — should NOT fire anymore
    dep.count = 0;
    a.set(20);
    expect(dep.count).toBe(0);

    // b changes — should fire now
    b.set(100);
    expect(dep.count).toBe(1);

    ctrl._removeDependent(dep);
  });

  it('works with computed dependencies', () => {
    const a = createSignal(1);
    const b = createSignal(2);
    const sum = computed(() => a() + b());
    const ctrl = track(() => sum() * 10);

    expect(ctrl.runIfDirty()).toBe(30);

    a.set(5);
    expect(ctrl.runIfDirty()).toBe(70);
  });

  it('version check skips recompute when computed absorbs change', () => {
    const a = createSignal(1);
    // computed that clamps to [0, 10]
    const clamped = computed(() => Math.min(10, Math.max(0, a())));

    let callCount = 0;
    const ctrl = track(() => {
      callCount++;
      return clamped() * 2;
    });

    ctrl.runIfDirty(); // clamped=1, result=2
    callCount = 0;

    // Set a to 100 — clamped still returns 10 → 1 (first time is new)
    a.set(100);
    ctrl.runIfDirty(); // clamped changed from 1 to 10
    expect(callCount).toBe(1);
    callCount = 0;

    // Set a to 200 — clamped still returns 10 → same value, version unchanged
    a.set(200);
    ctrl.runIfDirty();
    expect(callCount).toBe(0); // skipped!
  });
});

// ===========================================================================
// Version counters — diamond dependency optimization
// ===========================================================================

describe('version counters', () => {
  it('computed skips recompute when all deps absorbed the change (diamond)', () => {
    const a = createSignal(1);
    // B clamps to [0, 5], C clamps to [0, 5]
    const b = computed(() => Math.min(5, a()));
    const c = computed(() => Math.min(5, a()));

    let dCallCount = 0;
    const d = computed(() => {
      dCallCount++;
      return b() + c();
    });

    d(); // first eval: b=1, c=1, d=2
    dCallCount = 0;

    // Set a=10 — b clamps to 5, c clamps to 5: both change
    a.set(10);
    expect(d()).toBe(10);
    expect(dCallCount).toBe(1);
    dCallCount = 0;

    // Set a=20 — b still 5, c still 5: both absorb → d should skip
    a.set(20);
    expect(d()).toBe(10);
    expect(dCallCount).toBe(0); // skipped!
  });

  it('computed correctly bumps version when value changes', () => {
    const a = createSignal(1);
    const doubled = computed(() => a() * 2);

    // First eval
    doubled();

    // Chain: doubled -> tripled
    let tripledCount = 0;
    const tripled = computed(() => {
      tripledCount++;
      return doubled() * 3;
    });

    tripled(); // first eval
    tripledCount = 0;

    a.set(2);
    expect(tripled()).toBe(12); // 2*2*3
    expect(tripledCount).toBe(1);
  });
});

// ===========================================================================
// Liveness propagation — toSignal upstream activation via dependents
// ===========================================================================

describe('liveness propagation', () => {
  it('toSignal connects upstream when computed depends on it', () => {
    const relay = createRelay<number>();
    let connected = false;

    const source = {
      connect(sink: any) {
        connected = true;
        return relay.connect(sink);
      },
    };

    const sig = toSignal({ initial: 0 })(source);

    // No one cares yet
    expect(connected).toBe(false);

    // A computed reads the signal
    const doubled = computed(() => sig() * 2);

    // Just creating the computed doesn't connect (lazy)
    expect(connected).toBe(false);

    // Subscribe to the computed → computed evaluates → reads sig → registers as dependent
    const sink = recordSink<number>();
    const s = fromSignal(doubled).connect(sink);
    s.resume();

    // Now the computed has subscriptions, it evaluated and registered as dependent on sig
    // sig.observed should be true → upstream should be connected
    expect(connected).toBe(true);
    expect(sink.values).toEqual([0]); // initial value

    // Push a value through the relay
    relay.next(5);
    expect(sink.values).toEqual([0, 10]); // 5 * 2

    // Disconnect the subscriber → computed unsubscribed → sig has no dependents
    s[Symbol.dispose]();
  });

  it('toSignal disconnects upstream when last dependent leaves', () => {
    const relay = createRelay<number>();
    let disposeCount = 0;

    const source = {
      connect(sink: any) {
        const inner = relay.connect(sink);
        return {
          resume() { inner.resume(); },
          [Symbol.dispose]() {
            disposeCount++;
            inner[Symbol.dispose]();
          },
        };
      },
    };

    const sig = toSignal({ initial: 0 })(source);
    const doubled = computed(() => sig() * 2);

    const sink = recordSink<number>();
    const s = fromSignal(doubled).connect(sink);
    s.resume(); // triggers upstream connection

    expect(disposeCount).toBe(0);

    s[Symbol.dispose](); // last subscriber gone → computed unregisters → sig disconnects
    expect(disposeCount).toBe(1);
  });

  it('track() evaluation connects toSignal upstream', () => {
    const relay = createRelay<number>();
    let connected = false;

    const source = {
      connect(sink: any) {
        connected = true;
        return relay.connect(sink);
      },
    };

    const sig = toSignal({ initial: 0 })(source);
    const ctrl = track(() => sig() * 2);

    // Before evaluation: not connected
    expect(connected).toBe(false);

    // A plain evaluation reads the initial value without connecting upstream
    expect(ctrl.runIfDirty()).toBe(0);
    expect(connected).toBe(false);

    // Adding a dependent activates the tracker → toSignal connects upstream
    const dep = dirtyCounter();
    ctrl._addDependent(dep);
    expect(connected).toBe(true);

    // Push value → sig updates → track gets dirty notification
    relay.next(5);
    expect(dep.count).toBe(1);
    expect(ctrl.runIfDirty()).toBe(10);

    ctrl._removeDependent(dep);
  });
});
