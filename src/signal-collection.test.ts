// ---------------------------------------------------------------------------
// SignalDeduplicator — tests
// ---------------------------------------------------------------------------

import { describe, it, expect, vi } from 'vitest';
import { createSignal, computed } from './signal.js';
import { SignalDeduplicator } from './signal-collection.js';
import { fromSignal } from './sources/from-signal.js';

// Helper: factory that creates writable signals (tracks calls)
function trackingFactory() {
  const created = new Map<string, ReturnType<typeof createSignal<number>>>();
  const factory = (key: { id: string }) => {
    const signal = createSignal(Number(key.id));
    created.set(key.id, signal);
    return signal;
  };
  return { factory, created };
}

describe('SignalDeduplicator', () => {
  // --- Basic getOrCreate/cache ---

  it('creates a signal on first getOrCreate', () => {
    const { factory, created } = trackingFactory();
    const coll = new SignalDeduplicator<{ id: string }, number>();

    const sig = coll.getOrCreate({ id: '1' }, factory);
    expect(sig()).toBe(1);
    expect(created.size).toBe(1);
  });

  it('returns cached signal on subsequent getOrCreate with equal key', () => {
    const { factory, created } = trackingFactory();
    const coll = new SignalDeduplicator<{ id: string }, number>();

    const sig1 = coll.getOrCreate({ id: '1' }, factory);
    const sig2 = coll.getOrCreate({ id: '1' }, factory); // structurally equal key

    expect(sig1).toBe(sig2); // same reference
    expect(created.size).toBe(1); // factory called once
  });

  it('creates different signals for different keys', () => {
    const { factory } = trackingFactory();
    const coll = new SignalDeduplicator<{ id: string }, number>();

    const sig1 = coll.getOrCreate({ id: '1' }, factory);
    const sig2 = coll.getOrCreate({ id: '2' }, factory);

    expect(sig1).not.toBe(sig2);
    expect(sig1()).toBe(1);
    expect(sig2()).toBe(2);
    expect(coll.size).toBe(2);
  });

  // --- Structural key equality ---

  it('treats structurally equal keys as the same entry', () => {
    const created = new Map<string, ReturnType<typeof createSignal<number>>>();
    const factory = (key: { id: string; extra: number }) => {
      const s = createSignal(Number(key.id));
      created.set(key.id, s);
      return s;
    };
    const coll = new SignalDeduplicator<{ id: string; extra: number }, number>();

    const sig1 = coll.getOrCreate({ id: '1', extra: 42 }, factory);
    const sig2 = coll.getOrCreate({ extra: 42, id: '1' }, factory); // different key order

    expect(sig1).toBe(sig2);
    expect(coll.size).toBe(1);
  });

  // --- Auto-eviction via onUnobserved ---

  it('auto-evicts signal when computed stops depending on it', () => {
    const { factory } = trackingFactory();
    const coll = new SignalDeduplicator<{ id: string }, number>();

    const toggle = createSignal(true);
    const derived = computed(() => {
      if (toggle()) {
        return coll.getOrCreate({ id: '1' }, factory)();
      } else {
        return coll.getOrCreate({ id: '2' }, factory)();
      }
    });

    // Eviction is driven by 'deactivate', which only fires for observed
    // signals — so the consuming computed must itself be observed.
    const stop = derived.observe('value', () => {});

    // Initially depends on id:1
    expect(derived()).toBe(1);
    expect(coll.size).toBe(1);

    // Switch to id:2 — computed drops dependency on id:1
    toggle.set(false);
    expect(derived()).toBe(2);

    // id:1 was auto-evicted via onUnobserved (no manual evict() needed)
    expect(coll.size).toBe(1);
    expect([...coll.keys()]).toEqual([{ id: '2' }]);
    stop();
  });

  it('does not evict while the consuming computed is unobserved', () => {
    const { factory } = trackingFactory();
    const coll = new SignalDeduplicator<{ id: string }, number>();
    const toggle = createSignal(true);
    const derived = computed(() => (toggle() ? coll.getOrCreate({ id: '1' }, factory)() : 0));

    // A plain read registers nothing, so the cached signal never activates
    // and therefore never deactivates.
    expect(derived()).toBe(1);
    toggle.set(false);
    expect(derived()).toBe(0);
    expect(coll.size).toBe(1);
  });

  it('auto-evicts signal when stream subscriber disconnects', () => {
    const { factory } = trackingFactory();
    const coll = new SignalDeduplicator<{ id: string }, number>();

    const sig = coll.getOrCreate({ id: '1' }, factory);

    // Connect a stream subscriber
    const values: number[] = [];
    const stream = fromSignal(sig).connect({
      next: (v) => { values.push(v); return undefined; },
      complete: () => {},
      error: () => {},
    });
    stream.resume();
    expect(coll.size).toBe(1);

    // Disconnect — signal becomes unobserved → auto-evicted
    stream[Symbol.dispose]();
    expect(coll.size).toBe(0);
  });

  it('does not auto-evict while still observed by stream', () => {
    const { factory } = trackingFactory();
    const coll = new SignalDeduplicator<{ id: string }, number>();

    const sig = coll.getOrCreate({ id: '1' }, factory);

    // Connect a stream subscriber
    const stream = fromSignal(sig).connect({
      next: () => undefined,
      complete: () => {},
      error: () => {},
    });
    stream.resume();

    // Signal is observed via stream — stays in cache
    expect(coll.size).toBe(1);

    // Only auto-evicts after disconnect
    stream[Symbol.dispose]();
    expect(coll.size).toBe(0);
  });

  // --- onEvict callback ---

  it('calls onEvict on auto-eviction', () => {
    const { factory } = trackingFactory();
    const evictedKeys: string[] = [];
    const coll = new SignalDeduplicator<{ id: string }, number>({
      onEvict: (key) => evictedKeys.push(key.id),
    });

    const toggle = createSignal(true);
    const derived = computed(() => {
      if (toggle()) {
        return coll.getOrCreate({ id: '1' }, factory)();
      } else {
        return 0;
      }
    });

    const stop = derived.observe('value', () => {});
    expect(derived()).toBe(1);

    // Drop dependency — triggers auto-eviction and onEvict
    toggle.set(false);
    expect(derived()).toBe(0);

    expect(evictedKeys).toEqual(['1']);
    stop();
  });

  it('calls onEvict on delete()', () => {
    const { factory } = trackingFactory();
    const evictedKeys: string[] = [];
    const coll = new SignalDeduplicator<{ id: string }, number>({
      onEvict: (key) => evictedKeys.push(key.id),
    });

    coll.getOrCreate({ id: '1' }, factory);
    coll.delete({ id: '1' });

    expect(evictedKeys).toEqual(['1']);
  });

  // --- delete ---

  it('delete() removes a specific entry', () => {
    const { factory } = trackingFactory();
    const coll = new SignalDeduplicator<{ id: string }, number>();

    coll.getOrCreate({ id: '1' }, factory);
    coll.getOrCreate({ id: '2' }, factory);
    expect(coll.size).toBe(2);

    expect(coll.delete({ id: '1' })).toBe(true);
    expect(coll.size).toBe(1);
    expect(coll.delete({ id: '1' })).toBe(false); // already gone
  });

  // --- clear ---

  it('clear() removes all entries', () => {
    const { factory } = trackingFactory();
    const evictedKeys: string[] = [];
    const coll = new SignalDeduplicator<{ id: string }, number>({
      onEvict: (key) => evictedKeys.push(key.id),
    });

    coll.getOrCreate({ id: '1' }, factory);
    coll.getOrCreate({ id: '2' }, factory);
    coll.getOrCreate({ id: '3' }, factory);

    coll.clear();
    expect(coll.size).toBe(0);
    expect(evictedKeys).toHaveLength(3);
  });

  // --- Iteration ---

  it('entries() yields all cached pairs', () => {
    const { factory } = trackingFactory();
    const coll = new SignalDeduplicator<{ id: string }, number>();

    coll.getOrCreate({ id: '1' }, factory);
    coll.getOrCreate({ id: '2' }, factory);

    const entries = [...coll.entries()];
    expect(entries).toHaveLength(2);
  });

  it('keys() yields all keys', () => {
    const { factory } = trackingFactory();
    const coll = new SignalDeduplicator<{ id: string }, number>();

    coll.getOrCreate({ id: '1' }, factory);
    coll.getOrCreate({ id: '2' }, factory);

    const keys = [...coll.keys()];
    expect(keys).toHaveLength(2);
  });

  it('values() yields all signals', () => {
    const { factory } = trackingFactory();
    const coll = new SignalDeduplicator<{ id: string }, number>();

    coll.getOrCreate({ id: '1' }, factory);
    coll.getOrCreate({ id: '2' }, factory);

    const signals = [...coll.values()];
    expect(signals).toHaveLength(2);
    expect(signals[0]()).toBeDefined();
  });

  // --- Dynamic computed scenario (the motivating use case) ---

  it('supports dynamic table query caching inside computed with auto-eviction', () => {
    // Simulate: entries signal drives dependent line queries
    const entrySignal = createSignal([
      { id: 'e1', description: 'Purchase' },
      { id: 'e2', description: 'Sale' },
    ]);

    const lineData: Record<string, string[]> = {
      e1: ['Debit 1000', 'Credit 2000'],
      e2: ['Debit 3000', 'Credit 4000'],
    };

    const lineCache = new SignalDeduplicator<{ entryId: string }, string[]>();
    const lineFactory = (key: { entryId: string }) =>
      createSignal(lineData[key.entryId] ?? []);

    const journalView = computed(() => {
      return entrySignal().map((entry) => {
        const lines = lineCache.getOrCreate({ entryId: entry.id }, lineFactory);
        return { ...entry, lines: lines() };
      });
    });

    // The view is observed (as a live query would be), so dropped line
    // signals deactivate and are evicted.
    const stop = journalView.observe('value', () => {});

    // Initial read
    const result = journalView();
    expect(result).toEqual([
      { id: 'e1', description: 'Purchase', lines: ['Debit 1000', 'Credit 2000'] },
      { id: 'e2', description: 'Sale', lines: ['Debit 3000', 'Credit 4000'] },
    ]);
    expect(lineCache.size).toBe(2);

    // Change entries — now only e1
    entrySignal.set([{ id: 'e1', description: 'Purchase' }]);

    const result2 = journalView();
    expect(result2).toEqual([
      { id: 'e1', description: 'Purchase', lines: ['Debit 1000', 'Credit 2000'] },
    ]);

    // e2's line signal was auto-evicted (no manual evict() needed)
    expect(lineCache.size).toBe(1);
    stop();
  });

  // --- Signal.observe('deactivate') ---

  it('Signal.observe deactivate fires when last dependent leaves', () => {
    const sig = createSignal(42);
    const cb = vi.fn();
    sig.observe('deactivate', cb);

    // Observe via computed
    const derived = computed(() => sig() * 2);
    expect(derived()).toBe(84);

    // A plain read of an unobserved computed registers nothing
    expect(sig.observed).toBe(false);
    expect(cb).not.toHaveBeenCalled();

    // Create a second computed that also depends on sig
    const derived2 = computed(() => sig() + 1);
    expect(derived2()).toBe(43);

    // Connect a stream to derived to keep it alive, then dispose
    const s1 = fromSignal(derived).connect({ next: () => undefined, complete: () => {}, error: () => {} });
    s1.resume();
    expect(sig.observed).toBe(true);
    const s2 = fromSignal(derived2).connect({ next: () => undefined, complete: () => {}, error: () => {} });
    s2.resume();

    // Dispose one — still observed by derived2
    s1[Symbol.dispose]();
    expect(cb).not.toHaveBeenCalled();

    // Dispose the other — now unobserved
    s2[Symbol.dispose]();
    expect(cb).toHaveBeenCalledTimes(1);
  });

  it('Signal.observe deactivate returns unsubscribe function', () => {
    const sig = createSignal(0);
    const cb = vi.fn();
    const unsub = sig.observe('deactivate', cb);

    const derived = computed(() => sig());
    const s = fromSignal(derived).connect({ next: () => undefined, complete: () => {}, error: () => {} });
    s.resume();

    // Unsubscribe before losing observers
    unsub();
    s[Symbol.dispose]();

    expect(cb).not.toHaveBeenCalled();
  });
});
