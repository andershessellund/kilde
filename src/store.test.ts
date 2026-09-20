// ---------------------------------------------------------------------------
// Store — tests for createStore and intoStore
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { deepEqual } from 'valsem';
import type { Sink } from './types.js';
import { PAUSE } from './types.js';
import { createStore, intoStore } from './store.js';
import { createRelay } from './relay.js';
import { fromArray } from './sources/from-array.js';

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

// ===========================================================================
// createStore
// ===========================================================================

describe('createStore', () => {
  it('has initial value', () => {
    const s = createStore(42);
    expect(s()).toBe(42);
  });

  it('set updates value and notifies subscribers', () => {
    const s = createStore(0);
    const sink = recordSink<number>();
    const stream = s.connect(sink);
    stream.resume(); // replays 0

    s.set(1);
    s.set(2);
    expect(sink.values).toEqual([0, 1, 2]);
    expect(s()).toBe(2);
  });

  it('update applies function and notifies', () => {
    const s = createStore(0);
    const sink = recordSink<number>();
    const stream = s.connect(sink);
    stream.resume();

    s.update((n) => n + 1);
    s.update((n) => n + 10);
    expect(sink.values).toEqual([0, 1, 11]);
  });

  it('deduplicates via a custom equals', () => {
    const s = createStore({ a: 1 }, { equals: deepEqual });
    const sink = recordSink<{ a: number }>();
    const stream = s.connect(sink);
    stream.resume();

    s.set({ a: 1 }); // same deep value — skipped
    s.set({ a: 2 }); // different — emitted
    expect(sink.values).toEqual([{ a: 1 }, { a: 2 }]);
  });

  it('replays current value on subscribe', () => {
    const s = createStore('hello');
    s.set('world');

    const sink = recordSink<string>();
    const stream = s.connect(sink);
    stream.resume();

    expect(sink.values).toEqual(['world']);
  });

  it('supports multiple subscribers', () => {
    const s = createStore(0);
    const sink1 = recordSink<number>();
    const sink2 = recordSink<number>();
    const s1 = s.connect(sink1);
    const s2 = s.connect(sink2);
    s1.resume();
    s2.resume();

    s.set(1);
    expect(sink1.values).toEqual([0, 1]);
    expect(sink2.values).toEqual([0, 1]);
  });

  it('supports custom equals', () => {
    const s = createStore(0, { equals: (a, b) => Math.abs(a - b) < 0.01 });
    const sink = recordSink<number>();
    const stream = s.connect(sink);
    stream.resume();

    s.set(0.005); // within threshold — skipped
    s.set(1.0); // different — emitted
    expect(sink.values).toEqual([0, 1.0]);
  });

  it('respects PAUSE from subscriber', () => {
    const s = createStore(0);
    const sink = pauseAfter<number>(1);
    const stream = s.connect(sink);
    stream.resume(); // delivers 0, then paused

    s.set(1);
    s.set(2);
    // paused — only latest is kept
    expect(sink.values).toEqual([0]);

    stream.resume();
    expect(sink.values).toEqual([0, 2]); // only latest
  });

  // ---------------------------------------------------------------------------
  // Dispose (lifecycle)
  // ---------------------------------------------------------------------------

  it('dispose completes all subscribers', () => {
    const s = createStore(0);
    const sink1 = recordSink<number>();
    const sink2 = recordSink<number>();
    s.connect(sink1).resume();
    s.connect(sink2).resume();

    s[Symbol.dispose]();
    expect(sink1.completed).toBe(true);
    expect(sink2.completed).toBe(true);
  });

  it('disposed flag is set after dispose', () => {
    const s = createStore(0);
    expect(s.disposed).toBe(false);
    s[Symbol.dispose]();
    expect(s.disposed).toBe(true);
  });

  it('set/update are no-ops after dispose', () => {
    const s = createStore(0);
    s[Symbol.dispose]();
    s.set(42);
    s.update((n) => n + 1);
    expect(s()).toBe(0); // unchanged
  });

  it('connect after dispose completes immediately on resume', () => {
    const s = createStore(0);
    s[Symbol.dispose]();

    const sink = recordSink<number>();
    const stream = s.connect(sink);
    stream.resume();

    expect(sink.completed).toBe(true);
    expect(sink.values).toEqual([]); // no replay
  });

  it('disposing individual subscriber does not affect others', () => {
    const s = createStore(0);
    const sink1 = recordSink<number>();
    const sink2 = recordSink<number>();
    const s1 = s.connect(sink1);
    const s2 = s.connect(sink2);
    s1.resume();
    s2.resume();

    s1[Symbol.dispose](); // disconnect sink1

    s.set(1);
    expect(sink1.values).toEqual([0]); // no further values
    expect(sink2.values).toEqual([0, 1]); // still receiving
  });

  // ---------------------------------------------------------------------------
  // Regression: connections are tracked per stream, not per sink identity
  // ---------------------------------------------------------------------------

  it('the same sink connected twice: disposing one stream keeps the other alive', () => {
    const s = createStore(0);
    const sink = recordSink<number>();
    const a = s.connect(sink);
    const b = s.connect(sink);
    a.resume();
    b.resume();
    expect(sink.values).toEqual([0, 0]);

    a[Symbol.dispose]();
    s.set(1);
    expect(sink.values).toEqual([0, 0, 1]); // b still receives

    s[Symbol.dispose]();
    expect(sink.completed).toBe(true); // the survivor is completed
  });

  it('the same sink connected twice is completed once per live stream on dispose', () => {
    const s = createStore(0);
    let completions = 0;
    const sink: Sink<number> = {
      next: () => undefined,
      complete: () => {
        completions++;
      },
      error() {},
    };
    s.connect(sink).resume();
    s.connect(sink).resume();
    s[Symbol.dispose]();
    expect(completions).toBe(2);
  });

  it('store dispose does not complete a stream that was never resumed — it completes on its first resume', () => {
    const s = createStore(0);
    const sink = recordSink<number>();
    const stream = s.connect(sink);
    s[Symbol.dispose]();
    expect(sink.completed).toBe(false); // nothing before the first resume()
    stream.resume();
    expect(sink.completed).toBe(true);
    expect(sink.values).toEqual([]);
  });

  it('resume() after store dispose is a no-op (no double complete)', () => {
    const s = createStore(0);
    let completions = 0;
    const sink: Sink<number> = {
      next: () => undefined,
      complete: () => {
        completions++;
      },
      error() {},
    };
    const stream = s.connect(sink);
    stream.resume();
    s[Symbol.dispose]();
    stream.resume();
    stream.resume();
    expect(completions).toBe(1);

    // connect-after-dispose path is also idempotent
    const late = s.connect(sink);
    late.resume();
    late.resume();
    expect(completions).toBe(2);
  });

  it('store dispose releases the signal subscriptions', () => {
    const s = createStore(0);
    s.connect(recordSink<number>()).resume();
    expect(s.observed).toBe(true);
    s[Symbol.dispose]();
    expect(s.observed).toBe(false);
  });
});

// ===========================================================================
// intoStore
// ===========================================================================

describe('intoStore', () => {
  it('folds source values into store', async () => {
    const store = createStore<number[]>([]);
    const source = fromArray([1, 2, 3]);

    await intoStore(store, (list, item: number) => [...list, item])(source);

    expect(store()).toEqual([1, 2, 3]);
  });

  it('resolves when source completes', async () => {
    const store = createStore(0);
    const relay = createRelay<number>();

    const promise = intoStore(store, (_, item: number) => item)(relay);

    relay.next(42);
    relay.complete();

    await promise;
    expect(store()).toBe(42);
  });

  it('rejects when source errors', async () => {
    const store = createStore(0);
    const relay = createRelay<number>();

    const promise = intoStore(store, (_, item: number) => item)(relay);

    relay.next(1);
    relay.error(new Error('boom'));

    await expect(promise).rejects.toThrow('boom');
    expect(store()).toBe(1); // last successful value
  });

  it('resolves when store is disposed', async () => {
    const store = createStore(0);
    const relay = createRelay<number>();

    const promise = intoStore(store, (_, item: number) => item)(relay);

    relay.next(42);
    store[Symbol.dispose]();

    await promise; // should resolve, not hang
    expect(store()).toBe(42);
  });

  it('resolves immediately if store already disposed', async () => {
    const store = createStore(0);
    store[Symbol.dispose]();

    const relay = createRelay<number>();
    await intoStore(store, (_, item: number) => item)(relay);
    // should resolve immediately without error
  });

  it('multiple sources can feed one store', async () => {
    const store = createStore<string[]>([]);
    const adds = createRelay<string>();
    const removes = createRelay<string>();

    const p1 = intoStore(store, (list, item: string) => [...list, item])(adds);
    const p2 = intoStore(
      store,
      (list, id: string) => list.filter((x) => x !== id),
    )(removes);

    adds.next('a');
    adds.next('b');
    removes.next('a');
    adds.next('c');

    expect(store()).toEqual(['b', 'c']);

    adds.complete();
    removes.complete();
    await Promise.all([p1, p2]);
  });

  it('store observes values from both direct set and intoStore', async () => {
    const store = createStore(0);
    const sink = recordSink<number>();
    store.connect(sink).resume();

    const relay = createRelay<number>();
    const promise = intoStore(store, (_, item: number) => item)(relay);

    relay.next(1);
    store.set(2);
    relay.next(3);

    relay.complete();
    await promise;

    // 0 (replay), 1 (from relay), 2 (direct set), 3 (from relay)
    expect(sink.values).toEqual([0, 1, 2, 3]);
  });

  it('does not update store after source completes', async () => {
    const store = createStore(0);
    const relay = createRelay<number>();

    const promise = intoStore(store, (_, item: number) => item)(relay);

    relay.next(42);
    relay.complete();
    await promise;

    // Store still works for direct mutation
    store.set(100);
    expect(store()).toBe(100);
  });
});
