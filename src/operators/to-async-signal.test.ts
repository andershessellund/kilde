// ---------------------------------------------------------------------------
// toAsyncSignal — tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { toAsyncSignal } from './to-async-signal.js';
import { deferred } from '../sources/deferred.js';
import { createSignal } from '../signal.js';
import { fromSignal } from '../sources/from-signal.js';
import {
  available,
  isAvailable,
  isLoading,
  isUnavailable,
  isErrored,
} from '../async-value.js';
import type { Signal } from '../types.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sub<T>(signal: Signal<T>): () => void {
  const conn = fromSignal(signal).connect({
    next() { return undefined; },
    complete() {},
    error() {},
  });
  conn.resume();
  return () => conn[Symbol.dispose]();
}

// ---------------------------------------------------------------------------
// Cold mode (default)
// ---------------------------------------------------------------------------

describe('toAsyncSignal — cold', () => {
  it('starts as unavailable when no subscribers', () => {
    const d = deferred<number>();
    const sig = toAsyncSignal<number>()(d);
    expect(isUnavailable(sig())).toBe(true);
  });

  it('transitions to loading on first subscriber', () => {
    const d = deferred<number>();
    const sig = toAsyncSignal<number>()(d);
    const unsub = sub(sig);
    expect(isLoading(sig())).toBe(true);
    unsub();
  });

  it('becomes available when source emits', () => {
    const d = deferred<number>();
    const sig = toAsyncSignal<number>()(d);
    const unsub = sub(sig);
    d.resolve(42);
    expect(sig()).toEqual(available(42));
    unsub();
  });

  it('updates on subsequent emissions', () => {
    const inner = createSignal(1);
    const source = fromSignal(inner);
    const sig = toAsyncSignal<number>()(source);
    const unsub = sub(sig);

    expect(sig()).toEqual(available(1));
    inner.set(2);
    expect(sig()).toEqual(available(2));

    unsub();
  });

  it('returns to unavailable when last subscriber disconnects', () => {
    const d = deferred<number>();
    const sig = toAsyncSignal<number>()(d);
    const unsub = sub(sig);
    d.resolve(42);
    expect(isAvailable(sig())).toBe(true);

    unsub();
    expect(isUnavailable(sig())).toBe(true);
    // Stale value is preserved
    expect((sig() as any).staleValue).toBe(42);
  });

  it('reconnects on resubscribe', () => {
    const inner = createSignal(10);
    const source = fromSignal(inner);
    const sig = toAsyncSignal<number>()(source);

    const unsub1 = sub(sig);
    expect(sig()).toEqual(available(10));
    unsub1();
    expect(isUnavailable(sig())).toBe(true);

    inner.set(20);
    const unsub2 = sub(sig);
    expect(sig()).toEqual(available(20));
    unsub2();
  });

  it('handles source errors', () => {
    const d = deferred<number>();
    d.promise.catch(() => {});
    const sig = toAsyncSignal<number>()(d);
    const unsub = sub(sig);
    d.reject(new Error('fail'));
    expect(isErrored(sig())).toBe(true);
    expect((sig() as any).error.message).toBe('fail');
    unsub();
  });

  it('retry reconnects when errored', () => {
    // Create an erroring source that we can control
    const d = deferred<number>();
    d.promise.catch(() => {});
    const sig = toAsyncSignal<number>()(d);
    const unsub = sub(sig);
    d.reject(new Error('first'));
    expect(isErrored(sig())).toBe(true);

    // retry is a no-op right now since deferred is single-use,
    // but verify it doesn't throw
    sig.retry();
    unsub();
  });
});

// ---------------------------------------------------------------------------
// Hot mode
// ---------------------------------------------------------------------------

describe('toAsyncSignal — hot', () => {
  it('starts as loading immediately (no subscriber needed)', () => {
    const d = deferred<number>();
    const sig = toAsyncSignal<number>({ hot: true })(d);
    expect(isLoading(sig())).toBe(true);
  });

  it('becomes available when source emits', () => {
    const d = deferred<number>();
    const sig = toAsyncSignal<number>({ hot: true })(d);
    d.resolve(42);
    expect(sig()).toEqual(available(42));
  });

  it('does not return to unavailable when observers leave', () => {
    const inner = createSignal(1);
    const source = fromSignal(inner);
    const sig = toAsyncSignal<number>({ hot: true })(source);

    expect(sig()).toEqual(available(1));

    // Subscribe and unsubscribe — should keep value
    const unsub = sub(sig);
    inner.set(2);
    expect(sig()).toEqual(available(2));
    unsub();

    // Still available — hot stays connected
    expect(sig()).toEqual(available(2));
  });

  it('handles source errors', () => {
    const d = deferred<number>();
    d.promise.catch(() => {});
    const sig = toAsyncSignal<number>({ hot: true })(d);
    d.reject(new Error('hot-fail'));
    expect(isErrored(sig())).toBe(true);
    expect((sig() as any).error.message).toBe('hot-fail');
  });
});
