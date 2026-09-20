// ---------------------------------------------------------------------------
// AsyncSignal — tests (ref-counted lazy signal wrappers)
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { createSignal } from './signal.js';
import {
  computedAsync,
  deriveResource,
  alwaysAvailable,
  createAsyncSignal,
} from './async-state.js';
import {
  available,
  loading,
  errored,
  isAvailable,
  isLoading,
  isErrored,
  isUnavailable,
  unavailable,
} from './async-value.js';
import type { AsyncValue } from './async-value.js';
import type { Signal, WritableSignal } from './types.js';
import { fromSignal } from './sources/from-signal.js';
import { createOwner, withOwner } from './owner.js';
import type { Owner, OwnedTask } from './owner.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Subscribe to a signal for activation. Returns an unsubscribe function.
 * With ref-counted signals, this triggers activation on first call.
 */
function sub<T>(signal: Signal<T>): () => void {
  const conn = fromSignal(signal).connect({
    next() { return undefined; },
    complete() {},
    error() {},
  });
  conn.resume();
  return () => conn[Symbol.dispose]();
}

/** Flush promise microtasks. */
async function flush() {
  for (let i = 0; i < 5; i++) await Promise.resolve();
}

/**
 * Create a writable AsyncSignal for tests — a WritableSignal<AsyncValue<T>>
 * with a no-op .retry() attached so it satisfies the AsyncSignal interface.
 */
function asyncSignal<T>(initial: AsyncValue<T>): WritableSignal<AsyncValue<T>> & { retry(): void } {
  const sig = createSignal<AsyncValue<T>>(initial);
  Object.defineProperty(sig, 'retry', { value: () => {}, configurable: true, writable: true });
  return sig as WritableSignal<AsyncValue<T>> & { retry(): void };
}

// ---------------------------------------------------------------------------
// alwaysAvailable
// ---------------------------------------------------------------------------

describe('alwaysAvailable', () => {
  it('wraps a regular signal as always available', () => {
    const count = createSignal(42);
    const asyncCount = alwaysAvailable(count);

    expect(asyncCount()).toEqual(available(42));
    expect(typeof asyncCount.retry).toBe('function');

    count.set(99);
    expect(asyncCount()).toEqual(available(99));
  });
});

// ---------------------------------------------------------------------------
// computedAsync
// ---------------------------------------------------------------------------

describe('computedAsync', () => {
  it('combines available signals through async callback', async () => {
    const s1 = asyncSignal<number>(available(1));
    const s2 = asyncSignal<string>(available('a'));

    const result = computedAsync(
      [s1, s2],
      async (n, s) => ({ n, s }),
    );

    const unsub = sub(result);
    await flush();
    expect(result()).toEqual(available({ n: 1, s: 'a' }));

    unsub();
  });

  it('starts as unavailable when no subscribers (cold)', () => {
    const s1 = asyncSignal<number>(available(42));

    const result = computedAsync([s1], async (n) => n * 2);

    expect(isUnavailable(result())).toBe(true);
  });

  it('shows loading when inputs are available but promise pending', () => {
    const s1 = asyncSignal<number>(available(1));

    const result = computedAsync(
      [s1],
      () => new Promise<number>(() => {}), // never resolves
    );

    const unsub = sub(result);
    expect(isLoading(result())).toBe(true);
    unsub();
  });

  it('propagates loading when an input is loading', () => {
    const s1 = asyncSignal<number>(loading());

    const result = computedAsync([s1], async (n) => n * 2);

    const unsub = sub(result);
    expect(isLoading(result())).toBe(true);
    unsub();
  });

  it('propagates errored when an input is errored', () => {
    const s1 = asyncSignal<number>(errored('boom'));

    const result = computedAsync([s1], async (n) => n * 2);

    const unsub = sub(result);
    expect(isErrored(result())).toBe(true);
    expect((result() as any).error).toBe('boom');
    unsub();
  });

  it('recalculates when inputs change', async () => {
    const s1 = asyncSignal<number>(available(1));

    const result = computedAsync([s1], async (n) => n * 10);

    const unsub = sub(result);
    await flush();
    expect(result()).toEqual(available(10));

    s1.set(available(5));
    await flush();
    expect(result()).toEqual(available(50));

    unsub();
  });

  it('discards stale results when inputs change mid-flight', async () => {
    const s1 = asyncSignal<number>(available(1));
    const resolvers: Array<(v: number) => void> = [];

    const result = computedAsync(
      [s1],
      (_n) => new Promise<number>((resolve) => { resolvers.push(resolve); }),
    );

    const unsub = sub(result);
    expect(isLoading(result())).toBe(true);

    // Change input while first promise is pending
    s1.set(available(2));
    expect(isLoading(result())).toBe(true);

    // Resolve first promise (stale) — should be ignored
    resolvers[0](100);
    await flush();
    expect(isLoading(result())).toBe(true);

    // Resolve second promise (current) — should be used
    resolvers[1](200);
    await flush();
    expect(result()).toEqual(available(200));

    unsub();
  });

  it('deactivates on last subscriber — returns to unavailable', async () => {
    const s1 = asyncSignal<number>(available(42));

    const result = computedAsync([s1], async (n) => n * 2);

    const unsub = sub(result);
    await flush();
    expect(result()).toEqual(available(84));

    unsub();
    expect(isUnavailable(result())).toBe(true);
    expect((result() as any).staleValue).toBe(84);
  });

  it('handles callback errors', async () => {
    const s1 = asyncSignal<number>(available(1));

    const result = computedAsync(
      [s1],
      async () => { throw new Error('compute-fail'); },
    );

    const unsub = sub(result);
    await flush();
    expect(isErrored(result())).toBe(true);
    expect((result() as any).error).toBeInstanceOf(Error);
    expect((result() as any).error.message).toBe('compute-fail');

    unsub();
  });

  it('keepStale: false does not retain stale values', async () => {
    const s1 = asyncSignal<number>(available(1));

    const result = computedAsync([s1], async (n) => n * 10);

    const unsub = sub(result);
    await flush();
    expect(result()).toEqual(available(10));

    s1.set(available(2));
    expect(isLoading(result())).toBe(true);
    expect((result() as any).staleValue).toBeUndefined();

    await flush();
    expect(result()).toEqual(available(20));

    unsub();
  });

  it('activates ref-counted inputs on subscribe', async () => {
    let activated = false;
    const input = computedAsync([], async () => {
      activated = true;
      return 42;
    });

    const result = computedAsync(
      [input],
      async (n) => n * 2,
    );

    expect(activated).toBe(false);

    const unsub = sub(result);
    // Subscribing to result should activate input
    expect(activated).toBe(true);

    await flush();
    expect(result()).toEqual(available(84));

    unsub();
  });

  it('multiple inputs — all must be available', async () => {
    const s1 = asyncSignal<number>(available(1));
    const s2 = asyncSignal<string>(loading());
    const s3 = asyncSignal<boolean>(available(true));

    const result = computedAsync(
      [s1, s2, s3],
      async (n, s, b) => `${n}-${s}-${b}`,
    );

    const unsub = sub(result);
    expect(isLoading(result())).toBe(true);

    s2.set(available('x'));
    await flush();
    expect(result()).toEqual(available('1-x-true'));

    unsub();
  });

  it('retry is a no-op when not errored', async () => {
    let calls = 0;
    const s1 = asyncSignal<number>(available(1));

    const result = computedAsync([s1], async (n) => {
      calls++;
      return n * 10;
    });

    const unsub = sub(result);
    await flush();
    expect(result()).toEqual(available(10));
    expect(calls).toBe(1);

    // retry on available signal → no-op
    result.retry();
    await flush();
    expect(calls).toBe(1);

    unsub();
  });

  it('retry re-runs when errored', async () => {
    let calls = 0;
    const s1 = asyncSignal<number>(available(1));

    const result = computedAsync([s1], async (n) => {
      calls++;
      if (calls === 1) throw new Error('fail');
      return n * 10;
    });

    const unsub = sub(result);
    await flush();
    expect(isErrored(result())).toBe(true);
    expect(calls).toBe(1);

    result.retry();
    await flush();
    expect(result()).toEqual(available(10));
    expect(calls).toBe(2);

    unsub();
  });

  it('reload re-runs the callback and returns new value', async () => {
    let calls = 0;
    const s1 = asyncSignal<number>(available(1));

    const result = computedAsync([s1], async (n) => {
      calls++;
      return n * 10 + calls;
    });

    const unsub = sub(result);
    await flush();
    expect(result()).toEqual(available(11)); // 1*10 + 1
    expect(calls).toBe(1);

    const reloaded = await result.reload();
    expect(reloaded).toBe(12); // 1*10 + 2
    expect(result()).toEqual(available(12));
    expect(calls).toBe(2);

    unsub();
  });

  it('reload rejects when cold (inactive)', async () => {
    const s1 = asyncSignal<number>(available(1));
    const result = computedAsync([s1], async (n) => n * 2);

    // No subscription → cold
    await expect(result.reload()).rejects.toThrow();
  });

  it('reload coalesces with in-flight load', async () => {
    let calls = 0;
    let resolver: ((v: number) => void) | undefined;
    const s1 = asyncSignal<number>(available(1));

    const result = computedAsync(
      [s1],
      (_n) => new Promise<number>((r) => { calls++; resolver = r; }),
    );

    const unsub = sub(result);
    expect(isLoading(result())).toBe(true);
    expect(calls).toBe(1);

    // reload while loading → coalesces (doesn't start a new call)
    const p = result.reload();
    expect(calls).toBe(1);

    resolver!(42);
    await flush();
    const v = await p;
    expect(v).toBe(42);
    expect(result()).toEqual(available(42));

    unsub();
  });

  it('reactivates after deactivation', async () => {
    const s1 = asyncSignal<number>(available(1));

    const result = computedAsync([s1], async (n) => n * 2);

    // First subscription → activate
    const unsub1 = sub(result);
    await flush();
    expect(result()).toEqual(available(2));

    // Unsubscribe → deactivate (dormant)
    unsub1();
    expect(isUnavailable(result())).toBe(true);
    expect((result() as any).staleValue).toBe(2);

    // Re-subscribe → reactivate
    s1.set(available(5));
    const unsub2 = sub(result);
    await flush();
    expect(result()).toEqual(available(10));

    unsub2();
  });

  it('retry chains to upstream inputs when errored', async () => {
    let retried = false;
    const s1 = asyncSignal<number>(errored('upstream-fail'));
    s1.retry = () => { retried = true; };

    const result = computedAsync([s1], async (n) => n * 2);

    const unsub = sub(result);
    expect(isErrored(result())).toBe(true);

    result.retry();
    expect(retried).toBe(true);

    unsub();
  });
});

// ---------------------------------------------------------------------------
// deriveResource
// ---------------------------------------------------------------------------

describe('deriveResource', () => {
  it('derives a resource when inputs are available', async () => {
    const s1 = asyncSignal<number>(available(5));
    const owner = createOwner('test');

    const handle = deriveResource(
      s1,
      async (n) => ({ value: n * 10, [Symbol.dispose]() {} }),
      { owner },
    );

    await flush();
    expect(handle()).toEqual(available(50));

    handle.dispose();
    void owner.dispose();
  });

  it('starts as loading (hot)', () => {
    const s1 = asyncSignal<number>(available(1));
    const owner = createOwner('test');

    const handle = deriveResource(
      s1,
      async (n) => ({ value: n, [Symbol.dispose]() {} }),
      { owner },
    );

    expect(isLoading(handle())).toBe(true);

    handle.dispose();
    void owner.dispose();
  });

  it('disposes old resource when inputs change', async () => {
    const s1 = asyncSignal<number>(available(1));
    const owner = createOwner('test');
    let disposeCount = 0;

    const handle = deriveResource(
      s1,
      async (n) => ({
        value: n * 10,
        [Symbol.dispose]() { disposeCount++; },
      }),
      { owner },
    );

    await flush();
    expect(handle()).toEqual(available(10));
    expect(disposeCount).toBe(0);

    s1.set(available(2));
    await flush();
    expect(handle()).toEqual(available(20));
    expect(disposeCount).toBe(1);

    handle.dispose();
    expect(disposeCount).toBe(2);
    void owner.dispose();
  });

  it('disposes resource on handle dispose', async () => {
    const s1 = asyncSignal<number>(available(1));
    const owner = createOwner('test');
    let disposed = false;

    const handle = deriveResource(
      s1,
      async (n) => ({ value: n, [Symbol.dispose]() { disposed = true; } }),
      { owner },
    );

    await flush();
    expect(disposed).toBe(false);

    handle.dispose();
    expect(disposed).toBe(true);
    void owner.dispose();
  });

  it('auto-disposes when the owner is disposed', async () => {
    const s1 = asyncSignal<number>(available(1));
    const owner = createOwner('test');
    let disposed = false;

    deriveResource(
      s1,
      async (n) => ({ value: n, [Symbol.dispose]() { disposed = true; } }),
      { owner },
    );

    await flush();
    expect(disposed).toBe(false);

    void owner.dispose();
    expect(disposed).toBe(true);
  });

  it('propagates loading from inputs', () => {
    const s1 = asyncSignal<number>(loading());
    const owner = createOwner('test');

    const handle = deriveResource(
      s1,
      async (n) => ({ value: n, [Symbol.dispose]() {} }),
      { owner },
    );

    expect(isLoading(handle())).toBe(true);

    handle.dispose();
    void owner.dispose();
  });

  it('propagates errored from inputs', () => {
    const s1 = asyncSignal<number>(errored('fail'));
    const owner = createOwner('test');

    const handle = deriveResource(
      s1,
      async (n) => ({ value: n, [Symbol.dispose]() {} }),
      { owner },
    );

    expect(isErrored(handle())).toBe(true);
    expect((handle() as any).error).toBe('fail');

    handle.dispose();
    void owner.dispose();
  });

  it('disposes resource when inputs become unavailable', async () => {
    const s1 = asyncSignal<number>(available(1));
    const owner = createOwner('test');
    let disposed = false;

    const handle = deriveResource(
      s1,
      async (n) => ({ value: n, [Symbol.dispose]() { disposed = true; } }),
      { owner },
    );

    await flush();
    expect(isAvailable(handle())).toBe(true);
    expect(disposed).toBe(false);

    s1.set(unavailable());
    expect(disposed).toBe(true);
    expect(isUnavailable(handle())).toBe(true);

    handle.dispose();
    void owner.dispose();
  });

  it('handles factory errors', async () => {
    const s1 = asyncSignal<number>(available(1));
    const owner = createOwner('test');

    const handle = deriveResource(
      s1,
      async () => { throw new Error('derive-fail'); },
      { owner },
    );

    await flush();
    expect(isErrored(handle())).toBe(true);
    expect((handle() as any).error).toBeInstanceOf(Error);

    handle.dispose();
    void owner.dispose();
  });

  it('activates ref-counted inputs immediately (hot)', async () => {
    let activated = false;
    const input = computedAsync([], async () => {
      activated = true;
      return 42;
    });
    const owner = createOwner('test');

    const handle = deriveResource(
      input,
      async (n) => ({ value: n * 2, [Symbol.dispose]() {} }),
      { owner },
    );

    // Hot — activates input immediately (no subscriber needed)
    expect(activated).toBe(true);

    // Multiple async hops: computedAsync promise → signal → task → result
    await new Promise((r) => setTimeout(r, 20));
    expect(isAvailable(handle())).toBe(true);
    expect((handle() as any).value).toBe(84);

    handle.dispose();
    void owner.dispose();
  });

  it('retry chains to upstream inputs when errored', async () => {
    let retried = false;
    const s1 = asyncSignal<number>(errored('upstream-fail'));
    s1.retry = () => { retried = true; };
    const owner = createOwner('test');

    const handle = deriveResource(
      s1,
      async (n) => ({ value: n * 2, [Symbol.dispose]() {} }),
      { owner },
    );

    expect(isErrored(handle())).toBe(true);
    handle.retry();
    expect(retried).toBe(true);

    handle.dispose();
    void owner.dispose();
  });

  it('reload re-runs the factory and returns new value', async () => {
    let calls = 0;
    const s1 = asyncSignal<number>(available(1));
    const owner = createOwner('test');

    const handle = deriveResource(
      s1,
      async (n) => {
        calls++;
        return { value: n * 10 + calls, [Symbol.dispose]() {} };
      },
      { owner },
    );

    await flush();
    expect(handle()).toEqual(available(11));
    expect(calls).toBe(1);

    const reloaded = await handle.reload();
    expect(reloaded).toBe(12);
    expect(calls).toBe(2);

    handle.dispose();
    void owner.dispose();
  });

  it('reload rejects after dispose', async () => {
    const s1 = asyncSignal<number>(available(1));
    const owner = createOwner('test');

    const handle = deriveResource(
      s1,
      async (n) => ({ value: n, [Symbol.dispose]() {} }),
      { owner },
    );

    await flush();
    handle.dispose();

    await expect(handle.reload()).rejects.toThrow();
    void owner.dispose();
  });

  // --- Sync factory returns (AsyncValue<DerivedValue<R>>) -----------------

  it('sync: available() installs resource immediately (no loading)', () => {
    const s1 = asyncSignal<number>(available(5));
    const owner = createOwner('test');

    const handle = deriveResource(
      s1,
      (n) => available({ value: n * 10, [Symbol.dispose]() {} }),
      { owner },
    );

    // Sync — no loading state, available immediately
    expect(handle()).toEqual(available(50));

    handle.dispose();
    void owner.dispose();
  });

  it('sync: unavailable() skips resource creation', () => {
    const s1 = asyncSignal<number>(available(1));
    const owner = createOwner('test');

    const handle = deriveResource(
      s1,
      () => unavailable(),
      { owner },
    );

    expect(isUnavailable(handle())).toBe(true);

    handle.dispose();
    void owner.dispose();
  });

  it('sync: errored() sets error state', () => {
    const s1 = asyncSignal<number>(available(1));
    const owner = createOwner('test');

    const handle = deriveResource(
      s1,
      () => errored<never>('sync-fail'),
      { owner },
    );

    expect(isErrored(handle())).toBe(true);
    expect((handle() as any).error).toBe('sync-fail');

    handle.dispose();
    void owner.dispose();
  });

  it('sync: transitions from unavailable() to available() on input change', () => {
    const s1 = asyncSignal<number>(available(0));
    const owner = createOwner('test');

    const handle = deriveResource(
      s1,
      (n) => n > 0
        ? available({ value: n * 10, [Symbol.dispose]() {} })
        : unavailable(),
      { owner },
    );

    // n=0 → unavailable
    expect(isUnavailable(handle())).toBe(true);

    // n=5 → available
    s1.set(available(5));
    expect(handle()).toEqual(available(50));

    handle.dispose();
    void owner.dispose();
  });

  it('sync: disposes old resource when factory returns unavailable()', () => {
    const s1 = asyncSignal<number>(available(1));
    const owner = createOwner('test');
    let disposed = false;

    const handle = deriveResource(
      s1,
      (n) => n > 0
        ? available({ value: n, [Symbol.dispose]() { disposed = true; } })
        : unavailable(),
      { owner },
    );

    expect(handle()).toEqual(available(1));
    expect(disposed).toBe(false);

    // Input changes to 0 → factory returns unavailable → old resource disposed
    s1.set(available(0));
    expect(isUnavailable(handle())).toBe(true);
    expect(disposed).toBe(true);

    handle.dispose();
    void owner.dispose();
  });

  // --- Ownership ------------------------------------------------------------

  it('uses the ambient owner from withOwner() (sync path)', () => {
    const input = asyncSignal<number>(available(42));
    const owner = createOwner('scope');
    let disposed = false;

    const handle = withOwner(owner, () =>
      deriveResource(input, (n) =>
        available({ value: n * 2, [Symbol.dispose]() { disposed = true; } }),
      ),
    );

    expect(handle()).toEqual(available(84));
    expect(owner.size).toBe(1);

    void owner.dispose();
    expect(disposed).toBe(true);
  });

  it('uses the ambient owner from withOwner() (async path)', async () => {
    const input = asyncSignal<number>(available(7));
    const owner = createOwner('scope');

    const handle = withOwner(owner, () =>
      deriveResource(input, async (n) => ({ value: n * 3, [Symbol.dispose]() {} })),
    );

    expect(isLoading(handle())).toBe(true);
    await flush();
    expect(handle()).toEqual(available(21));

    handle.dispose();
    expect(owner.size).toBe(0);
    void owner.dispose();
  });

  it('works without any owner (untracked)', async () => {
    const input = asyncSignal<number>(available(1));
    let disposed = false;

    const handle = deriveResource(input, async (n) => ({
      value: n,
      [Symbol.dispose]() { disposed = true; },
    }));

    await flush();
    expect(handle()).toEqual(available(1));

    handle.dispose();
    expect(disposed).toBe(true);
  });

  it('start: false defers observation until start()', async () => {
    const input = asyncSignal<number>(available(5));
    let calls = 0;

    const handle = deriveResource(
      input,
      (n) => { calls++; return available({ value: n, [Symbol.dispose]() {} }); },
      { start: false },
    );

    expect(calls).toBe(0);
    expect(isLoading(handle())).toBe(true);

    handle.start();
    expect(calls).toBe(1);
    expect(handle()).toEqual(available(5));

    handle.start(); // idempotent
    expect(calls).toBe(1);

    handle.dispose();
  });

  it('runs promise derivations through owner.spawn() when provided', async () => {
    const input = asyncSignal<number>(available(2));
    const spawned: string[] = [];
    let aborted = 0;

    const owner: Owner = {
      register: () => undefined,
      spawn<T>(task: () => Promise<T>, name?: string): OwnedTask<T> {
        spawned.push(name ?? '?');
        return { promise: task(), abort() { aborted++; } };
      },
    };

    const handle = deriveResource(
      input,
      async (n) => ({ value: n * 10, [Symbol.dispose]() {} }),
      { owner, name: 'db' },
    );

    await flush();
    expect(handle()).toEqual(available(20));
    expect(spawned).toEqual(['db::derive']);

    // A new input value mid-flight aborts the supervised task
    input.set(loading());
    input.set(available(3));
    expect(aborted).toBe(0); // the first task had already settled
    input.set(available(4));
    expect(aborted).toBe(1); // the task for 3 was still in flight

    await flush();
    expect(handle()).toEqual(available(40));
    handle.dispose();
  });
});

// ---------------------------------------------------------------------------
// createAsyncSignal — cold Promise→AsyncSignal bridge
// ---------------------------------------------------------------------------

describe('createAsyncSignal', () => {
  it('starts as unavailable (cold)', () => {
    const handle = createAsyncSignal(() => new Promise<number>(() => {}));
    expect(isUnavailable(handle())).toBe(true);
  });

  it('transitions to loading then available when subscribed', async () => {
    const handle = createAsyncSignal(() => Promise.resolve(42));
    const unsub = sub(handle);
    expect(isLoading(handle())).toBe(true);
    await flush();
    const v = handle();
    expect(isAvailable(v)).toBe(true);
    expect((v as any).value).toBe(42);
    unsub();
  });

  it('transitions to errored on reject', async () => {
    const handle = createAsyncSignal(() => Promise.reject(new Error('boom')));
    const unsub = sub(handle);
    await flush();
    const v = handle();
    expect(isErrored(v)).toBe(true);
    expect((v as any).error.message).toBe('boom');
    unsub();
  });

  it('retry re-invokes the factory', async () => {
    let calls = 0;
    const handle = createAsyncSignal(() => {
      calls++;
      return calls === 1
        ? Promise.reject(new Error('first'))
        : Promise.resolve('ok');
    });
    const unsub = sub(handle);
    await flush();
    expect(isErrored(handle())).toBe(true);
    expect(calls).toBe(1);

    handle.retry();
    expect(isLoading(handle())).toBe(true);
    await flush();
    expect(isAvailable(handle())).toBe(true);
    expect((handle() as any).value).toBe('ok');
    expect(calls).toBe(2);
    unsub();
  });

  it('retry is a no-op when already available', async () => {
    const handle = createAsyncSignal(() => Promise.resolve(42));
    const unsub = sub(handle);
    await flush();
    expect(isAvailable(handle())).toBe(true);

    handle.retry();
    // Still available, no re-invoke (retry is error-only)
    expect(isAvailable(handle())).toBe(true);
    unsub();
  });

  it('reload re-invokes and returns new value', async () => {
    let calls = 0;
    const handle = createAsyncSignal(() => {
      calls++;
      return Promise.resolve(calls * 10);
    });
    const unsub = sub(handle);
    await flush();
    expect((handle() as any).value).toBe(10);

    const reloaded = await handle.reload();
    expect(reloaded).toBe(20);
    expect((handle() as any).value).toBe(20);
    unsub();
  });

  it('reload carries stale value while loading', async () => {
    let calls = 0;
    const handle = createAsyncSignal(() => {
      calls++;
      return Promise.resolve(calls * 10);
    });
    const unsub = sub(handle);
    await flush();
    expect((handle() as any).value).toBe(10);

    const p = handle.reload();
    const s = handle();
    expect(s.status).toBe('loading');
    expect((s as any).staleValue).toBe(10);

    await p;
    expect((handle() as any).value).toBe(20);
    unsub();
  });

  it('returns to unavailable when last subscriber disconnects', async () => {
    const handle = createAsyncSignal(() => Promise.resolve(42));
    const unsub = sub(handle);
    await flush();
    expect(isAvailable(handle())).toBe(true);

    unsub();
    expect(isUnavailable(handle())).toBe(true);
  });
});
