// ---------------------------------------------------------------------------
// to-async.test.ts — Tests for toAsync() operator
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { stream } from '../stream.js';
import { deferred } from '../sources/deferred.js';
import { map } from './map.js';
import { toAsync } from './to-async.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { createOwner, withOwner } from '../owner.js';

describe('toAsync()', () => {
  it('produces a thunk that resolves with last value', async () => {
    const d = deferred<number>();
    const fn = stream(d, toAsync());
    expect(typeof fn).toBe('function');

    // The thunk hasn't connected yet — resolve the deferred, then call
    d.resolve(42);
    const result = await fn();
    expect(result).toBe(42);
  });

  it('works with operators in the pipeline', async () => {
    const d = deferred<number>();
    const fn = stream(d, map((x: number) => x * 2), toAsync());
    d.resolve(21);
    expect(await fn()).toBe(42);
  });

  it('runs under the owner ambient when the thunk is called', async () => {
    const owner = createOwner('test-toAsync-call');

    const d = deferred<number>();
    const fn = stream(d, toAsync());
    d.resolve(99);

    const result = await withOwner(owner, fn);
    expect(result).toBe(99);

    await owner.dispose();
  });

  it('rejects with StreamDisposedError when the owner is disposed', async () => {
    const owner = createOwner('test-toAsync-dispose');

    const d = deferred<number>();
    const fn = stream(d, toAsync());

    const pending = withOwner(owner, fn);
    expect(owner.size).toBe(1);

    await owner.dispose();

    await expect(pending).rejects.toBeInstanceOf(StreamDisposedError);
  });

  it('registers with the owner current at call time, not build time', async () => {
    const buildOwner = createOwner('build');
    const callOwner = createOwner('call');

    const d = deferred<number>();
    // Build the thunk under one owner...
    const fn = withOwner(buildOwner, () => stream(d, toAsync()));
    expect(buildOwner.size).toBe(0);

    // ...call it under another
    const pending = withOwner(callOwner, fn);
    expect(buildOwner.size).toBe(0);
    expect(callOwner.size).toBe(1);

    d.resolve(42);
    expect(await pending).toBe(42);

    await buildOwner.dispose();
    await callOwner.dispose();
  });

  it('accepts an explicit owner option', async () => {
    const owner = createOwner('explicit');
    const d = deferred<number>();
    const fn = stream(d, toAsync({ owner }));

    const pending = fn();
    expect(owner.size).toBe(1);

    await owner.dispose();
    await expect(pending).rejects.toBeInstanceOf(StreamDisposedError);
  });
});
