// ---------------------------------------------------------------------------
// to-callback.test.ts — Tests for toCallback with owner tracking
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { stream } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { deferred } from '../sources/deferred.js';
import { createRelay } from '../relay.js';
import { map } from './map.js';
import { toCallback } from './to-callback.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { createOwner, withOwner } from '../owner.js';

describe('toCallback()', () => {
  it('calls fn for each value and resolves on complete', async () => {
    const values: number[] = [];
    const p = stream(fromArray([1, 2, 3]), toCallback((v: number) => {
      values.push(v);
    }));
    await p;
    expect(values).toEqual([1, 2, 3]);
  });

  it('resolves void (no value)', async () => {
    const result = await stream(fromArray([42]), toCallback(() => {}));
    expect(result).toBeUndefined();
  });

  it('rejects when source errors', async () => {
    const d = deferred<number>();
    const p = stream(d, toCallback(() => {}));
    d.promise.catch(() => {});
    d.reject(new Error('boom'));
    await expect(p).rejects.toThrow('boom');
  });

  it('works with operators in the pipeline', async () => {
    const values: number[] = [];
    const p = stream(
      fromArray([1, 2, 3]),
      map((x: number) => x * 10),
      toCallback((v: number) => { values.push(v); }),
    );
    await p;
    expect(values).toEqual([10, 20, 30]);
  });

  it('works under an owner (scoped lifetime)', async () => {
    const owner = createOwner('test-toCallback-owner');
    const values: number[] = [];

    await withOwner(owner, () => {
      const d = deferred<number>();
      const p = stream(d, toCallback((v: number) => { values.push(v); }));
      d.resolve(99);
      return p;
    });

    expect(values).toEqual([99]);
    await owner.dispose();
  });

  it('handles async push sources (relay)', async () => {
    const owner = createOwner('test-toCallback-relay');
    const values: number[] = [];
    const relay = createRelay<number>();

    const done = withOwner(owner, () =>
      stream(relay, toCallback((v: number) => { values.push(v); })),
    );

    relay.next(10);
    relay.next(20);
    relay.complete();

    await done;
    expect(values).toEqual([10, 20]);
    await owner.dispose();
  });

  it('rejects with StreamDisposedError when the owner is disposed', async () => {
    const owner = createOwner('test-toCallback-dispose');
    const d = deferred<number>();

    const callbackPromise = withOwner(owner, () => stream(d, toCallback(() => {})));
    expect(owner.size).toBe(1);

    await owner.dispose();

    await expect(callbackPromise).rejects.toBeInstanceOf(StreamDisposedError);
  });

  it('accepts an explicit owner option', async () => {
    const owner = createOwner('explicit');
    const d = deferred<number>();

    const p = stream(d, toCallback(() => {}, { owner }));
    expect(owner.size).toBe(1);

    await owner.dispose();
    await expect(p).rejects.toBeInstanceOf(StreamDisposedError);
  });

  it('unregisters from the owner when source completes naturally', async () => {
    const owner = createOwner('test-toCallback-unregister');

    await withOwner(owner, async () => {
      const d = deferred<number>();
      const p = stream(d, toCallback(() => {}));
      expect(owner.size).toBe(1);
      d.resolve(1);
      await p;
    });

    expect(owner.size).toBe(0);
    await owner.dispose();
  });
});
