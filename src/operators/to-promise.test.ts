// ---------------------------------------------------------------------------
// to-promise.test.ts — Tests for toPromise with owner tracking
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { stream } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { deferred } from '../sources/deferred.js';
import { map } from './map.js';
import { toPromise } from './to-promise.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { createOwner, withOwner } from '../owner.js';

describe('toPromise()', () => {
  it('resolves with first value (no owner)', () => {
    const d = deferred<number>();
    const p = stream(d, toPromise());
    d.resolve(42);
    return expect(p).resolves.toBe(42);
  });

  it('takes the first value from a multi-value source', () => {
    const p = stream(fromArray([1, 2, 3]), toPromise());
    return expect(p).resolves.toBe(1);
  });

  it('rejects when source errors (no owner)', async () => {
    const d = deferred<number>();
    const p = stream(d, toPromise());
    // Catch the deferred's internal promise to prevent unhandled rejection
    d.promise.catch(() => {});
    const assertion = expect(p).rejects.toThrow('boom');
    d.reject(new Error('boom'));
    await assertion;
  });

  it('resolves normally under an owner', async () => {
    const owner = createOwner('test-toPromise');

    const result = await withOwner(owner, () => {
      const d = deferred<number>();
      const p = stream(d, toPromise());
      d.resolve(99);
      return p;
    });

    expect(result).toBe(99);
    await owner.dispose();
  });

  it('works with operators in the pipeline', async () => {
    const owner = createOwner('test-toPromise-pipeline');

    const result = await withOwner(owner, () => {
      const d = deferred<number>();
      const p = stream(d, map((x: number) => x * 2), toPromise());
      d.resolve(21);
      return p;
    });

    expect(result).toBe(42);
    await owner.dispose();
  });

  it('rejects with StreamDisposedError when the owner is disposed', async () => {
    const owner = createOwner('test-toPromise-dispose');
    const d = deferred<number>();

    const promise = withOwner(owner, () => stream(d, toPromise()));
    expect(owner.size).toBe(1);

    await owner.dispose();

    await expect(promise).rejects.toBeInstanceOf(StreamDisposedError);
  });

  it('accepts an explicit owner option', async () => {
    const owner = createOwner('explicit');
    const d = deferred<number>();

    const promise = stream(d, toPromise({ owner }));
    expect(owner.size).toBe(1);

    await owner.dispose();
    await expect(promise).rejects.toBeInstanceOf(StreamDisposedError);
  });

  it('unregisters from the owner when source completes naturally', async () => {
    const owner = createOwner('test-toPromise-unregister');

    await withOwner(owner, async () => {
      const d = deferred<number>();
      const p = stream(d, toPromise());
      expect(owner.size).toBe(1);
      d.resolve(42);
      const result = await p;
      expect(result).toBe(42);
    });

    expect(owner.size).toBe(0);
    await owner.dispose();
  });
});
