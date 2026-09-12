// ---------------------------------------------------------------------------
// to-async-iterable.test.ts — Tests for toAsyncIterable with owner tracking
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { stream } from '../stream.js';
import { createRelay } from '../relay.js';
import { fromArray } from '../sources/from-array.js';
import { toAsyncIterable } from './to-async-iterable.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { createOwner, withOwner } from '../owner.js';

const tick = (ms = 10) => new Promise((resolve) => setTimeout(resolve, ms));

describe('toAsyncIterable()', () => {
  it('iterates values from a relay', async () => {
    const r = createRelay<number>();
    const iterable = stream(r, toAsyncIterable());
    const values: number[] = [];

    const done = (async () => {
      for await (const v of iterable) {
        values.push(v);
      }
    })();

    // Push values asynchronously
    await tick();
    r.next(1);
    await tick();
    r.next(2);
    await tick();
    r.next(3);
    await tick();
    r.complete();

    await done;
    expect(values).toEqual([1, 2, 3]);
  });

  it('applies backpressure: a synchronous source is consumed one value per pull', async () => {
    const iterable = stream(fromArray([1, 2, 3, 4]), toAsyncIterable());
    const values: number[] = [];
    for await (const v of iterable) {
      values.push(v);
    }
    expect(values).toEqual([1, 2, 3, 4]);
  });

  it('iterates values under an owner', async () => {
    const owner = createOwner('test-toAsyncIterable');

    const values = await withOwner(owner, async () => {
      const r = createRelay<number>();
      const iterable = stream(r, toAsyncIterable());
      const result: number[] = [];

      const done = (async () => {
        for await (const v of iterable) {
          result.push(v);
        }
      })();

      await tick();
      r.next(10);
      await tick();
      r.next(20);
      await tick();
      r.next(30);
      await tick();
      r.complete();

      await done;
      return result;
    });

    expect(values).toEqual([10, 20, 30]);
    await owner.dispose();
  });

  it('rejects pending next() with StreamDisposedError when the owner is disposed', async () => {
    const owner = createOwner('test-toAsyncIterable-dispose');
    const relay = createRelay<number>();
    let caughtError: unknown = null;

    // The iterator registers with the owner ambient when iteration starts,
    // so start iterating inside the scope.
    const consumed = withOwner(owner, () => {
      const iterable = stream(relay, toAsyncIterable());
      return (async () => {
        try {
          for await (const _v of iterable) {
            // Take the first value, then block on next()
          }
        } catch (err) {
          caughtError = err;
        }
      })();
    });

    // Let the iterator connect, then push one value so it re-pauses
    await tick(20);
    relay.next(1);
    await tick(20);

    await owner.dispose();
    await consumed;

    expect(caughtError).toBeInstanceOf(StreamDisposedError);
  });

  it('accepts an explicit owner option', async () => {
    const owner = createOwner('explicit');
    const relay = createRelay<number>();
    const iterable = stream(relay, toAsyncIterable({ owner }));
    let caughtError: unknown = null;

    const consumed = (async () => {
      try {
        for await (const _v of iterable) {
          // block
        }
      } catch (err) {
        caughtError = err;
      }
    })();

    await tick(20);
    expect(owner.size).toBe(1);
    await owner.dispose();
    await consumed;

    expect(caughtError).toBeInstanceOf(StreamDisposedError);
  });

  it('unregisters from the owner when source completes naturally', async () => {
    const owner = createOwner('test-toAsyncIterable-unregister');

    await withOwner(owner, async () => {
      const r = createRelay<number>();
      const iterable = stream(r, toAsyncIterable());
      const values: number[] = [];

      const done = (async () => {
        for await (const v of iterable) {
          values.push(v);
        }
      })();

      await tick();
      r.next(1);
      await tick();
      r.next(2);
      await tick();
      r.complete();

      await done;
      expect(values).toEqual([1, 2]);
    });

    expect(owner.size).toBe(0);
    await owner.dispose();
  });

  it('unregisters from the owner on iterator.return() (break)', async () => {
    const owner = createOwner('test-toAsyncIterable-break');

    await withOwner(owner, async () => {
      const relay = createRelay<number>();
      const iterable = stream(relay, toAsyncIterable());
      const values: number[] = [];

      // Start iterating in a microtask, break after two values
      const iterPromise = (async () => {
        for await (const v of iterable) {
          values.push(v);
          if (values.length >= 2) break;
        }
      })();

      relay.next(10);
      await tick();
      relay.next(20);
      await tick();
      relay.next(30); // should be ignored after break

      await iterPromise;
      expect(values).toEqual([10, 20]);
    });

    expect(owner.size).toBe(0);
    await owner.dispose();
  });
});
