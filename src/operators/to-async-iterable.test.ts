// ---------------------------------------------------------------------------
// to-async-iterable.test.ts — Tests for toAsyncIterable with owner tracking
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source, Sink } from '../types.js';
import { stream, pipe } from '../stream.js';
import { createRelay } from '../relay.js';
import { fromArray } from '../sources/from-array.js';
import { toAsyncIterable } from './to-async-iterable.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { createOwner, withOwner } from '../owner.js';
import { testSink } from '../testing/test-sink.js';
import { assertProtocol } from '../testing/protocol.js';

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

  it('bug 10: concurrent next() calls are served FIFO', async () => {
    const relay = createRelay<number>();
    const it = stream(relay, toAsyncIterable())[Symbol.asyncIterator]();
    const p1 = it.next();
    const p2 = it.next();
    const p3 = it.next();
    relay.next(1);
    relay.next(2);
    relay.next(3);
    expect(await p1).toEqual({ value: 1, done: false });
    expect(await p2).toEqual({ value: 2, done: false });
    expect(await p3).toEqual({ value: 3, done: false });
    relay.complete();
    expect(await it.next()).toEqual({ value: undefined, done: true });
  });

  it('bug 10: resumes the upstream once per pause, not once per pull', () => {
    let resumes = 0;
    const relay = createRelay<number>();
    const src: Source<number> = {
      connect(sink: Sink<number>) {
        const s = relay.connect(sink);
        return {
          resume() {
            resumes++;
            s.resume();
          },
          [Symbol.dispose]: () => s[Symbol.dispose](),
        };
      },
    };
    const it = stream(src, toAsyncIterable())[Symbol.asyncIterator]();
    void it.next();
    void it.next();
    expect(resumes).toBe(1);
    relay.next(1); // serves the first pull; a second is waiting → no PAUSE
    relay.next(2); // serves the second → PAUSE
    void it.next(); // needs a resume again
    expect(resumes).toBe(2);
    void it.return?.();
  });

  it('bug 10: return() while a next() is pending settles it with done: true', async () => {
    const relay = createRelay<number>();
    const it = stream(relay, toAsyncIterable())[Symbol.asyncIterator]();
    const pending = it.next();
    const pending2 = it.next();
    expect(await it.return!()).toEqual({ value: undefined, done: true });
    expect(await pending).toEqual({ value: undefined, done: true });
    expect(await pending2).toEqual({ value: undefined, done: true });
    // The upstream is gone: later pushes are not observed
    relay.next(5);
    expect(await it.next()).toEqual({ value: undefined, done: true });
  });

  it('complete() arriving while paused: the buffered value is yielded, then done', async () => {
    // A source that ignores PAUSE and completes right after its value.
    const src: Source<number> = {
      connect(sink: Sink<number>) {
        return {
          resume() {
            sink.next(1);
            sink.next(2);
            sink.complete();
          },
          [Symbol.dispose]() {},
        };
      },
    };
    const it = stream(src, toAsyncIterable())[Symbol.asyncIterator]();
    expect(await it.next()).toEqual({ value: 1, done: false });
    expect(await it.next()).toEqual({ value: 2, done: false });
    expect(await it.next()).toEqual({ value: undefined, done: true });
    expect(await it.next()).toEqual({ value: undefined, done: true });
  });

  it('error() arriving while paused: the buffered value is yielded, then the error', async () => {
    const src: Source<number> = {
      connect(sink: Sink<number>) {
        return {
          resume() {
            sink.next(1);
            sink.error(new Error('late'));
          },
          [Symbol.dispose]() {},
        };
      },
    };
    const it = stream(src, toAsyncIterable())[Symbol.asyncIterator]();
    expect(await it.next()).toEqual({ value: 1, done: false });
    await expect(it.next()).rejects.toThrow('late');
    expect(await it.next()).toEqual({ value: undefined, done: true });
  });

  it('error with several pulls pending: first rejects, the rest are done', async () => {
    const relay = createRelay<number>();
    const it = stream(relay, toAsyncIterable())[Symbol.asyncIterator]();
    const p1 = it.next();
    const p2 = it.next();
    relay.error(new Error('boom'));
    await expect(p1).rejects.toThrow('boom');
    expect(await p2).toEqual({ value: undefined, done: true });
  });

  it('bug 13: an already-disposed owner never connects; next() rejects', async () => {
    const owner = createOwner('dead');
    await owner.dispose();
    let connects = 0;
    const src: Source<number> = {
      connect(sink: Sink<number>) {
        connects++;
        return fromArray([1]).connect(sink);
      },
    };
    const it = stream(src, toAsyncIterable({ owner }))[Symbol.asyncIterator]();
    await expect(it.next()).rejects.toBeInstanceOf(StreamDisposedError);
    expect(connects).toBe(0);
  });

  it('bug 12: a second resume() emits only one iterable and one complete()', () => {
    const sink = testSink<AsyncIterable<number>>();
    const s = pipe(fromArray([1]), toAsyncIterable(), assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    expect(sink.values).toHaveLength(1);
    expect(sink.completeCount).toBe(1);
  });
});
