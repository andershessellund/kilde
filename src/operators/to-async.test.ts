// ---------------------------------------------------------------------------
// to-async.test.ts — Tests for toAsync() operator
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { stream, pipe } from '../stream.js';
import { deferred } from '../sources/deferred.js';
import { fromArray } from '../sources/from-array.js';
import { map } from './map.js';
import { toAsync } from './to-async.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { createOwner, withOwner } from '../owner.js';
import { testSink } from '../testing/test-sink.js';
import { assertProtocol } from '../testing/protocol.js';

describe('toAsync()', () => {
  it('produces a thunk that resolves with the first value', async () => {
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

  it('each call of the thunk opens a fresh connection', async () => {
    const fn = stream(fromArray([7]), toAsync());
    expect(await fn()).toBe(7);
    expect(await fn()).toBe(7);
  });

  it('bug 12: a second resume() emits only one thunk and one complete()', () => {
    const sink = testSink<() => Promise<number>>();
    const s = pipe(fromArray([1]), toAsync(), assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    expect(sink.values).toHaveLength(1);
    expect(sink.completeCount).toBe(1);
  });
});
