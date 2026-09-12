// ---------------------------------------------------------------------------
// fromChannel — protocol tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { fromChannel } from './from-channel.js';
import { createChannel, ChanPendingReceivers, ChanCloseListeners } from '../channel.js';
import type { ChannelImpl } from '../channel.js';
import { pipe } from '../stream.js';
import { PAUSE } from '../types.js';
import type { Sink, PAUSE as PAUSETYPE } from '../types.js';
import { testSink } from '../testing/test-sink.js';
import { assertProtocol } from '../testing/protocol.js';

/** A sink that pauses on every value. */
function pausingSink<T>(): Sink<T> & { values: T[]; completeCount: number } {
  const sink = {
    values: [] as T[],
    completeCount: 0,
    next(v: T): undefined | PAUSETYPE {
      sink.values.push(v);
      return PAUSE;
    },
    complete() {
      sink.completeCount++;
    },
    error() {},
  };
  return sink;
}

function internals<T>(ch: unknown): ChannelImpl<T> {
  return ch as ChannelImpl<T>;
}

describe('fromChannel', () => {
  it('delivers nothing before the first resume()', () => {
    const ch = createChannel<number>(4);
    ch.putSync(1);
    const sink = testSink<number>();
    pipe(fromChannel(ch), assertProtocol()).connect(sink);
    expect(sink.values).toEqual([]);
  });

  it('drains buffered values and completes when closed', () => {
    const ch = createChannel<number>(4);
    ch.putSync(1);
    ch.putSync(2);
    const sink = testSink<number>();
    const s = pipe(fromChannel(ch), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    expect(sink.completeCount).toBe(0);
    ch.putSync(3);
    expect(sink.values).toEqual([1, 2, 3]);
    ch.close();
    expect(sink.completeCount).toBe(1);
  });

  it('completion deregisters from the channel and resume() afterwards is a no-op', () => {
    const ch = createChannel<number>(4);
    const impl = internals<number>(ch);
    const sink = testSink<number>();
    const s = pipe(fromChannel(ch), assertProtocol()).connect(sink);
    s.resume(); // nothing available — registers as pending receiver
    expect(impl[ChanPendingReceivers]).toHaveLength(1);
    expect(impl[ChanCloseListeners].size).toBe(1);
    ch.close();
    expect(sink.completeCount).toBe(1);
    expect(impl[ChanPendingReceivers]).toHaveLength(0);
    expect(impl[ChanCloseListeners].size).toBe(0);
    s.resume();
    s.resume();
    expect(sink.completeCount).toBe(1);
  });

  it('closed channel with buffered values: drains then completes on resume', () => {
    const ch = createChannel<number>(4);
    ch.putSync(1);
    ch.putSync(2);
    ch.close();
    const sink = testSink<number>();
    const s = pipe(fromChannel(ch), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    expect(sink.completeCount).toBe(1);
  });

  it('respects PAUSE — one value per resume()', () => {
    const ch = createChannel<number>(4);
    ch.putSync(1);
    ch.putSync(2);
    ch.close();
    const sink = pausingSink<number>();
    const s = pipe(fromChannel(ch), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.values).toEqual([1]);
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    expect(sink.completeCount).toBe(0);
    s.resume();
    expect(sink.completeCount).toBe(1);
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    expect(sink.completeCount).toBe(1);
  });

  it('while paused, values stay in the channel and close does not complete', () => {
    const ch = createChannel<number>(4);
    const sink = pausingSink<number>();
    const s = pipe(fromChannel(ch), assertProtocol()).connect(sink);
    s.resume(); // pending receiver
    ch.putSync(1); // delivered, sink pauses
    expect(sink.values).toEqual([1]);
    ch.putSync(2);
    ch.close();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(0);
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    s.resume();
    expect(sink.completeCount).toBe(1);
  });

  it('dispose mid-flight deregisters and stops delivery', () => {
    const ch = createChannel<number>(4);
    const impl = internals<number>(ch);
    const sink = testSink<number>();
    const s = pipe(fromChannel(ch), assertProtocol()).connect(sink);
    s.resume();
    expect(impl[ChanPendingReceivers]).toHaveLength(1);
    s[Symbol.dispose]();
    expect(impl[ChanPendingReceivers]).toHaveLength(0);
    expect(impl[ChanCloseListeners].size).toBe(0);
    ch.putSync(1);
    ch.close();
    s.resume();
    expect(sink.values).toEqual([]);
    expect(sink.completeCount).toBe(0);
    expect(ch.canTakeSync).toBe(true); // value still in the channel
  });

  it('dispose from inside next() stops the pull loop', () => {
    const ch = createChannel<number>(4);
    ch.putSync(1);
    ch.putSync(2);
    ch.putSync(3);
    const values: number[] = [];
    const s: { resume(): void; [Symbol.dispose](): void } = fromChannel(ch).connect({
      next(v: number) {
        values.push(v);
        if (v === 2) s[Symbol.dispose]();
        return undefined;
      },
      complete() {},
      error() {},
    });
    s.resume();
    expect(values).toEqual([1, 2]);
    expect(ch.canTakeSync).toBe(true);
  });

  it('competing consumers each get every value at most once', () => {
    const ch = createChannel<number>(8);
    const a = testSink<number>();
    const b = testSink<number>();
    fromChannel(ch).connect(a).resume();
    fromChannel(ch).connect(b).resume();
    for (let i = 0; i < 6; i++) ch.putSync(i);
    ch.close();
    expect([...a.values, ...b.values].sort()).toEqual([0, 1, 2, 3, 4, 5]);
    expect(a.completeCount).toBe(1);
    expect(b.completeCount).toBe(1);
  });

  it('async: values put later are delivered as they arrive', async () => {
    const ch = createChannel<string>();
    const sink = testSink<string>();
    const s = pipe(fromChannel(ch), assertProtocol()).connect(sink);
    s.resume();
    await new Promise((r) => setTimeout(r, 0));
    ch.putSync('a');
    await new Promise((r) => setTimeout(r, 0));
    ch.putSync('b');
    ch.close();
    await new Promise((r) => setTimeout(r, 0));
    expect(sink.values).toEqual(['a', 'b']);
    expect(sink.completeCount).toBe(1);
  });
});
