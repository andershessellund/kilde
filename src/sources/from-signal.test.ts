// ---------------------------------------------------------------------------
// fromSignal — protocol tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { fromSignal } from './from-signal.js';
import { createSignal } from '../signal.js';
import { pipe } from '../stream.js';
import { PAUSE } from '../types.js';
import type { Sink, PAUSE as PAUSETYPE } from '../types.js';
import { testSink } from '../testing/test-sink.js';
import { assertProtocol } from '../testing/protocol.js';

/** A sink that pauses after every value. */
function pausingSink<T>(): Sink<T> & { values: T[] } {
  const sink = {
    values: [] as T[],
    next(v: T): undefined | PAUSETYPE {
      sink.values.push(v);
      return PAUSE;
    },
    complete() {},
    error() {},
  };
  return sink;
}

describe('fromSignal', () => {
  it('delivers nothing before the first resume()', () => {
    const sig = createSignal(0);
    const sink = testSink<number>();
    pipe(fromSignal(sig), assertProtocol()).connect(sink);
    sig.set(1);
    expect(sink.values).toEqual([]);
  });

  it('delivers the current value on resume, then each change', () => {
    const sig = createSignal(0);
    const sink = testSink<number>();
    const s = pipe(fromSignal(sig), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.values).toEqual([0]);
    sig.set(1);
    sig.set(2);
    expect(sink.values).toEqual([0, 1, 2]);
    s[Symbol.dispose]();
  });

  it('resume() while already active is a no-op — never re-delivers', () => {
    const sig = createSignal('a');
    const sink = testSink<string>();
    const s = pipe(fromSignal(sig), assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    s.resume();
    expect(sink.values).toEqual(['a']);
    sig.set('b');
    s.resume();
    expect(sink.values).toEqual(['a', 'b']);
    s[Symbol.dispose]();
  });

  it('conflates while paused: only the latest value is delivered on resume', () => {
    const sig = createSignal(0);
    const sink = pausingSink<number>();
    const s = pipe(fromSignal(sig), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.values).toEqual([0]);
    sig.set(1);
    sig.set(2);
    sig.set(3);
    expect(sink.values).toEqual([0]); // paused
    s.resume();
    expect(sink.values).toEqual([0, 3]);
    s[Symbol.dispose]();
  });

  it('resume() after a pause with no change delivers nothing', () => {
    const sig = createSignal(0);
    const sink = pausingSink<number>();
    const s = pipe(fromSignal(sig), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.values).toEqual([0]);
    s.resume();
    s.resume();
    expect(sink.values).toEqual([0]);
    sig.set(1);
    s.resume();
    expect(sink.values).toEqual([0, 1]);
    s.resume(); // paused again after 1, nothing changed
    expect(sink.values).toEqual([0, 1]);
    s[Symbol.dispose]();
  });

  it('a change back to the delivered value while paused is still a (conflated) change', () => {
    // Only "changed while paused" is tracked, not "differs from last delivered".
    const sig = createSignal(0);
    const sink = pausingSink<number>();
    const s = fromSignal(sig).connect(sink);
    s.resume();
    sig.set(1);
    sig.set(0);
    s.resume();
    expect(sink.values).toEqual([0, 0]);
    s[Symbol.dispose]();
  });

  it('dispose unsubscribes; nothing is delivered afterwards', () => {
    const sig = createSignal(0);
    const sink = testSink<number>();
    const s = pipe(fromSignal(sig), assertProtocol()).connect(sink);
    s.resume();
    s[Symbol.dispose]();
    sig.set(1);
    s.resume();
    expect(sink.values).toEqual([0]);
    expect(sig.observed).toBe(false);
  });

  it('dispose before resume never subscribes', () => {
    const sig = createSignal(0);
    const sink = testSink<number>();
    const s = fromSignal(sig).connect(sink);
    s[Symbol.dispose]();
    s.resume();
    expect(sink.values).toEqual([]);
    expect(sig.observed).toBe(false);
  });

  it('each connection is independent', () => {
    const sig = createSignal(0);
    const a = pausingSink<number>();
    const b = testSink<number>();
    const sa = fromSignal(sig).connect(a);
    const sb = fromSignal(sig).connect(b);
    sa.resume();
    sb.resume();
    sig.set(1);
    sig.set(2);
    expect(a.values).toEqual([0]);
    expect(b.values).toEqual([0, 1, 2]);
    sa.resume();
    expect(a.values).toEqual([0, 2]);
    sa[Symbol.dispose]();
    sb[Symbol.dispose]();
  });

  it('a sink that pauses on the initial value gets the conflated latest on resume', () => {
    const sig = createSignal(0);
    let count = 0;
    const values: number[] = [];
    const s = fromSignal(sig).connect({
      next(v: number) {
        values.push(v);
        return ++count === 1 ? PAUSE : undefined;
      },
      complete() {},
      error() {},
    });
    s.resume(); // 0, paused
    sig.set(5);
    s.resume(); // 5, unpaused
    sig.set(6); // delivered directly
    expect(values).toEqual([0, 5, 6]);
    s[Symbol.dispose]();
  });
});
