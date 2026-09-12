// ---------------------------------------------------------------------------
// pausable — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe } from '../stream.js';
import { createRelay } from '../relay.js';
import { pausable } from './pausable.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('pausable (exhaustive)', () => {
  it('three values — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, pausable(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('five values — buffer drain orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3, 4, 5], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, pausable(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3, 4, 5]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('single value', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([42], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, pausable(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([42]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty source', () => {
    exhaustiveTest((oracle) => {
      const src = testSource<number>([], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, pausable(), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });
});

describe('pausable (buffering)', () => {
  it('buffers values pushed while paused and delivers them on resume, before complete', () => {
    const relay = createRelay<number>();
    let pauseNext = true;
    const sink = testSink<number>({ oracle: { integer: () => (pauseNext ? 1 : 0) } });
    const s = pipe(relay, pausable(), assertProtocol()).connect(sink);
    s.resume();
    relay.next(1); // sink pauses
    relay.next(2);
    relay.next(3);
    relay.complete(); // arrives while values are queued
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(0);

    pauseNext = false;
    s.resume();
    expect(sink.values).toEqual([1, 2, 3]);
    expect(sink.completeCount).toBe(1);
    s.resume(); // no-op after terminal
    expect(sink.completeCount).toBe(1);
  });

  it('holds an error until the buffer has drained', () => {
    const relay = createRelay<number>();
    let pauseNext = true;
    const sink = testSink<number>({ oracle: { integer: () => (pauseNext ? 1 : 0) } });
    const s = pipe(relay, pausable(), assertProtocol()).connect(sink);
    s.resume();
    relay.next(1);
    relay.next(2);
    relay.error(new Error('late'));
    expect(sink.errors).toHaveLength(0);

    pauseNext = false;
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    expect(sink.errors).toHaveLength(1);
  });

  it('nothing is delivered after dispose', () => {
    const relay = createRelay<number>();
    const sink = testSink<number>({ oracle: { integer: () => 1 } });
    const s = pipe(relay, pausable()).connect(sink);
    s.resume();
    relay.next(1);
    relay.next(2); // buffered
    s[Symbol.dispose]();
    s.resume();
    relay.complete();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(0);
  });
});
