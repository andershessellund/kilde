// ---------------------------------------------------------------------------
// scheduleOn — tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe } from '../stream.js';
import { scheduleOn } from './schedule-on.js';
import { fromArray } from '../sources/from-array.js';
import { immediateScheduler, microtaskScheduler } from '../signal.js';
import { createRelay } from '../relay.js';
import type { Sink, Stream, Source, Scheduler } from '../types.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

// --- Manual scheduler: collects callbacks, flush on demand ---

function createManualScheduler(): Scheduler & { flush(): void; pending: number } {
  const queue: Array<() => void> = [];
  return {
    get pending() { return queue.length; },
    schedule(callback: () => void) {
      queue.push(callback);
    },
    flush() {
      while (queue.length > 0) {
        queue.shift()!();
      }
    },
  };
}

// --- Collect sink: records values, complete, and errors ---

function collectSink<T>(): Sink<T> & { values: T[]; completeCount: number; errors: unknown[] } {
  const state = {
    values: [] as T[],
    completeCount: 0,
    errors: [] as unknown[],
    next(value: T) { state.values.push(value); return undefined as undefined; },
    complete() { state.completeCount++; },
    error(err: unknown) { state.errors.push(err); },
  };
  return state;
}

// --- Push source: emit values imperatively ---

function createPushSource<T>(): Source<T> & { push(value: T): void; end(): void; fail(err: unknown): void } {
  let sink: Sink<T> | null = null;
  return {
    connect(s: Sink<T>): Stream {
      sink = s;
      return {
        resume() {},
        [Symbol.dispose]() { sink = null; },
      };
    },
    push(value: T) { sink?.next(value); },
    end() { sink?.complete(); },
    fail(err: unknown) { sink?.error(err); },
  };
}

describe('scheduleOn', () => {
  it('buffers values and delivers on flush', () => {
    const scheduler = createManualScheduler();
    const source = createPushSource<number>();
    const sink = collectSink<number>();

    const s = pipe(source, scheduleOn(scheduler)).connect(sink);
    s.resume();

    // Push 3 values synchronously
    source.push(1);
    source.push(2);
    source.push(3);

    // Nothing delivered yet
    expect(sink.values).toEqual([]);
    expect(scheduler.pending).toBe(1); // schedule called once (deduped)

    // Flush delivers all
    scheduler.flush();
    expect(sink.values).toEqual([1, 2, 3]);
  });

  it('schedule is called once for multiple synchronous emissions', () => {
    let scheduleCallCount = 0;
    const callbacks: Array<() => void> = [];
    const scheduler: Scheduler = {
      schedule(cb) {
        scheduleCallCount++;
        callbacks.push(cb);
      },
    };
    const source = createPushSource<number>();
    const sink = collectSink<number>();

    const s = pipe(source, scheduleOn(scheduler)).connect(sink);
    s.resume();

    source.push(1);
    source.push(2);
    source.push(3);

    expect(scheduleCallCount).toBe(1);

    // Flush
    callbacks.forEach(cb => cb());
    expect(sink.values).toEqual([1, 2, 3]);
  });

  it('forwards complete after buffer is drained', () => {
    const scheduler = createManualScheduler();
    const source = createPushSource<number>();
    const sink = collectSink<number>();

    const s = pipe(source, scheduleOn(scheduler)).connect(sink);
    s.resume();

    source.push(1);
    source.end();

    expect(sink.values).toEqual([]);
    expect(sink.completeCount).toBe(0);

    scheduler.flush();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(1);
  });

  it('forwards error after buffer is drained', () => {
    const scheduler = createManualScheduler();
    const source = createPushSource<number>();
    const sink = collectSink<number>();

    const s = pipe(source, scheduleOn(scheduler)).connect(sink);
    s.resume();

    source.push(1);
    source.fail(new Error('boom'));

    expect(sink.values).toEqual([]);
    expect(sink.errors).toHaveLength(0);

    scheduler.flush();
    expect(sink.values).toEqual([1]);
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('boom');
  });

  it('complete with no values', () => {
    const scheduler = createManualScheduler();
    const source = createPushSource<number>();
    const sink = collectSink<number>();

    const s = pipe(source, scheduleOn(scheduler)).connect(sink);
    s.resume();

    source.end();
    expect(sink.completeCount).toBe(0); // waits for the tick like values do

    scheduler.flush();
    expect(sink.values).toEqual([]);
    expect(sink.completeCount).toBe(1);
  });

  it('disposal stops delivery', () => {
    const scheduler = createManualScheduler();
    const source = createPushSource<number>();
    const sink = collectSink<number>();

    const s = pipe(source, scheduleOn(scheduler)).connect(sink);
    s.resume();

    source.push(1);
    source.push(2);

    // Dispose before flush
    s[Symbol.dispose]();

    scheduler.flush();
    expect(sink.values).toEqual([]); // Nothing delivered
    expect(sink.completeCount).toBe(0);
  });

  it('with immediateScheduler — pass-through', () => {
    const source = createPushSource<number>();
    const sink = collectSink<number>();

    const s = pipe(source, scheduleOn(immediateScheduler)).connect(sink);
    s.resume();

    source.push(1);
    expect(sink.values).toEqual([1]); // Delivered immediately

    source.push(2);
    expect(sink.values).toEqual([1, 2]);

    source.end();
    expect(sink.completeCount).toBe(1);
  });

  it('multiple flush cycles', () => {
    const scheduler = createManualScheduler();
    const source = createPushSource<number>();
    const sink = collectSink<number>();

    const s = pipe(source, scheduleOn(scheduler)).connect(sink);
    s.resume();

    // First batch
    source.push(1);
    source.push(2);
    scheduler.flush();
    expect(sink.values).toEqual([1, 2]);

    // Second batch
    source.push(3);
    source.push(4);
    scheduler.flush();
    expect(sink.values).toEqual([1, 2, 3, 4]);
  });

  it('synchronous source (fromArray) buffers everything', () => {
    const scheduler = createManualScheduler();
    const sink = collectSink<number>();

    const s = pipe(fromArray([10, 20, 30]), scheduleOn(scheduler)).connect(sink);
    s.resume();

    // All values buffered, plus complete
    expect(sink.values).toEqual([]);
    expect(sink.completeCount).toBe(0);

    scheduler.flush();
    expect(sink.values).toEqual([10, 20, 30]);
    expect(sink.completeCount).toBe(1);
  });

  it('post-disposal emissions are ignored', () => {
    const scheduler = createManualScheduler();
    const source = createPushSource<number>();
    const sink = collectSink<number>();

    const s = pipe(source, scheduleOn(scheduler)).connect(sink);
    s.resume();

    source.push(1);
    scheduler.flush();
    expect(sink.values).toEqual([1]);

    s[Symbol.dispose]();

    // Emissions after disposal are dropped
    source.push(2);
    scheduler.flush();
    expect(sink.values).toEqual([1]);
  });
});

describe('scheduleOn (bug 5 — downstream pause mid-flush)', () => {
  it('does not re-schedule; resume() drains the rest without a tick', () => {
    const scheduler = createManualScheduler();
    const source = createPushSource<number>();
    let pauseNext = true;
    const sink = testSink<number>({ oracle: { integer: () => (pauseNext ? 1 : 0) } });

    const s = pipe(source, scheduleOn(scheduler), assertProtocol()).connect(sink);
    s.resume();

    source.push(1);
    source.push(2);
    source.push(3);
    source.end();
    scheduler.flush(); // delivers 1, downstream pauses
    expect(sink.values).toEqual([1]);
    expect(sink.paused).toBe(true);
    expect(scheduler.pending).toBe(0); // no re-schedule
    expect(sink.completeCount).toBe(0);

    // Even if a tick fired now, nothing may be pushed into a paused sink
    // (assertProtocol would throw).
    scheduler.flush();
    expect(sink.values).toEqual([1]);

    pauseNext = false;
    s.resume(); // drains 2, 3 and then completes — exactly one resume
    expect(sink.values).toEqual([1, 2, 3]);
    expect(sink.completeCount).toBe(1);
    expect(scheduler.pending).toBe(0);
  });

  it('values arriving while paused wait for their own tick, then queue behind the drain', () => {
    const scheduler = createManualScheduler();
    const source = createPushSource<number>();
    let pauseNext = true;
    const sink = testSink<number>({ oracle: { integer: () => (pauseNext ? 1 : 0) } });

    const s = pipe(source, scheduleOn(scheduler), assertProtocol()).connect(sink);
    s.resume();

    source.push(1);
    source.push(2);
    scheduler.flush(); // 1 delivered, sink paused, 2 queued
    source.push(3); // new batch → new tick requested
    expect(scheduler.pending).toBe(1);

    pauseNext = false;
    s.resume(); // drains 2; 3 still waits for its tick
    expect(sink.values).toEqual([1, 2]);
    scheduler.flush();
    expect(sink.values).toEqual([1, 2, 3]);
  });

  it('resume() after the terminal is a no-op', () => {
    const sink = testSink<number>();
    const s = pipe(fromArray([1]), scheduleOn(immediateScheduler), assertProtocol()).connect(sink);
    s.resume();
    expect(sink.completeCount).toBe(1);
    s.resume();
    expect(sink.completeCount).toBe(1);
  });
});

describe('scheduleOn (exhaustive)', () => {
  it('immediate scheduler — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, scheduleOn(immediateScheduler), assertProtocol()).connect(sink);
      for (let i = 0; i < 40 && !sink.completeCount; i++) s.resume();
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('manual scheduler — all pause orderings, ticks between resumes', () => {
    exhaustiveTest((oracle) => {
      const scheduler = createManualScheduler();
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(src, scheduleOn(scheduler), assertProtocol()).connect(sink);
      for (let i = 0; i < 40 && !sink.completeCount; i++) {
        s.resume();
        scheduler.flush();
      }
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
    });
  });
});

describe('scheduleOn with the shared microtask scheduler', () => {
  it('two streams fed in one tick both deliver', async () => {
    const r1 = createRelay<number>();
    const r2 = createRelay<number>();
    const got1: number[] = [];
    const got2: number[] = [];
    const s1 = pipe(r1, scheduleOn(microtaskScheduler)).connect({ next(v) { got1.push(v); return undefined; }, complete() {}, error() {} });
    const s2 = pipe(r2, scheduleOn(microtaskScheduler)).connect({ next(v) { got2.push(v); return undefined; }, complete() {}, error() {} });
    s1.resume();
    s2.resume();
    r1.next(1);
    r2.next(2);
    await Promise.resolve();
    expect(got1).toEqual([1]);
    expect(got2).toEqual([2]);
  });
});
