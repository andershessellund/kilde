// ---------------------------------------------------------------------------
// scheduleOn — tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe } from '../stream.js';
import { scheduleOn } from './schedule-on.js';
import { fromArray } from '../sources/from-array.js';
import { immediateScheduler } from '../signal.js';
import type { Sink, Stream, Source, Scheduler } from '../types.js';

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
