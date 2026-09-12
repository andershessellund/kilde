// ---------------------------------------------------------------------------
// channel / take / put / closed / intoChannel — tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { createChannel, droppingBuffer, slidingBuffer, unboundedBuffer } from './channel.js';
import type { ReadChannel, WriteChannel } from './channel.js';
import { select, UnhandledCloseError } from './select.js';
import { take } from './choices/take.js';
import { put } from './choices/put.js';
import { closed } from './choices/closed.js';
import { timeout } from './choices/timeout.js';
import { intoChannel } from './operators/into-channel.js';
import { fromChannel } from './sources/from-channel.js';
import { stream } from './stream.js';
import { fromArray } from './sources/from-array.js';
import { createRelay } from './relay.js';
import { map } from './operators/map.js';
import { toArray } from './operators/to-array.js';
import { PAUSE } from './types.js';

// ---------------------------------------------------------------------------
// createChannel() basics
// ---------------------------------------------------------------------------

describe('createChannel()', () => {
  it('creates an open channel', () => {
    const ch = createChannel<number>();
    expect(ch.closed).toBe(false);
  });

  it('closing a channel sets closed to true', () => {
    const ch = createChannel<number>();
    ch.close();
    expect(ch.closed).toBe(true);
  });

  it('closing is idempotent', () => {
    const ch = createChannel<number>();
    ch.close();
    ch.close();
    expect(ch.closed).toBe(true);
  });

  it('rejects invalid buffer sizes', () => {
    expect(() => createChannel(-1)).toThrow('non-negative integer');
    expect(() => createChannel(1.5)).toThrow('non-negative integer');
  });
});

// ---------------------------------------------------------------------------
// Buffered channel — put/take
// ---------------------------------------------------------------------------

describe('buffered channel', () => {
  it('put then take — single value', async () => {
    const ch = createChannel<string>(1);
    await put(ch, 'hello');
    const v = await take(ch);
    expect(v).toBe('hello');
  });

  it('multiple values — FIFO order', async () => {
    const ch = createChannel<number>(3);
    await put(ch, 1);
    await put(ch, 2);
    await put(ch, 3);

    expect(await take(ch)).toBe(1);
    expect(await take(ch)).toBe(2);
    expect(await take(ch)).toBe(3);
  });

  it('take blocks until put provides a value', async () => {
    const ch = createChannel<number>(1);
    let received: number | undefined;

    const takePromise = (async () => {
      received = await take(ch);
    })();

    // Value not yet available
    await Promise.resolve();
    expect(received).toBe(undefined);

    await put(ch, 42);
    await takePromise;
    expect(received).toBe(42);
  });

  it('put blocks when buffer is full', async () => {
    const ch = createChannel<number>(1);
    await put(ch, 1); // fills buffer

    let putDone = false;
    const putPromise = (async () => {
      await put(ch, 2);
      putDone = true;
    })();

    await Promise.resolve();
    await Promise.resolve();
    expect(putDone).toBe(false);

    // Taking frees buffer space — put should complete
    const v = await take(ch);
    expect(v).toBe(1);
    await putPromise;
    expect(putDone).toBe(true);

    // Second value is now in buffer
    expect(await take(ch)).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Rendezvous channel (buffer = 0)
// ---------------------------------------------------------------------------

describe('rendezvous channel', () => {
  it('put blocks until take is ready', async () => {
    const ch = createChannel<string>();
    let putDone = false;

    const putPromise = (async () => {
      await put(ch, 'msg');
      putDone = true;
    })();

    await Promise.resolve();
    await Promise.resolve();
    expect(putDone).toBe(false);

    const v = await take(ch);
    expect(v).toBe('msg');
    await putPromise;
    expect(putDone).toBe(true);
  });

  it('take blocks until put is ready', async () => {
    const ch = createChannel<number>();
    let received: number | undefined;

    const takePromise = (async () => {
      received = await take(ch);
    })();

    await Promise.resolve();
    expect(received).toBe(undefined);

    await put(ch, 99);
    await takePromise;
    expect(received).toBe(99);
  });
});

// ---------------------------------------------------------------------------
// Channel close
// ---------------------------------------------------------------------------

describe('channel close', () => {
  it('put on closed channel throws', () => {
    const ch = createChannel<number>(1);
    ch.close();
    expect(() => put(ch, 1)).toThrow('closed channel');
  });

  it('take on closed empty channel throws UnhandledCloseError (lazy)', async () => {
    const ch = createChannel<number>();
    ch.close();
    await expect(take(ch)).rejects.toBeInstanceOf(UnhandledCloseError);
  });

  it('take on closed channel with buffered data succeeds', async () => {
    const ch = createChannel<number>(2);
    await put(ch, 1);
    await put(ch, 2);
    ch.close();

    expect(await take(ch)).toBe(1);
    expect(await take(ch)).toBe(2);
    // Now empty + closed
    await expect(take(ch)).rejects.toBeInstanceOf(UnhandledCloseError);
  });

  it('closed() choice fires on close', async () => {
    const ch = createChannel<number>();
    const sel = select({ done: closed(ch) });
    ch.close();
    const result = await sel;
    expect(result.tag).toBe('done');
  });

  it('closed() on already-closed channel fires immediately', async () => {
    const ch = createChannel<number>();
    ch.close();
    const result = await select({ done: closed(ch) });
    expect(result.tag).toBe('done');
  });

  it('select take + closed — close fires closed branch', async () => {
    const ch = createChannel<number>();
    const sel = select({
      msg: take(ch),
      done: closed(ch),
    });
    ch.close();
    const result = await sel;
    expect(result.tag).toBe('done');
  });

  it('select take + closed — value fires msg branch', async () => {
    const ch = createChannel<number>(1);
    await put(ch, 42);
    const result = await select({
      msg: take(ch),
      done: closed(ch),
    });
    expect(result.tag).toBe('msg');
    expect(result.value).toBe(42);
  });
});

// ---------------------------------------------------------------------------
// select with timeout on channel
// ---------------------------------------------------------------------------

describe('select with channel + timeout', () => {
  it('take wins over timeout when data is ready', async () => {
    const ch = createChannel<string>(1);
    await put(ch, 'fast');
    const result = await select({
      msg: take(ch),
      timeout: timeout(1000),
    });
    expect(result.tag).toBe('msg');
    expect(result.value).toBe('fast');
  });
});

// ---------------------------------------------------------------------------
// intoChannel()
// ---------------------------------------------------------------------------

describe('intoChannel()', () => {
  it('pipes array source into buffered channel', async () => {
    const ch = createChannel<number>(10);
    const src = fromArray([1, 2, 3]);

    await stream(src, intoChannel(ch));

    expect(await take(ch)).toBe(1);
    expect(await take(ch)).toBe(2);
    expect(await take(ch)).toBe(3);
    expect(ch.closed).toBe(true);
  });

  it('pipe into already-closed channel resolves immediately', async () => {
    const ch = createChannel<number>(1);
    ch.close();
    await stream(fromArray([1, 2, 3]), intoChannel(ch));
    // resolves without error
  });

  it('relay source — pipe and take concurrently', async () => {
    const ch = createChannel<string>(2);
    const relay = createRelay<string>();

    const pipePromise = stream(relay, intoChannel(ch));

    relay.next('a');
    relay.next('b');

    expect(await take(ch)).toBe('a');
    expect(await take(ch)).toBe('b');

    relay.complete();
    await pipePromise;
    expect(ch.closed).toBe(true);
  });

  it('source error rejects and closes the channel', async () => {
    const ch = createChannel<string>(2);
    const relay = createRelay<string>();

    const pipePromise = stream(relay, intoChannel(ch));

    relay.next('ok');
    relay.error(new Error('src-fail'));

    await expect(pipePromise).rejects.toThrow('src-fail');
    expect(ch.closed).toBe(true);

    // Can still take buffered value
    expect(await take(ch)).toBe('ok');
  });

  it('close: false keeps channel open after source completes', async () => {
    const ch = createChannel<number>(10);
    const src = fromArray([1, 2, 3]);

    await stream(src, intoChannel(ch, { close: false }));

    expect(ch.closed).toBe(false);
    expect(await take(ch)).toBe(1);
    expect(await take(ch)).toBe(2);
    expect(await take(ch)).toBe(3);
  });

  it('close: false keeps channel open after source errors', async () => {
    const ch = createChannel<string>(2);
    const relay = createRelay<string>();

    const pipePromise = stream(relay, intoChannel(ch, { close: false }));

    relay.next('ok');
    relay.error(new Error('fail'));

    await expect(pipePromise).rejects.toThrow('fail');
    expect(ch.closed).toBe(false);
    expect(await take(ch)).toBe('ok');
  });

  it('works with .stream() method', async () => {
    const ch = createChannel<number>(10);
    await fromArray([10, 20]).stream(intoChannel(ch));

    expect(await take(ch)).toBe(10);
    expect(await take(ch)).toBe(20);
    expect(ch.closed).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Dropping buffer
// ---------------------------------------------------------------------------

describe('droppingBuffer', () => {
  it('rejects invalid sizes', () => {
    expect(() => droppingBuffer(0)).toThrow('positive integer');
    expect(() => droppingBuffer(-1)).toThrow('positive integer');
    expect(() => droppingBuffer(1.5)).toThrow('positive integer');
  });

  it('accepts values up to capacity', async () => {
    const ch = createChannel<number>(droppingBuffer(2));
    await put(ch, 1);
    await put(ch, 2);
    expect(await take(ch)).toBe(1);
    expect(await take(ch)).toBe(2);
  });

  it('drops new values when full — sender never blocks', async () => {
    const ch = createChannel<number>(droppingBuffer(2));
    await put(ch, 1);
    await put(ch, 2);
    await put(ch, 3); // dropped — does NOT block
    await put(ch, 4); // dropped

    expect(await take(ch)).toBe(1);
    expect(await take(ch)).toBe(2);
  });

  it('put after drain works normally', async () => {
    const ch = createChannel<number>(droppingBuffer(1));
    await put(ch, 1);
    await put(ch, 2); // dropped
    expect(await take(ch)).toBe(1);

    await put(ch, 3); // space available now
    expect(await take(ch)).toBe(3);
  });
});

// ---------------------------------------------------------------------------
// Sliding buffer
// ---------------------------------------------------------------------------

describe('slidingBuffer', () => {
  it('rejects invalid sizes', () => {
    expect(() => slidingBuffer(0)).toThrow('positive integer');
    expect(() => slidingBuffer(-1)).toThrow('positive integer');
  });

  it('accepts values up to capacity', async () => {
    const ch = createChannel<number>(slidingBuffer(2));
    await put(ch, 1);
    await put(ch, 2);
    expect(await take(ch)).toBe(1);
    expect(await take(ch)).toBe(2);
  });

  it('evicts oldest when full — sender never blocks', async () => {
    const ch = createChannel<number>(slidingBuffer(2));
    await put(ch, 1);
    await put(ch, 2);
    await put(ch, 3); // evicts 1
    await put(ch, 4); // evicts 2

    expect(await take(ch)).toBe(3);
    expect(await take(ch)).toBe(4);
  });

  it('slidingBuffer(1) keeps only latest value', async () => {
    const ch = createChannel<string>(slidingBuffer(1));
    await put(ch, 'a');
    await put(ch, 'b');
    await put(ch, 'c');

    expect(await take(ch)).toBe('c');
  });
});

// ---------------------------------------------------------------------------
// put() dead-end on channel close (bug fix)
// ---------------------------------------------------------------------------

describe('put() on channel close', () => {
  it('pending put throws UnhandledCloseError when channel closes', async () => {
    const ch = createChannel<number>(); // rendezvous
    const sel = select({
      sent: put(ch, 42),
    });
    // Close channel while put is pending
    ch.close();
    await expect(sel).rejects.toBeInstanceOf(UnhandledCloseError);
  });

  it('pending put with closed() handler resolves to closed branch', async () => {
    const ch = createChannel<number>(); // rendezvous
    const sel = select({
      sent: put(ch, 42),
      done: closed(ch),
    });
    ch.close();
    const result = await sel;
    expect(result.tag).toBe('done');
  });
});

// ---------------------------------------------------------------------------
// fromChannel()
// ---------------------------------------------------------------------------

describe('fromChannel()', () => {
  it('pulls buffered values and completes on close', async () => {
    const ch = createChannel<number>(10);
    await put(ch, 1);
    await put(ch, 2);
    await put(ch, 3);
    ch.close();

    const result = stream(fromChannel(ch), toArray());
    expect(result).toEqual([1, 2, 3]);
  });

  it('completes immediately on already-closed empty channel', async () => {
    const ch = createChannel<number>(1);
    ch.close();

    const result = stream(fromChannel(ch), toArray());
    expect(result).toEqual([]);
  });

  it('pulls from rendezvous channel with concurrent puts', async () => {
    const ch = createChannel<string>(); // rendezvous
    const values: string[] = [];

    const src = fromChannel(ch);
    const conn = src.connect({
      next(v) { values.push(v); return undefined; },
      complete() {},
      error() {},
    });
    conn.resume();

    // Now put values — each should be delivered to the waiting receiver
    await put(ch, 'a');
    await put(ch, 'b');

    expect(values).toEqual(['a', 'b']);

    conn[Symbol.dispose]();
  });

  it('respects PAUSE backpressure', async () => {
    const ch = createChannel<number>(10);
    await put(ch, 1);
    await put(ch, 2);
    await put(ch, 3);

    const values: number[] = [];
    const src = fromChannel(ch);
    const conn = src.connect({
      next(v) {
        values.push(v);
        // Pause after first value
        if (values.length === 1) return PAUSE;
        return undefined;
      },
      complete() {},
      error() {},
    });
    conn.resume();

    // Should have taken only 1 value due to PAUSE
    expect(values).toEqual([1]);

    // Resume — should take 2 and 3
    conn.resume();
    expect(values).toEqual([1, 2, 3]);

    conn[Symbol.dispose]();
  });

  it('respects PAUSE backpressure with real PAUSE symbol', async () => {
    const ch = createChannel<number>(10);
    await put(ch, 10);
    await put(ch, 20);
    await put(ch, 30);

    const values: number[] = [];
    const conn = fromChannel(ch).connect({
      next(v) {
        values.push(v);
        return values.length === 1 ? PAUSE : undefined;
      },
      complete() {},
      error() {},
    });
    conn.resume();
    expect(values).toEqual([10]);

    conn.resume();
    expect(values).toEqual([10, 20, 30]);
    conn[Symbol.dispose]();
  });

  it('completes when channel closes while waiting', async () => {
    const ch = createChannel<number>(); // rendezvous
    let completed = false;

    const conn = fromChannel(ch).connect({
      next() { return undefined; },
      complete() { completed = true; },
      error() {},
    });
    conn.resume();

    expect(completed).toBe(false);
    ch.close();
    expect(completed).toBe(true);
  });

  it('drains buffer then completes on close', async () => {
    const ch = createChannel<number>(5);
    await put(ch, 1);
    await put(ch, 2);
    await put(ch, 3);
    ch.close();

    const values: number[] = [];
    let completed = false;

    const conn = fromChannel(ch).connect({
      next(v) { values.push(v); return undefined; },
      complete() { completed = true; },
      error() {},
    });
    conn.resume();

    expect(values).toEqual([1, 2, 3]);
    expect(completed).toBe(true);
  });

  it('dispose stops consuming without affecting channel', async () => {
    const ch = createChannel<number>(10);
    await put(ch, 1);
    await put(ch, 2);

    const values: number[] = [];
    const conn = fromChannel(ch).connect({
      next(v) { values.push(v); return undefined; },
      complete() {},
      error() {},
    });
    conn.resume();
    expect(values).toEqual([1, 2]);

    conn[Symbol.dispose]();

    // Channel is still open and functional
    expect(ch.closed).toBe(false);
    await put(ch, 3);
    expect(await take(ch)).toBe(3);
  });

  it('competing consumers — each value goes to exactly one', async () => {
    const ch = createChannel<number>(10);
    const valuesA: number[] = [];
    const valuesB: number[] = [];

    const connA = fromChannel(ch).connect({
      next(v) { valuesA.push(v); return undefined; },
      complete() {},
      error() {},
    });
    const connB = fromChannel(ch).connect({
      next(v) { valuesB.push(v); return undefined; },
      complete() {},
      error() {},
    });

    connA.resume();
    connB.resume();

    await put(ch, 1);
    await put(ch, 2);
    await put(ch, 3);
    await put(ch, 4);

    // Each value should go to exactly one consumer
    expect(valuesA.length + valuesB.length).toBe(4);
    const all = [...valuesA, ...valuesB].sort();
    expect(all).toEqual([1, 2, 3, 4]);

    connA[Symbol.dispose]();
    connB[Symbol.dispose]();
  });

  it('works with operators (map + toArray)', async () => {
    const ch = createChannel<number>(10);
    await put(ch, 1);
    await put(ch, 2);
    await put(ch, 3);
    ch.close();

    const result = stream(
      fromChannel(ch),
      map(x => x * 10),
      toArray(),
    );
    expect(result).toEqual([10, 20, 30]);
  });

  it('works with .stream() method', async () => {
    const ch = createChannel<number>(10);
    await put(ch, 5);
    await put(ch, 6);
    ch.close();

    const result = fromChannel(ch).stream(toArray());
    expect(result).toEqual([5, 6]);
  });

  it('fromChannel → intoChannel pipes between channels', async () => {
    const input = createChannel<number>(10);
    const output = createChannel<number>(10);

    await put(input, 1);
    await put(input, 2);
    await put(input, 3);
    input.close();

    await fromChannel(input).stream(
      map(x => x * 2),
      intoChannel(output),
    );

    expect(await take(output)).toBe(2);
    expect(await take(output)).toBe(4);
    expect(await take(output)).toBe(6);
    expect(output.closed).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// unboundedBuffer
// ---------------------------------------------------------------------------

describe('unboundedBuffer', () => {
  it('sender never blocks', async () => {
    const ch = createChannel<number>(unboundedBuffer());
    for (let i = 0; i < 1000; i++) {
      await put(ch, i);
    }
    for (let i = 0; i < 1000; i++) {
      expect(await take(ch)).toBe(i);
    }
  });

  it('canPutSync is always true when open', () => {
    const ch = createChannel<number>(unboundedBuffer());
    expect(ch.canPutSync).toBe(true);
    ch.putSync(1);
    ch.putSync(2);
    expect(ch.canPutSync).toBe(true);
  });

  it('works with intoChannel + fromChannel', async () => {
    const ch = createChannel<number>(unboundedBuffer());
    const src = fromArray([1, 2, 3]);
    await stream(src, intoChannel(ch));

    expect(await take(ch)).toBe(1);
    expect(await take(ch)).toBe(2);
    expect(await take(ch)).toBe(3);
    expect(ch.closed).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Sync methods: putSync, takeSync, canPutSync, canTakeSync
// ---------------------------------------------------------------------------

describe('sync methods', () => {
  describe('takeSync', () => {
    it('takes a buffered value synchronously', async () => {
      const ch = createChannel<number>(5);
      await put(ch, 1);
      await put(ch, 2);

      expect(ch.takeSync()).toBe(1);
      expect(ch.takeSync()).toBe(2);
    });

    it('throws when no value available', () => {
      const ch = createChannel<number>(5);
      expect(() => ch.takeSync()).toThrow('no value available');
    });

    it('throws on closed empty channel', () => {
      const ch = createChannel<number>(5);
      ch.close();
      expect(() => ch.takeSync()).toThrow('no value available');
    });

    it('succeeds on closed channel with buffered data', async () => {
      const ch = createChannel<number>(5);
      await put(ch, 42);
      ch.close();
      expect(ch.takeSync()).toBe(42);
    });

    it('takes from pending sender via putSync (rendezvous)', () => {
      const ch = createChannel<number>(); // rendezvous
      // putSync force-pushes into buffer, making it available for takeSync
      ch.putSync(99);
      expect(ch.takeSync()).toBe(99);
    });

    it('unblocks pending sender when taking from full buffer', async () => {
      const ch = createChannel<number>(1);
      await put(ch, 1);
      // This put will pend (buffer full)
      const putPromise = put(ch, 2);
      // takeSync should take 1, move 2 into buffer, notify sender
      expect(ch.takeSync()).toBe(1);
      await putPromise; // should resolve now
      expect(ch.takeSync()).toBe(2);
    });
  });

  describe('putSync', () => {
    it('puts into buffer synchronously', () => {
      const ch = createChannel<number>(5);
      ch.putSync(1);
      ch.putSync(2);
      expect(ch.takeSync()).toBe(1);
      expect(ch.takeSync()).toBe(2);
    });

    it('throws on closed channel', () => {
      const ch = createChannel<number>(5);
      ch.close();
      expect(() => ch.putSync(1)).toThrow('closed channel');
    });

    it('force-pushes beyond buffer limit when full', () => {
      const ch = createChannel<number>(2);
      ch.putSync(1);
      ch.putSync(2);
      // Buffer is full — putSync should exceed the limit
      ch.putSync(3);
      expect(ch.takeSync()).toBe(1);
      expect(ch.takeSync()).toBe(2);
      expect(ch.takeSync()).toBe(3);
    });

    it('delivers to pending receiver directly', () => {
      const ch = createChannel<number>(); // rendezvous
      const values: number[] = [];

      // Start an async take — it will register as a pending receiver
      const conn = fromChannel(ch).connect({
        next(v) { values.push(v); return undefined; },
        complete() {},
        error() {},
      });
      conn.resume();

      ch.putSync(42);
      expect(values).toEqual([42]);
      conn[Symbol.dispose]();
    });
  });

  describe('canTakeSync', () => {
    it('returns false on empty channel', () => {
      const ch = createChannel<number>(5);
      expect(ch.canTakeSync).toBe(false);
    });

    it('returns true when buffer has values', async () => {
      const ch = createChannel<number>(5);
      await put(ch, 1);
      expect(ch.canTakeSync).toBe(true);
    });

    it('returns true when putSync force-pushed into buffer', () => {
      const ch = createChannel<number>(); // rendezvous
      ch.putSync(1); // force-pushes into 0-size buffer
      expect(ch.canTakeSync).toBe(true);
    });

    it('returns false on closed empty channel', () => {
      const ch = createChannel<number>(5);
      ch.close();
      expect(ch.canTakeSync).toBe(false);
    });
  });

  describe('canPutSync', () => {
    it('returns true when buffer has space', () => {
      const ch = createChannel<number>(5);
      expect(ch.canPutSync).toBe(true);
    });

    it('returns false when buffer is full (fixed)', async () => {
      const ch = createChannel<number>(1);
      await put(ch, 1);
      expect(ch.canPutSync).toBe(false);
    });

    it('returns false on closed channel', () => {
      const ch = createChannel<number>(5);
      ch.close();
      expect(ch.canPutSync).toBe(false);
    });

    it('returns true for dropping/sliding (never full)', async () => {
      const drop = createChannel<number>(droppingBuffer(1));
      await put(drop, 1);
      expect(drop.canPutSync).toBe(true);

      const slide = createChannel<number>(slidingBuffer(1));
      await put(slide, 1);
      expect(slide.canPutSync).toBe(true);
    });

    it('returns true for unbounded buffer', () => {
      const ch = createChannel<number>(unboundedBuffer());
      expect(ch.canPutSync).toBe(true);
    });
  });
});

// ---------------------------------------------------------------------------
// ReadChannel / WriteChannel type narrowing
// ---------------------------------------------------------------------------

describe('directional channel views', () => {
  it('ReadChannel accepts take() and fromChannel()', async () => {
    const ch = createChannel<number>(5);
    await put(ch, 1);

    const read: ReadChannel<number> = ch;
    expect(await take(read)).toBe(1);
    expect(read.closed).toBe(false);
    expect(read.canTakeSync).toBe(false);
  });

  it('WriteChannel accepts put(), intoChannel(), close()', async () => {
    const ch = createChannel<number>(5);
    const write: WriteChannel<number> = ch;

    await put(write, 1);
    expect(write.canPutSync).toBe(true);
    write.putSync(2);
    write.close();
    expect(write.closed).toBe(true);

    // Values are in the channel
    expect(ch.takeSync()).toBe(1);
    expect(ch.takeSync()).toBe(2);
  });

  it('closed() accepts both ReadChannel and WriteChannel', async () => {
    const ch = createChannel<number>(5);

    const read: ReadChannel<number> = ch;
    const write: WriteChannel<number> = ch;

    const readClosed = select({ done: closed(read) });
    const writeClosed = select({ done: closed(write) });

    ch.close();

    expect((await readClosed).tag).toBe('done');
    expect((await writeClosed).tag).toBe('done');
  });
});
