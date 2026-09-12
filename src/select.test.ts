// ---------------------------------------------------------------------------
// select / choice / timeout / promise — tests
// ---------------------------------------------------------------------------

import { describe, it, expect, vi } from 'vitest';
import { select, UnhandledRejectionError } from './select.js';
import { timeout } from './choices/timeout.js';
import { resolved, rejected } from './choices/promise.js';
import { take } from './choices/take.js';
import { put } from './choices/put.js';
import { defaultChoice } from './choices/default.js';
import { createChannel } from './channel.js';

// ---------------------------------------------------------------------------
// select() basics
// ---------------------------------------------------------------------------

describe('select()', () => {
  it('rejects on empty map', async () => {
    await expect(select({})).rejects.toThrow('at least one choice');
  });

  it('returns the first ready choice (priority order)', async () => {
    const result = await select({
      a: timeout(0),
      b: timeout(0),
    });
    expect(result.tag).toBe('a');
  });
});

// ---------------------------------------------------------------------------
// timeout()
// ---------------------------------------------------------------------------

describe('timeout()', () => {
  it('is directly awaitable', async () => {
    await timeout(0);
    // resolves without error
  });

  it('fires in select after delay', async () => {
    vi.useFakeTimers();
    const p = select({ t: timeout(100) });
    vi.advanceTimersByTime(100);
    const result = await p;
    expect(result.tag).toBe('t');
    expect(result.value).toBe(undefined);
    vi.useRealTimers();
  });

  it('already-fired timeout resolves synchronously in poll', async () => {
    vi.useFakeTimers();
    const t = timeout(10);
    vi.advanceTimersByTime(10);

    // poll should see it immediately
    const result = await select({ t });
    expect(result.tag).toBe('t');
    vi.useRealTimers();
  });

  it('shorter timeout wins over longer', async () => {
    vi.useFakeTimers();
    const p = select({
      short: timeout(50),
      long: timeout(200),
    });
    vi.advanceTimersByTime(50);
    const result = await p;
    expect(result.tag).toBe('short');
    vi.useRealTimers();
  });
});

// ---------------------------------------------------------------------------
// resolved() / rejected()
// ---------------------------------------------------------------------------

describe('resolved()', () => {
  it('is directly awaitable', async () => {
    const value = await resolved(Promise.resolve(42));
    expect(value).toBe(42);
  });

  it('already-resolved promise produces immediate result', async () => {
    const p = Promise.resolve('hello');
    await p; // ensure settled
    const result = await select({ v: resolved(p) });
    expect(result.tag).toBe('v');
    expect(result.value).toBe('hello');
  });

  it('pending promise resolves after fulfillment', async () => {
    let doResolve!: (v: number) => void;
    const p = new Promise<number>((r) => { doResolve = r; });
    const sel = select({ v: resolved(p) });
    doResolve(99);
    const result = await sel;
    expect(result.tag).toBe('v');
    expect(result.value).toBe(99);
  });

  it('throws UnhandledRejectionError when promise rejects without handler', async () => {
    let doReject!: (e: unknown) => void;
    const p = new Promise<number>((_, r) => { doReject = r; });
    // Suppress unhandled rejection
    p.catch(() => {});
    const sel = select({ v: resolved(p) });
    doReject(new Error('boom'));
    await expect(sel).rejects.toBeInstanceOf(UnhandledRejectionError);
  });

  it('does not throw when rejected() handler is present', async () => {
    let doReject!: (e: unknown) => void;
    const p = new Promise<number>((_, r) => { doReject = r; });
    p.catch(() => {});
    const sel = select({
      v: resolved(p),
      err: rejected(p),
    });
    doReject('fail');
    const result = await sel;
    expect(result.tag).toBe('err');
    expect(result.value).toBe('fail');
  });
});

describe('rejected()', () => {
  it('fires on rejection', async () => {
    let doReject!: (e: unknown) => void;
    const p = new Promise<number>((_, r) => { doReject = r; });
    p.catch(() => {});
    const sel = select({ e: rejected(p) });
    doReject('oops');
    const result = await sel;
    expect(result.tag).toBe('e');
    expect(result.value).toBe('oops');
  });

  it('is directly awaitable', async () => {
    const p = Promise.reject('reason');
    p.catch(() => {});
    const value = await rejected(p);
    expect(value).toBe('reason');
  });
});

// ---------------------------------------------------------------------------
// Fan-in (array of choices)
// ---------------------------------------------------------------------------

describe('select() fan-in', () => {
  it('selects from array of choices', async () => {
    const ch1 = createChannel<string>(1);
    const ch2 = createChannel<string>(1);
    await put(ch1, 'from-1');

    const result = await select({
      msg: [take(ch1), take(ch2)],
    });
    expect(result.tag).toBe('msg');
    expect(result.value).toBe('from-1');
    expect(result.channel).toBe(ch1);
  });

  it('second channel in fan-in array can win', async () => {
    const ch1 = createChannel<string>(1);
    const ch2 = createChannel<string>(1);
    await put(ch2, 'from-2');

    const result = await select({
      msg: [take(ch1), take(ch2)],
    });
    expect(result.tag).toBe('msg');
    expect(result.value).toBe('from-2');
    expect(result.channel).toBe(ch2);
  });
});

// ---------------------------------------------------------------------------
// Choice awaitability (then mixin)
// ---------------------------------------------------------------------------

describe('Choice awaitable (then mixin)', () => {
  it('take() is directly awaitable', async () => {
    const ch = createChannel<number>(1);
    await put(ch, 7);
    const v = await take(ch);
    expect(v).toBe(7);
  });

  it('put() is directly awaitable', async () => {
    const ch = createChannel<number>(1);
    await put(ch, 42);
    const v = await take(ch);
    expect(v).toBe(42);
  });
});

// ---------------------------------------------------------------------------
// defaultChoice()
// ---------------------------------------------------------------------------

describe('defaultChoice()', () => {
  it('fires immediately in select', async () => {
    const result = await select({
      fallback: defaultChoice(),
    });
    expect(result.tag).toBe('fallback');
    expect(result.value).toBe(undefined);
  });

  it('other ready choice wins by priority over default', async () => {
    const ch = createChannel<number>(1);
    await put(ch, 1);
    const result = await select({
      msg: take(ch),
      fallback: defaultChoice(),
    });
    expect(result.tag).toBe('msg');
    expect(result.value).toBe(1);
  });

  it('default wins when no other choice is ready', async () => {
    const ch = createChannel<number>();
    const result = await select({
      msg: take(ch),
      none: defaultChoice(),
    });
    expect(result.tag).toBe('none');
  });

  it('is directly awaitable', async () => {
    await defaultChoice();
    // resolves without error
  });
});
