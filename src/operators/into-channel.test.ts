// ---------------------------------------------------------------------------
// intoChannel — tests (basic behaviour is also covered in channel.test.ts)
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source, Sink } from '../types.js';
import { PAUSE } from '../types.js';
import { stream, pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { createRelay } from '../relay.js';
import { createChannel } from '../channel.js';
import { take } from '../choices/take.js';
import { intoChannel } from './into-channel.js';
import { StreamDisposedError } from '../stream-disposed-error.js';
import { createOwner, withOwner } from '../owner.js';
import { testSink } from '../testing/test-sink.js';
import { assertProtocol } from '../testing/protocol.js';

const microtask = () => new Promise<void>((r) => queueMicrotask(r));

function tracked<T>(source: Source<T>): Source<T> & { connects: number; disposes: number; resumes: number } {
  const t = {
    connects: 0,
    disposes: 0,
    resumes: 0,
    connect(sink: Sink<T>) {
      t.connects++;
      const s = source.connect(sink);
      return {
        resume: () => {
          t.resumes++;
          s.resume();
        },
        [Symbol.dispose]: () => {
          t.disposes++;
          s[Symbol.dispose]();
        },
      };
    },
  };
  return t;
}

describe('intoChannel()', () => {
  it('rendezvous channel: values flow one take at a time', async () => {
    const ch = createChannel<number>();
    const done = stream(fromArray([1, 2, 3]), intoChannel(ch));
    expect(await take(ch)).toBe(1);
    expect(await take(ch)).toBe(2);
    expect(await take(ch)).toBe(3);
    await done;
    expect(ch.closed).toBe(true);
  });

  it('bug 15: the upstream is resumed on a microtask, never inside the take', async () => {
    const ch = createChannel<number>(1);
    const src = tracked(fromArray([1, 2, 3]));
    const done = stream(src, intoChannel(ch));
    // 1 is buffered, 2 is parked as a pending sender, upstream paused.
    expect(src.resumes).toBe(1);
    expect(ch.takeSync()).toBe(1); // moves 2 into the buffer and notifies us
    expect(src.resumes).toBe(1); // not resumed synchronously inside the take
    await microtask();
    expect(src.resumes).toBe(2); // now 3 is parked
    expect(ch.takeSync()).toBe(2);
    await microtask();
    expect(ch.takeSync()).toBe(3);
    await done;
    expect(ch.closed).toBe(true);
  });

  it('a terminal arriving while a value is parked is applied after the value is taken', async () => {
    // Delivers 1 (buffered), 2 (parked → PAUSE) and completes at once.
    const src: Source<number> = {
      connect(sink: Sink<number>) {
        return {
          resume() {
            sink.next(1);
            expect(sink.next(2)).toBe(PAUSE);
            sink.complete();
          },
          [Symbol.dispose]() {},
        };
      },
    };
    const ch = createChannel<number>(1);
    let settled = false;
    const done = stream(src, intoChannel(ch)).then(() => {
      settled = true;
    });
    await microtask();
    expect(settled).toBe(false);
    expect(ch.closed).toBe(false); // closing now would drop the parked 2
    expect(await take(ch)).toBe(1);
    expect(await take(ch)).toBe(2);
    await done;
    expect(ch.closed).toBe(true);
  });

  it('an error arriving while a value is parked is applied after the value is taken', async () => {
    const src: Source<number> = {
      connect(sink: Sink<number>) {
        return {
          resume() {
            sink.next(1);
            sink.next(2);
            sink.error(new Error('late'));
          },
          [Symbol.dispose]() {},
        };
      },
    };
    const ch = createChannel<number>(1);
    const done = stream(src, intoChannel(ch));
    done.catch(() => {});
    expect(await take(ch)).toBe(1);
    expect(await take(ch)).toBe(2);
    await expect(done).rejects.toThrow('late');
    expect(ch.closed).toBe(true);
  });

  it('closing the channel while a value is parked disposes the upstream and resolves', async () => {
    const ch = createChannel<number>(1);
    const src = tracked(fromArray([1, 2, 3]));
    const done = stream(src, intoChannel(ch));
    ch.close();
    await done;
    expect(src.disposes).toBe(1);
  });

  it('registers with the owner; owner disposal rejects and leaves the channel open', async () => {
    const owner = createOwner('pipe');
    const ch = createChannel<number>(4);
    const relay = createRelay<number>();
    const src = tracked(relay);
    const done = withOwner(owner, () => stream(src, intoChannel(ch)));
    expect(owner.size).toBe(1);
    relay.next(1);
    await owner.dispose();
    await expect(done).rejects.toBeInstanceOf(StreamDisposedError);
    expect(src.disposes).toBe(1);
    expect(ch.closed).toBe(false);
    expect(ch.takeSync()).toBe(1);
  });

  it('unregisters from the owner when the source completes', async () => {
    const owner = createOwner('pipe');
    const ch = createChannel<number>(4);
    await stream(fromArray([1]), intoChannel(ch, { owner }));
    expect(owner.size).toBe(0);
  });

  it('bug 13: an already-disposed owner never connects', async () => {
    const owner = createOwner('dead');
    await owner.dispose();
    const ch = createChannel<number>(4);
    const src = tracked(fromArray([1]));
    await expect(stream(src, intoChannel(ch, { owner }))).rejects.toBeInstanceOf(StreamDisposedError);
    expect(src.connects).toBe(0);
  });

  it('bug 12: a second resume() does not open a second upstream connection', () => {
    const ch = createChannel<number>(4);
    const src = tracked(fromArray([1]));
    const sink = testSink<Promise<void>>();
    const s = pipe(src, intoChannel(ch), assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    expect(src.connects).toBe(1);
    expect(sink.values).toHaveLength(1);
    expect(sink.completeCount).toBe(1);
  });
});
