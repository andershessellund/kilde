// ---------------------------------------------------------------------------
// relay — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe, stream } from './stream.js';
import { map } from './operators/map.js';
import { filter } from './operators/filter.js';
import { take } from './operators/take.js';
import { toPromise } from './operators/to-promise.js';
import { createRelay } from './relay.js';
import { testSink } from './testing/test-sink.js';
import { exhaustiveTest } from './testing/exhaustive.js';
import { assertProtocol } from './testing/protocol.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('relay (exhaustive)', () => {
  it('relay through map — all pause orderings', async () => {
    await exhaustiveTest((oracle) => {
      const relay = createRelay<number>();
      const mapped = pipe(
        relay,
        map((x) => x * 10),
        assertProtocol(),
      );
      const sink = testSink<number>({ oracle });
      const s = mapped.connect(sink);
      s.resume();

      relay.next(1);
      relay.next(2);
      relay.next(3);
      relay.complete();

      drive(s, sink);
      expect(sink.values).toEqual([10, 20, 30]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('relay through filter — all pause orderings', async () => {
    await exhaustiveTest((oracle) => {
      const relay = createRelay<number>();
      const filtered = pipe(
        relay,
        filter((x) => x % 2 === 0),
        assertProtocol(),
      );
      const sink = testSink<number>({ oracle });
      const s = filtered.connect(sink);
      s.resume();

      relay.next(1);
      relay.next(2);
      relay.next(3);
      relay.next(4);
      relay.complete();

      drive(s, sink);
      expect(sink.values).toEqual([2, 4]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('relay through take — early termination', async () => {
    await exhaustiveTest((oracle) => {
      const relay = createRelay<number>();
      const taken = pipe(relay, take(2), assertProtocol());
      const sink = testSink<number>({ oracle });
      const s = taken.connect(sink);
      s.resume();

      relay.next(1);
      relay.next(2);
      relay.next(3); // ignored — take(2) already done

      drive(s, sink);
      expect(sink.values).toEqual([1, 2]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('two subscribers — all pause orderings per subscriber', async () => {
    await exhaustiveTest((oracle) => {
      const relay = createRelay<number>();
      const sink1 = testSink<number>({ oracle });
      const sink2 = testSink<number>({ oracle });
      const s1 = pipe(relay, assertProtocol()).connect(sink1);
      const s2 = pipe(relay, assertProtocol()).connect(sink2);
      s1.resume();
      s2.resume();

      relay.next(1);
      relay.next(2);
      relay.complete();

      drive(s1, sink1);
      drive(s2, sink2);
      expect(sink1.values).toEqual([1, 2]);
      expect(sink1.completeCount).toBe(1);
      expect(sink2.values).toEqual([1, 2]);
      expect(sink2.completeCount).toBe(1);
    });
  });

  it('relay error — all pause orderings', async () => {
    await exhaustiveTest((oracle) => {
      const relay = createRelay<number>();
      const sink = testSink<number>({ oracle });
      const s = pipe(relay, assertProtocol()).connect(sink);
      s.resume();

      relay.next(1);
      relay.error(new Error('relay-err'));

      drive(s, sink);
      expect(sink.values).toEqual([1]);
      expect(sink.errors).toHaveLength(1);
      expect((sink.errors[0] as Error).message).toBe('relay-err');
    });
  });

  it('values pushed before the first resume are delivered on resume — all pause orderings', async () => {
    await exhaustiveTest((oracle) => {
      const relay = createRelay<number>();
      const sink = testSink<number>({ oracle });
      const s = pipe(relay, assertProtocol()).connect(sink);

      relay.next(1);
      relay.next(2);
      relay.complete();
      expect(sink.values).toEqual([]);

      drive(s, sink);
      expect(sink.values).toEqual([1, 2]);
      expect(sink.completeCount).toBe(1);
    });
  });
});

describe('relay (bug 4 — terminal before the first resume)', () => {
  it('a subscriber connecting after complete() gets complete() on its first resume, not inside connect()', () => {
    const relay = createRelay<number>();
    relay.complete();
    const sink = testSink<number>();
    const s = pipe(relay, assertProtocol()).connect(sink); // assertProtocol throws on delivery inside connect()
    expect(sink.completeCount).toBe(0);
    s.resume();
    expect(sink.completeCount).toBe(1);
    s.resume(); // no-op
    expect(sink.completeCount).toBe(1);
  });

  it('a subscriber connecting after error() gets error() on its first resume', () => {
    const relay = createRelay<number>();
    relay.error(new Error('gone'));
    const sink = testSink<number>();
    const s = pipe(relay, assertProtocol()).connect(sink);
    expect(sink.errors).toHaveLength(0);
    s.resume();
    expect(sink.errors).toHaveLength(1);
  });

  it('r.complete(); stream(r, toPromise()) rejects instead of crashing', async () => {
    const r = createRelay<number>();
    r.complete();
    await expect(stream(r, toPromise())).rejects.toThrow('completed without emitting');
  });
});

describe('relay (protocol)', () => {
  it('resume() is idempotent and a no-op after the terminal', () => {
    const relay = createRelay<number>();
    const sink = testSink<number>();
    const s = pipe(relay, assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    relay.next(1);
    relay.complete();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(1);
    s.resume();
    expect(sink.completeCount).toBe(1);
  });

  it('complete() while paused waits for the buffer to drain', () => {
    const relay = createRelay<number>();
    let pauseNext = true;
    const sink = testSink<number>({ oracle: { integer: () => (pauseNext ? 1 : 0) } });
    const s = pipe(relay, assertProtocol()).connect(sink);
    s.resume();
    relay.next(1);
    relay.next(2);
    relay.complete();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(0);
    pauseNext = false;
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    expect(sink.completeCount).toBe(1);
  });

  it('a disposed subscriber receives nothing more', () => {
    const relay = createRelay<number>();
    const sink = testSink<number>();
    const s = relay.connect(sink);
    s.resume();
    relay.next(1);
    s[Symbol.dispose]();
    relay.next(2);
    relay.complete();
    s.resume();
    expect(sink.values).toEqual([1]);
    expect(sink.completeCount).toBe(0);
  });

  it('a subscriber that errors out of a paused buffer is not delivered twice', () => {
    const relay = createRelay<number>();
    const sink = testSink<number>({ oracle: { integer: () => 1 } });
    const s = pipe(relay, assertProtocol()).connect(sink);
    s.resume();
    relay.next(1);
    relay.error(new Error('x'));
    expect(sink.errors).toHaveLength(1); // buffer was empty → delivered at once, while paused
    s.resume();
    expect(sink.errors).toHaveLength(1);
  });
});
