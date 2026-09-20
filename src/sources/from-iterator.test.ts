// ---------------------------------------------------------------------------
// fromIterator — exhaustive protocol tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { fromIterator } from './from-iterator.js';
import { pipe, stream } from '../stream.js';
import { toArray } from '../operators/to-array.js';
import { take } from '../operators/take.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

function drive(s: { resume(): void }, sink: { terminated: boolean }, max = 40) {
  for (let i = 0; i < max && !sink.terminated; i++) s.resume();
}

function* naturals(): Generator<number> {
  let i = 0;
  while (true) yield i++;
}

describe('fromIterator', () => {
  it('pulls values lazily and completes', () => {
    expect(stream(fromIterator([1, 2, 3][Symbol.iterator]()), toArray())).toEqual([1, 2, 3]);
    expect(stream(fromIterator(naturals()), take(3), toArray())).toEqual([0, 1, 2]);
  });

  it('respects PAUSE and completes exactly once under every pause pattern', async () => {
    await exhaustiveTest((oracle) => {
      const sink = testSink<number>({ oracle });
      const s = pipe(fromIterator([1, 2, 3][Symbol.iterator]()), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
      s.resume();
      s.resume();
      expect(sink.completeCount).toBe(1);
    });
  });

  it('does not call iterator.next() again after done', () => {
    let nextCalls = 0;
    const iterator: Iterator<number> = {
      next() {
        nextCalls++;
        return { done: true, value: undefined };
      },
    };
    const sink = testSink<number>();
    const s = pipe(fromIterator(iterator), assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    expect(nextCalls).toBe(1);
    expect(sink.completeCount).toBe(1);
  });

  it('routes an exception from iterator.next() to sink.error() and stops', async () => {
    await exhaustiveTest((oracle) => {
      function* failing(): Generator<number> {
        yield 1;
        yield 2;
        throw new Error('iterator broke');
      }
      const sink = testSink<number>({ oracle });
      const s = pipe(fromIterator(failing()), assertProtocol()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2]);
      expect(sink.errors).toHaveLength(1);
      expect((sink.errors[0] as Error).message).toBe('iterator broke');
      expect(sink.completeCount).toBe(0);
      s.resume(); // no-op after error
      expect(sink.errors).toHaveLength(1);
    });
  });

  it('calls iterator.return() on dispose while running', () => {
    let returned = false;
    function* gen(): Generator<number> {
      try {
        yield 1;
        yield 2;
      } finally {
        returned = true;
      }
    }
    const sink = testSink<number>({ oracle: { integer: () => 1 } }); // always PAUSE
    const s = fromIterator(gen()).connect(sink);
    s.resume();
    expect(sink.values).toEqual([1]);
    s[Symbol.dispose]();
    expect(returned).toBe(true);
    s.resume(); // no-op
    expect(sink.values).toEqual([1]);
  });

  it('swallows an exception thrown by iterator.return() during dispose', () => {
    const iterator: Iterator<number> = {
      next() {
        return { done: false, value: 1 };
      },
      return() {
        throw new Error('return failed');
      },
    };
    const sink = testSink<number>({ oracle: { integer: () => 1 } });
    const s = fromIterator(iterator).connect(sink);
    s.resume();
    expect(() => s[Symbol.dispose]()).not.toThrow();
    expect(() => s[Symbol.dispose]()).not.toThrow(); // idempotent
  });

  it('does not call iterator.return() after the iterator finished or broke', () => {
    let returnCalls = 0;
    const done: Iterator<number> = {
      next: () => ({ done: true, value: undefined }),
      return: () => {
        returnCalls++;
        return { done: true, value: undefined };
      },
    };
    const s1 = fromIterator(done).connect(testSink<number>());
    s1.resume();
    s1[Symbol.dispose]();

    const broken: Iterator<number> = {
      next: () => {
        throw new Error('x');
      },
      return: () => {
        returnCalls++;
        return { done: true, value: undefined };
      },
    };
    const s2 = fromIterator(broken).connect(testSink<number>());
    s2.resume();
    s2[Symbol.dispose]();

    expect(returnCalls).toBe(0);
  });

  it('dispose from inside next() stops the pull loop', () => {
    let pulled = 0;
    function* gen(): Generator<number> {
      while (true) {
        pulled++;
        yield pulled;
      }
    }
    const values: number[] = [];
    const s: { resume(): void; [Symbol.dispose](): void } = fromIterator(gen()).connect({
      next(v: number) {
        values.push(v);
        if (v === 3) s[Symbol.dispose]();
        return undefined;
      },
      complete() {},
      error() {},
    });
    s.resume();
    expect(values).toEqual([1, 2, 3]);
    expect(pulled).toBe(3);
  });
});
