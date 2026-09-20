// ---------------------------------------------------------------------------
// catchError — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source, Sink } from '../types.js';
import { PAUSE } from '../types.js';
import { pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { createRelay } from '../relay.js';
import { catchError } from './catch-error.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import type { DecisionOracle } from '../testing/oracle.js';
import { assertProtocol } from '../testing/protocol.js';

/**
 * Source that emits `values` then errors with `err`. With an oracle it
 * behaves like testSource: it may self-pause between values, and when the
 * last value is answered with PAUSE the error may arrive immediately
 * (while paused) or on the next resume().
 */
function failAfter<T>(
  values: T[],
  err: unknown = new Error('fail'),
  oracle?: DecisionOracle,
): Source<T> {
  return {
    connect(sink) {
      let index = 0;
      let disposed = false;
      let failed = false;
      const fail = () => {
        if (disposed || failed) return;
        failed = true;
        sink.error(err);
      };
      return {
        resume() {
          if (disposed || failed) return;
          while (index < values.length) {
            const result = sink.next(values[index++]);
            if (disposed) return;
            if (result === PAUSE) {
              if (index >= values.length && oracle && oracle.integer(2) === 1) fail();
              return;
            }
            if (oracle && index < values.length && oracle.integer(2) === 1) return;
          }
          fail();
        },
        [Symbol.dispose]() {
          disposed = true;
        },
      };
    },
  };
}

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('catchError (exhaustive)', () => {
  it('error after one value — recover with array', async () => {
    await exhaustiveTest((oracle) => {
      const sink = testSink<number>({ oracle });
      const s = pipe(
        failAfter([1], undefined, oracle),
        catchError(() => testSource([99], { oracle })),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 99]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('error after two values — longer fallback', async () => {
    await exhaustiveTest((oracle) => {
      const sink = testSink<number>({ oracle });
      const s = pipe(
        failAfter([1, 2], undefined, oracle),
        catchError(() => testSource([8, 9], { oracle })),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 8, 9]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('immediate error — full fallback', async () => {
    await exhaustiveTest((oracle) => {
      const sink = testSink<number>({ oracle });
      const s = pipe(
        failAfter<number>([], undefined, oracle),
        catchError(() => testSource([5, 6, 7], { oracle })),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([5, 6, 7]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('no error — passthrough', async () => {
    await exhaustiveTest((oracle) => {
      const src = testSource([1, 2, 3], { oracle });
      const sink = testSink<number>({ oracle });
      const s = pipe(
        src,
        catchError(() => fromArray([99])),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([1, 2, 3]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('fallback that errors too — error forwarded once', async () => {
    await exhaustiveTest((oracle) => {
      const sink = testSink<number>({ oracle });
      const s = pipe(
        failAfter([1], new Error('first'), oracle),
        catchError(() => failAfter([2], new Error('second'), oracle)),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink, 10);
      expect(sink.values).toEqual([1, 2]);
      expect(sink.errors).toHaveLength(1);
      expect((sink.errors[0] as Error).message).toBe('second');
    });
  });

  it('error handler receives original error', async () => {
    const captured: unknown[] = [];
    await exhaustiveTest((oracle) => {
      captured.length = 0;
      const sink = testSink<number>({ oracle });
      const s = pipe(
        failAfter([1], new Error('oops'), oracle),
        catchError((err) => {
          captured.push(err);
          return fromArray([42]);
        }),
        assertProtocol(),
      ).connect(sink);
      drive(s, sink);
      expect(captured).toHaveLength(1);
      expect((captured[0] as Error).message).toBe('oops');
    });
  });
});

describe('catchError (bug 6 — error while downstream paused)', () => {
  it('does not resume the fallback until the downstream resumes', () => {
    const relay = createRelay<number>();
    let fallbackResumed = 0;
    const fallback: Source<number> = {
      connect(inner: Sink<number>) {
        return {
          resume() {
            fallbackResumed++;
            inner.next(99);
            inner.complete();
          },
          [Symbol.dispose]() {},
        };
      },
    };
    const sink = testSink<number>({ oracle: { integer: () => 1 } }); // always PAUSE
    const s = pipe(relay, catchError(() => fallback), assertProtocol()).connect(sink);
    s.resume();
    relay.next(1); // sink pauses
    expect(sink.paused).toBe(true);
    relay.error(new Error('boom')); // arrives while paused
    expect(fallbackResumed).toBe(0);
    expect(sink.values).toEqual([1]);

    s.resume(); // now the fallback is started
    expect(fallbackResumed).toBe(1);
    expect(sink.values).toEqual([1, 99]);
    expect(sink.completeCount).toBe(1);
  });

  it('forwards later resume() calls to the fallback', () => {
    const relay = createRelay<number>();
    const fallback = createRelay<number>();
    const sink = testSink<number>({ oracle: { integer: () => 1 } }); // always PAUSE
    const s = pipe(relay, catchError(() => fallback), assertProtocol()).connect(sink);
    s.resume();
    relay.error(new Error('boom')); // downstream unpaused → fallback resumed now
    fallback.next(1);
    expect(sink.values).toEqual([1]);
    fallback.next(2); // buffered in the fallback relay, sink is paused
    expect(sink.values).toEqual([1]);
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    fallback.complete();
    expect(sink.completeCount).toBe(1);
  });

  it('a throwing handler errors the downstream', () => {
    const sink = testSink<number>();
    const s = pipe(
      failAfter([1]),
      catchError(() => {
        throw new Error('handler-fail');
      }),
      assertProtocol(),
    ).connect(sink);
    s.resume();
    expect(sink.values).toEqual([1]);
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('handler-fail');
    s.resume(); // no-op after terminal
  });

  it('dispose releases the fallback', () => {
    let disposed = false;
    const fallback: Source<number> = {
      connect() {
        return {
          resume() {},
          [Symbol.dispose]() {
            disposed = true;
          },
        };
      },
    };
    const sink = testSink<number>();
    const s = pipe(failAfter<number>([]), catchError(() => fallback)).connect(sink);
    s.resume();
    s[Symbol.dispose]();
    expect(disposed).toBe(true);
  });
});
