// ---------------------------------------------------------------------------
// Stream tests — protocol, sources, operators, relay
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { PAUSE } from './types.js';
import type { Source, Sink } from './types.js';
import { stream, pipe, comp } from './stream.js';
import { fromArray, of } from './sources/from-array.js';
import { fromIterator } from './sources/from-iterator.js';
import { empty } from './sources/empty.js';
import { deferred } from './sources/deferred.js';
import { map } from './operators/map.js';
import { filter } from './operators/filter.js';
import { take } from './operators/take.js';
import { scan } from './operators/scan.js';
import { reduce } from './operators/reduce.js';
import { flatten } from './operators/flatten.js';
import { catchError } from './operators/catch-error.js';
import { pausable } from './operators/pausable.js';
import { toArray } from './operators/to-array.js';
import { toSource } from './operators/to-source.js';
import { toPromise } from './operators/to-promise.js';
import { createRelay } from './relay.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const add = (a: number, b: number) => a + b;

function collectSink<T>(): Sink<T> & { values: T[]; completed: boolean; errors: unknown[] } {
  const result = {
    values: [] as T[],
    completed: false,
    errors: [] as unknown[],
    next(value: T) {
      result.values.push(value);
      return undefined as undefined;
    },
    complete() {
      result.completed = true;
    },
    error(err: unknown) {
      result.errors.push(err);
    },
  };
  return result;
}

function pausingSink<T>(
  pauseAfter: number,
): Sink<T> & { values: T[]; completed: boolean; errors: unknown[]; pauseCount: number } {
  let count = 0;
  const result = {
    values: [] as T[],
    completed: false,
    errors: [] as unknown[],
    pauseCount: 0,
    next(value: T): undefined | typeof PAUSE {
      result.values.push(value);
      count++;
      if (count >= pauseAfter) {
        count = 0;
        result.pauseCount++;
        return PAUSE;
      }
      return undefined;
    },
    complete() {
      result.completed = true;
    },
    error(err: unknown) {
      result.errors.push(err);
    },
  };
  return result;
}

// ---------------------------------------------------------------------------
// Protocol
// ---------------------------------------------------------------------------

describe('Stream protocol', () => {
  it('connect returns a paused stream', () => {
    const sink = collectSink<number>();
    const s = fromArray([1, 2, 3]).connect(sink);
    expect(sink.values).toEqual([]);
    s.resume();
    expect(sink.values).toEqual([1, 2, 3]);
    expect(sink.completed).toBe(true);
  });

  it('PAUSE stops emission', () => {
    const sink = pausingSink<number>(2);
    const s = fromArray([1, 2, 3, 4, 5]).connect(sink);
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    s.resume();
    expect(sink.values).toEqual([1, 2, 3, 4]);
    s.resume();
    expect(sink.values).toEqual([1, 2, 3, 4, 5]);
    expect(sink.completed).toBe(true);
  });

  it('dispose stops emission', () => {
    const sink = pausingSink<number>(2);
    const s = fromArray([1, 2, 3, 4, 5]).connect(sink);
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    s[Symbol.dispose]();
    s.resume();
    expect(sink.values).toEqual([1, 2]); // No more values after dispose
  });
});

// ---------------------------------------------------------------------------
// stream() — synchronous extraction
// ---------------------------------------------------------------------------

describe('stream()', () => {
  it('extracts a single value from a single-element source', () => {
    expect(stream(fromArray([42]))).toBe(42);
  });

  it('reduce as collector', () => {
    expect(stream(fromArray([1, 2, 3]), reduce(add, 0))).toBe(6);
  });

  it('toArray as collector', () => {
    expect(stream(fromArray([1, 2, 3]), toArray())).toEqual([1, 2, 3]);
  });

  it('map + reduce', () => {
    expect(
      stream(
        fromArray([1, 2, 3]),
        map((x) => x * 2),
        reduce(add, 0),
      ),
    ).toBe(12);
  });

  it('map + toArray', () => {
    expect(
      stream(
        fromArray([1, 2, 3]),
        map((x) => x * 2),
        toArray(),
      ),
    ).toEqual([2, 4, 6]);
  });

  it('filter + toArray', () => {
    expect(
      stream(
        fromArray([1, 2, 3, 4, 5]),
        filter((x) => x % 2 === 0),
        toArray(),
      ),
    ).toEqual([2, 4]);
  });

  it('throws on no emission', () => {
    expect(() => stream(empty())).toThrow('did not emit');
  });

  it('throws on error', () => {
    const errSource: Source<number> = {
      connect(sink) {
        return {
          resume() {
            sink.error(new Error('boom'));
          },
          [Symbol.dispose]() {},
        };
      },
    };
    expect(() => stream(errSource)).toThrow('boom');
  });

  it('empty array with toArray', () => {
    expect(stream(fromArray([]), toArray())).toEqual([]);
  });

  it('of() works like fromArray', () => {
    expect(stream(of(1, 2, 3), toArray())).toEqual([1, 2, 3]);
  });
});

// ---------------------------------------------------------------------------
// pipe() — lazy composition
// ---------------------------------------------------------------------------

describe('pipe()', () => {
  it('returns a Source', () => {
    const source = pipe(
      fromArray([1, 2, 3]),
      map((x) => x * 2),
    );
    expect(source).toHaveProperty('connect');
  });

  it('does not connect', () => {
    let connected = false;
    const spySource: Source<number> = {
      connect(sink) {
        connected = true;
        return fromArray([1]).connect(sink);
      },
    };
    pipe(
      spySource,
      map((x) => x * 2),
    );
    expect(connected).toBe(false);
  });

  it('composed source works when consumed', () => {
    const doubled = pipe(
      fromArray([1, 2, 3]),
      map((x) => x * 2),
    );
    expect(stream(doubled, toArray())).toEqual([2, 4, 6]);
  });
});

// ---------------------------------------------------------------------------
// comp() — operator composition
// ---------------------------------------------------------------------------

describe('comp()', () => {
  it('composes operators', () => {
    const doubleAndTake2 = comp(
      'doubleAndTake2',
      map((x: number) => x * 2),
      take(2),
    );
    expect(stream(fromArray([1, 2, 3, 4]), doubleAndTake2, toArray())).toEqual([2, 4]);
  });
});

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

describe('fromArray', () => {
  it('emits all values and completes', () => {
    const sink = collectSink<number>();
    const s = fromArray([1, 2, 3]).connect(sink);
    s.resume();
    expect(sink.values).toEqual([1, 2, 3]);
    expect(sink.completed).toBe(true);
  });

  it('empty array completes immediately', () => {
    const sink = collectSink<number>();
    const s = fromArray([]).connect(sink);
    s.resume();
    expect(sink.values).toEqual([]);
    expect(sink.completed).toBe(true);
  });
});

describe('fromIterator', () => {
  it('emits values from iterator and completes', () => {
    function* gen() {
      yield 1;
      yield 2;
      yield 3;
    }
    expect(stream(fromIterator(gen()), toArray())).toEqual([1, 2, 3]);
  });

  it('respects PAUSE', () => {
    function* gen() {
      yield 1;
      yield 2;
      yield 3;
    }
    const sink = pausingSink<number>(2);
    const s = fromIterator(gen()).connect(sink);
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    s.resume();
    expect(sink.values).toEqual([1, 2, 3]);
    expect(sink.completed).toBe(true);
  });

  it('infinite iterator with take', () => {
    function* naturals() {
      let i = 0;
      while (true) yield i++;
    }
    expect(stream(fromIterator(naturals()), take(5), toArray())).toEqual([0, 1, 2, 3, 4]);
  });
});

describe('empty', () => {
  it('completes immediately', () => {
    const sink = collectSink<number>();
    const s = empty<number>().connect(sink);
    s.resume();
    expect(sink.values).toEqual([]);
    expect(sink.completed).toBe(true);
  });
});

describe('deferred', () => {
  it('resolve before resume', () => {
    const d = deferred<number>();
    const sink = collectSink<number>();
    const s = d.connect(sink);
    d.resolve(42);
    s.resume();
    expect(sink.values).toEqual([42]);
    expect(sink.completed).toBe(true);
  });

  it('resolve after resume', () => {
    const d = deferred<number>();
    const sink = collectSink<number>();
    const s = d.connect(sink);
    s.resume();
    expect(sink.values).toEqual([]);
    d.resolve(42);
    expect(sink.values).toEqual([42]);
    expect(sink.completed).toBe(true);
  });

  it('reject after resume', () => {
    const d = deferred<number>();
    // Suppress unhandled promise rejection from internal promise
    d.promise.catch(() => {});
    const sink = collectSink<number>();
    const s = d.connect(sink);
    s.resume();
    d.reject(new Error('fail'));
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('fail');
  });

  it('dispose before resolve — sink never called', () => {
    const d = deferred<number>();
    const sink = collectSink<number>();
    const s = d.connect(sink);
    s.resume();
    s[Symbol.dispose]();
    d.resolve(42);
    expect(sink.values).toEqual([]);
    expect(sink.completed).toBe(false);
  });

  it('multiple subscribers', () => {
    const d = deferred<number>();
    const sink1 = collectSink<number>();
    const sink2 = collectSink<number>();
    const s1 = d.connect(sink1);
    const s2 = d.connect(sink2);
    s1.resume();
    s2.resume();
    d.resolve(99);
    expect(sink1.values).toEqual([99]);
    expect(sink2.values).toEqual([99]);
  });

  it('promise resolves', async () => {
    const d = deferred<number>();
    d.resolve(7);
    expect(await d.promise).toBe(7);
  });

  it('promise rejects', async () => {
    const d = deferred<number>();
    d.reject(new Error('nope'));
    await expect(d.promise).rejects.toThrow('nope');
  });
});

// ---------------------------------------------------------------------------
// Operators
// ---------------------------------------------------------------------------

describe('map', () => {
  it('transforms values', () => {
    expect(
      stream(
        fromArray([1, 2, 3]),
        map((x) => x * 10),
        toArray(),
      ),
    ).toEqual([10, 20, 30]);
  });

  it('error in mapFn sends error downstream', () => {
    const errSource = pipe(
      fromArray([1, 2, 3]),
      map(() => {
        throw new Error('mapErr');
      }),
    );
    const sink = collectSink<never>();
    const s = errSource.connect(sink);
    s.resume();
    expect(sink.errors).toHaveLength(1);
    expect((sink.errors[0] as Error).message).toBe('mapErr');
  });
});

describe('filter', () => {
  it('filters values', () => {
    expect(
      stream(
        fromArray([1, 2, 3, 4, 5, 6]),
        filter((x) => x % 2 === 0),
        toArray(),
      ),
    ).toEqual([2, 4, 6]);
  });

  it('no matches → empty', () => {
    expect(
      stream(
        fromArray([1, 3, 5]),
        filter((x) => x % 2 === 0),
        toArray(),
      ),
    ).toEqual([]);
  });

  it('error in predicate sends error downstream', () => {
    const errSource = pipe(
      fromArray([1]),
      filter(() => {
        throw new Error('predErr');
      }),
    );
    const sink = collectSink<number>();
    const s = errSource.connect(sink);
    s.resume();
    expect(sink.errors).toHaveLength(1);
  });
});

describe('take', () => {
  it('emits N then completes', () => {
    expect(stream(fromArray([1, 2, 3, 4, 5]), take(3), toArray())).toEqual([1, 2, 3]);
  });

  it('take(0) completes immediately', () => {
    expect(stream(fromArray([1, 2, 3]), take(0), toArray())).toEqual([]);
  });

  it('take more than available', () => {
    expect(stream(fromArray([1, 2]), take(10), toArray())).toEqual([1, 2]);
  });
});

describe('scan', () => {
  it('emits intermediate accumulations', () => {
    expect(stream(fromArray([1, 2, 3]), scan(add, 0), toArray())).toEqual([1, 3, 6]);
  });

  it('empty source → no emissions', () => {
    expect(stream(fromArray<number>([]), scan(add, 0), toArray())).toEqual([]);
  });
});

describe('reduce', () => {
  it('accumulate and emit on complete', () => {
    expect(stream(fromArray([1, 2, 3, 4]), reduce(add, 0))).toBe(10);
  });

  it('empty source → emits initial', () => {
    expect(stream(fromArray<number>([]), reduce(add, 0))).toBe(0);
  });

  it('single value', () => {
    expect(stream(fromArray([42]), reduce(add, 0))).toBe(42);
  });
});

describe('flatten', () => {
  it('flattens source of sources', () => {
    const inner1 = fromArray([1, 2]);
    const inner2 = fromArray([3, 4]);
    expect(stream(fromArray([inner1, inner2]), flatten(), toArray())).toEqual([1, 2, 3, 4]);
  });

  it('empty inner sources', () => {
    expect(
      stream(fromArray([empty<number>(), fromArray([1]), empty<number>()]), flatten(), toArray()),
    ).toEqual([1]);
  });

  it('empty outer', () => {
    expect(stream(fromArray<Source<number>>([]), flatten(), toArray())).toEqual([]);
  });

  it('single inner', () => {
    expect(stream(fromArray([fromArray([10, 20])]), flatten(), toArray())).toEqual([10, 20]);
  });
});

describe('catchError', () => {
  it('recovers from error with fallback source', () => {
    const failing: Source<number> = {
      connect(sink) {
        return {
          resume() {
            sink.next(1);
            sink.error(new Error('fail'));
          },
          [Symbol.dispose]() {},
        };
      },
    };
    expect(
      stream(
        pipe(
          failing,
          catchError(() => fromArray([99])),
        ),
        toArray(),
      ),
    ).toEqual([1, 99]);
  });

  it('no error → passthrough', () => {
    expect(
      stream(
        fromArray([1, 2, 3]),
        catchError(() => fromArray([99])),
        toArray(),
      ),
    ).toEqual([1, 2, 3]);
  });
});

describe('pausable', () => {
  it('buffers when paused, drains on resume', () => {
    const sink = pausingSink<number>(2);
    const s = pipe(fromArray([1, 2, 3, 4, 5]), pausable()).connect(sink);
    s.resume();
    expect(sink.values).toEqual([1, 2]);
    s.resume();
    expect(sink.values).toEqual([1, 2, 3, 4]);
    s.resume();
    expect(sink.values).toEqual([1, 2, 3, 4, 5]);
    expect(sink.completed).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Collecting operators
// ---------------------------------------------------------------------------

describe('toArray', () => {
  it('collects all values', () => {
    expect(stream(fromArray([1, 2, 3]), toArray())).toEqual([1, 2, 3]);
  });
});

describe('toSource', () => {
  it('wraps pipeline as a single value', () => {
    const src = stream(
      fromArray([1, 2, 3]),
      map((x) => x * 2),
      toSource(),
    );
    // src is Source<number> — consume it
    expect(stream(src, toArray())).toEqual([2, 4, 6]);
  });
});

describe('toPromise', () => {
  it('resolves with last value', async () => {
    const d = deferred<number>();
    const p = stream(d, toPromise());
    d.resolve(42);
    expect(await p).toBe(42);
  });
});

// ---------------------------------------------------------------------------
// Relay
// ---------------------------------------------------------------------------

describe('createRelay', () => {
  it('multicasts to subscribers', () => {
    const relay = createRelay<number>();
    const sink1 = collectSink<number>();
    const sink2 = collectSink<number>();
    const s1 = relay.connect(sink1);
    const s2 = relay.connect(sink2);
    s1.resume();
    s2.resume();

    relay.next(1);
    relay.next(2);
    relay.complete();

    expect(sink1.values).toEqual([1, 2]);
    expect(sink1.completed).toBe(true);
    expect(sink2.values).toEqual([1, 2]);
    expect(sink2.completed).toBe(true);
  });

  it('late subscriber misses past values', () => {
    const relay = createRelay<number>();
    const sink1 = collectSink<number>();
    const s1 = relay.connect(sink1);
    s1.resume();

    relay.next(1);

    const sink2 = collectSink<number>();
    const s2 = relay.connect(sink2);
    s2.resume();

    relay.next(2);
    relay.complete();

    expect(sink1.values).toEqual([1, 2]);
    expect(sink2.values).toEqual([2]); // Missed value 1
  });

  it('backpressure per subscriber', () => {
    const relay = createRelay<number>();
    const fastSink = collectSink<number>();
    const slowSink = pausingSink<number>(1);

    const s1 = relay.connect(fastSink);
    const s2 = relay.connect(slowSink);
    s1.resume();
    s2.resume();

    relay.next(1);
    relay.next(2);
    relay.next(3);

    // Fast got all values; slow is paused after 1
    expect(fastSink.values).toEqual([1, 2, 3]);
    expect(slowSink.values).toEqual([1]);

    // Resume slow — drains buffer
    s2.resume();
    expect(slowSink.values).toEqual([1, 2]);
    s2.resume();
    expect(slowSink.values).toEqual([1, 2, 3]);
  });

  it('error propagates to all subscribers', () => {
    const relay = createRelay<number>();
    const sink = collectSink<number>();
    const s = relay.connect(sink);
    s.resume();

    relay.error(new Error('relay-err'));
    expect(sink.errors).toHaveLength(1);
  });

  it('dispose removes subscription', () => {
    const relay = createRelay<number>();
    const sink = collectSink<number>();
    const s = relay.connect(sink);
    s.resume();

    relay.next(1);
    s[Symbol.dispose]();
    relay.next(2);

    expect(sink.values).toEqual([1]); // Only got value before dispose
  });
});

// ---------------------------------------------------------------------------
// stream() contract: exactly one value, completed synchronously
// ---------------------------------------------------------------------------

describe('stream() contract', () => {
  it('throws when the final source emits more than one value', () => {
    expect(() => stream(fromArray([1, 2, 3]))).toThrow(/emitted 3 values/);
  });

  it('throws and releases the connection when the source does not complete', () => {
    let disposed = false;
    const src = {
      connect(sink: { next(v: number): unknown }) {
        return { resume() { sink.next(1); }, [Symbol.dispose]() { disposed = true; } };
      },
    };
    expect(() => stream(src)).toThrow(/did not complete/);
    expect(disposed).toBe(true);
  });

  it('propagates an undefined error', () => {
    const src = {
      connect(sink: { error(e: unknown): void }) {
        return { resume() { sink.error(undefined); }, [Symbol.dispose]() {} };
      },
    };
    expect(() => stream(src)).toThrow();
  });

  it('comp() names the composed operator', () => {
    const op = comp('doubleEvens', filter((n: number) => n % 2 === 0), map((n: number) => n * 2));
    expect(op.name).toBe('doubleEvens');
    expect(stream(fromArray([1, 2, 3, 4]), op, toArray())).toEqual([4, 8]);
  });
});

describe('edge operators disposed from inside next()', () => {
  it('do not complete after the sink disposed the stream', () => {
    let completes = 0;
    const handle: { s?: { resume(): void; [Symbol.dispose](): void } } = {};
    handle.s = pipe(fromArray([1]), toArray()).connect({
      next() { handle.s?.[Symbol.dispose](); return undefined; },
      complete() { completes++; },
      error() {},
    });
    handle.s.resume();
    expect(completes).toBe(0);
  });
});
