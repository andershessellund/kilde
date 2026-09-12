// ---------------------------------------------------------------------------
// relay — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe } from './stream.js';
import { map } from './operators/map.js';
import { filter } from './operators/filter.js';
import { take } from './operators/take.js';
import { createRelay } from './relay.js';
import { testSink } from './testing/test-sink.js';
import { exhaustiveTest } from './testing/exhaustive.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 40) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('relay (exhaustive)', () => {
  it('relay through map — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const relay = createRelay<number>();
      const mapped = pipe(
        relay,
        map((x) => x * 10),
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

  it('relay through filter — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const relay = createRelay<number>();
      const filtered = pipe(
        relay,
        filter((x) => x % 2 === 0),
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

  it('relay through take — early termination', () => {
    exhaustiveTest((oracle) => {
      const relay = createRelay<number>();
      const taken = pipe(relay, take(2));
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

  it('two subscribers — all pause orderings per subscriber', () => {
    exhaustiveTest((oracle) => {
      const relay = createRelay<number>();
      const sink1 = testSink<number>({ oracle });
      const sink2 = testSink<number>({ oracle });
      const s1 = relay.connect(sink1);
      const s2 = relay.connect(sink2);
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

  it('relay error — all pause orderings', () => {
    exhaustiveTest((oracle) => {
      const relay = createRelay<number>();
      const sink = testSink<number>({ oracle });
      const s = relay.connect(sink);
      s.resume();

      relay.next(1);
      relay.error(new Error('relay-err'));

      drive(s, sink);
      expect(sink.values).toEqual([1]);
      expect(sink.errors).toHaveLength(1);
      expect((sink.errors[0] as Error).message).toBe('relay-err');
    });
  });
});
