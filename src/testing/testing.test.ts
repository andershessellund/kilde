// ---------------------------------------------------------------------------
// Testing framework self-tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { ExhaustiveOracle, incrementSequence } from './oracle.js';
import { testSource } from './test-source.js';
import { testSink } from './test-sink.js';
import { exhaustiveTest } from './exhaustive.js';
import { toArray } from '../operators/to-array.js';
import { stream } from '../stream.js';

describe('ExhaustiveOracle', () => {
  it('first run returns 0 for all decisions', () => {
    const oracle = new ExhaustiveOracle([]);
    expect(oracle.integer(3)).toBe(0); // first unknown → 0
    expect(oracle.integer(5)).toBe(0);
    expect(oracle.getSequence()).toEqual([
      { picked: 0, range: 3 },
      { picked: 0, range: 5 },
    ]);
  });

  it('replays known decisions', () => {
    const oracle = new ExhaustiveOracle([
      { picked: 1, range: 3 },
      { picked: 2, range: 5 },
    ]);
    expect(oracle.integer(3)).toBe(1);
    expect(oracle.integer(5)).toBe(2);
  });

  it('extends with 0 when past known decisions', () => {
    const oracle = new ExhaustiveOracle([{ picked: 1, range: 3 }]);
    expect(oracle.integer(3)).toBe(1); // replay
    expect(oracle.integer(4)).toBe(0); // new → 0
    expect(oracle.getSequence()).toHaveLength(2);
  });
});

describe('incrementSequence', () => {
  it('increments last digit', () => {
    const seq = [
      { picked: 0, range: 3 },
      { picked: 0, range: 2 },
    ];
    const result = incrementSequence(seq);
    expect(result).toEqual([
      { picked: 0, range: 3 },
      { picked: 1, range: 2 },
    ]);
  });

  it('carries over (drops overflowed digit)', () => {
    const seq = [
      { picked: 0, range: 3 },
      { picked: 1, range: 2 },
    ];
    const result = incrementSequence(seq);
    // Last digit overflows → popped, first digit incremented
    expect(result).toEqual([{ picked: 1, range: 3 }]);
  });

  it('returns empty when fully explored', () => {
    const seq = [
      { picked: 2, range: 3 },
      { picked: 1, range: 2 },
    ];
    const result = incrementSequence(seq);
    expect(result).toEqual([]);
  });

  it('empty input stays empty', () => {
    expect(incrementSequence([])).toEqual([]);
  });
});

describe('testSource', () => {
  it('emits values with oracle controlling pause', () => {
    // Oracle always returns 0 → never self-pause
    const oracle = { integer: () => 0 };
    const src = testSource([10, 20, 30], { oracle });
    expect(stream(src, toArray())).toEqual([10, 20, 30]);
  });
});

describe('testSink', () => {
  it('records values and complete', () => {
    const oracle = { integer: () => 0 }; // never pause
    const sink = testSink<number>({ oracle });
    sink.next(1);
    sink.next(2);
    sink.complete();
    expect(sink.values).toEqual([1, 2]);
    expect(sink.completeCount).toBe(1);
  });

  it('records error', () => {
    const oracle = { integer: () => 0 };
    const sink = testSink<number>({ oracle });
    sink.error(new Error('oops'));
    expect(sink.errors).toHaveLength(1);
  });
});

describe('exhaustiveTest', () => {
  it('explores all permutations of a trivial case', () => {
    let runCount = 0;
    exhaustiveTest((oracle) => {
      runCount++;
      const choice = oracle.integer(2); // 0 or 1
      if (choice === 0) {
        expect(1 + 1).toBe(2);
      } else {
        expect(2 + 2).toBe(4);
      }
    });
    expect(runCount).toBe(2);
  });

  it('explores multiple decision points', () => {
    let runCount = 0;
    exhaustiveTest((oracle) => {
      runCount++;
      oracle.integer(2); // 2 choices
      oracle.integer(3); // 3 choices
    });
    expect(runCount).toBe(6); // 2 * 3
  });

  it('throws on first failure with permutation info', () => {
    expect(() => {
      exhaustiveTest((oracle) => {
        const v = oracle.integer(3);
        if (v === 2) throw new Error('bad permutation');
      });
    }).toThrow(/permutation/i);
  });
});
