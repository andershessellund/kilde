// ---------------------------------------------------------------------------
// Testing framework self-tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { testSource } from './test-source.js';
import { testSink } from './test-sink.js';
import { exhaustiveTest } from './exhaustive.js';
import { toArray } from '../operators/to-array.js';
import { stream } from '../stream.js';

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
  it('runs the body once per leaf of the decision tree', async () => {
    let runCount = 0;
    const stats = await exhaustiveTest((oracle) => {
      runCount++;
      oracle.integer(2);
      oracle.integer(2);
      oracle.integer(2);
    });
    expect(runCount).toBe(8);
    expect(stats.runs).toBe(8);
    expect(stats.completed).toBe(true);
    expect(stats.exhaustive).toBe(true);
  });

  it('says a space bounded by maxDeviations was not exhausted', async () => {
    const stats = await exhaustiveTest(
      (oracle) => {
        oracle.integer(2);
        oracle.integer(2);
        oracle.integer(2);
      },
      { maxDeviations: 1 },
    );
    // The plain schedule, then one run per single deviation.
    expect(stats.runs).toBe(4);
    expect(stats.maxDeviationsReached).toBe(1);
    expect(stats.completed).toBe(true);
    expect(stats.exhaustive).toBe(false);
  });

  it('reports the failure with the fewest deviations, not the first found', async () => {
    // Fails on [1, 1] (two deviations) and on [0, 0, 1] (one deviation).
    await expect(
      exhaustiveTest((oracle) => {
        const a = oracle.integer(2);
        const b = oracle.integer(2);
        if (a === 1 && b === 1) throw new Error('two deviations');
        const c = oracle.integer(2);
        if (a === 0 && b === 0 && c === 1) throw new Error('one deviation');
      }),
    ).rejects.toThrow(/1 deviation:[\s\S]*3 decisions[\s\S]*decisions: \[0, 0, 1\][\s\S]*one deviation/);
  });

  it('rejects a body that is not deterministic', async () => {
    let calls = 0;
    await expect(
      exhaustiveTest((oracle) => {
        calls++;
        oracle.integer(calls === 1 ? 2 : 3);
      }),
    ).rejects.toThrow(/not deterministic/);
  });

  it('explores all permutations of a trivial case', async () => {
    let runCount = 0;
    await exhaustiveTest((oracle) => {
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

  it('explores multiple decision points', async () => {
    let runCount = 0;
    await exhaustiveTest((oracle) => {
      runCount++;
      oracle.integer(2); // 2 choices
      oracle.integer(3); // 3 choices
    });
    expect(runCount).toBe(6); // 2 * 3
  });

  it('throws on failure with the decision sequence', async () => {
    await expect(
      exhaustiveTest((oracle) => {
        const v = oracle.integer(3);
        if (v === 2) throw new Error('bad permutation');
      }),
    ).rejects.toThrow(/decisions: \[2\]|decisions: \[2\/3\]/);
  });
});
