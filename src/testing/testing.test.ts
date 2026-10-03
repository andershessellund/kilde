// ---------------------------------------------------------------------------
// Testing framework self-tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { DecisionsError, IncompleteError, ViolationError, decisionsOf, runOnce } from 'stifinder';
import { testSource } from './test-source.js';
import { testSink } from './test-sink.js';
import { exhaustiveTest } from './exhaustive.js';
import { toArray } from '../operators/to-array.js';
import { pipe, stream } from '../stream.js';

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
    // Every prefix of every sequence: 1 + 2 + 4 + 8.
    expect(stats.states).toBe(15);
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
    const error = await exhaustiveTest((oracle) => {
      const a = oracle.integer(2);
      const b = oracle.integer(2);
      if (a === 1 && b === 1) throw new Error('two deviations');
      const c = oracle.integer(2);
      if (a === 0 && b === 0 && c === 1) throw new Error('one deviation');
    }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ViolationError);
    expect(decisionsOf(error as ViolationError)).toEqual([0, 0, 1]);
    expect((error as ViolationError).cause).toEqual(new Error('one deviation'));
  });

  it('names each deviation in the report, in the words of the decision point', async () => {
    await expect(
      exhaustiveTest((oracle) => {
        const sink = testSink<number>({ oracle });
        pipe(testSource([1, 2], { oracle })).connect(sink).resume();
        if (sink.values.length < 2) throw new Error('stopped early');
      }),
    ).rejects.toThrow(/stopped early\n1 deviation[\s\S]*sink pauses after value #1 \(1\)/);
  });

  it('fails a body that throws before its first decision', async () => {
    await expect(
      exhaustiveTest(() => {
        throw new Error('no decisions made');
      }),
    ).rejects.toThrow(/no decisions made\nno steps: the initial state fails/);
  });

  it('awaits a body that returns a promise, and fails it on rejection', async () => {
    const error = await exhaustiveTest(async (oracle) => {
      const v = oracle.integer(2);
      await Promise.resolve();
      if (v === 1) throw new Error('rejected after a pause');
    }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ViolationError);
    expect(decisionsOf(error as ViolationError)).toEqual([1]);
  });

  it('runs a reported failure again with runOnce', async () => {
    const body = (oracle: { integer(range: number): number }) => {
      if (oracle.integer(3) === 2) throw new Error('bad pick');
    };
    const error = await exhaustiveTest(body).catch((e: unknown) => e);
    await expect(runOnce(body, error as ViolationError)).rejects.toThrow('bad pick');
  });

  it('cuts off a run past maxDecisions, and fails it', async () => {
    await expect(
      exhaustiveTest(
        (oracle) => {
          for (;;) oracle.integer(2);
        },
        { maxDecisions: 50 },
      ),
    ).rejects.toThrow(/more than 50 decisions/);
  });

  it('rejects when maxEdges cuts the search short', async () => {
    await expect(
      exhaustiveTest(
        (oracle) => {
          for (let i = 0; i < 10; i++) oracle.integer(2);
        },
        { maxEdges: 5 },
      ),
    ).rejects.toBeInstanceOf(IncompleteError);
  });

  it('rejects a body that is not deterministic', async () => {
    let calls = 0;
    await expect(
      exhaustiveTest((oracle) => {
        calls++;
        oracle.integer(calls === 1 ? 2 : 3);
      }),
    ).rejects.toThrow(DecisionsError);
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
    ).rejects.toThrow(/bad permutation[\s\S]*decisions \[2\]/);
  });
});
