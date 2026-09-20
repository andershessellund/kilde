// ---------------------------------------------------------------------------
// empty — protocol tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { empty } from './empty.js';
import { pipe, stream } from '../stream.js';
import { toArray } from '../operators/to-array.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

describe('empty', () => {
  it('completes without values', () => {
    expect(stream(empty<number>(), toArray())).toEqual([]);
  });

  it('does nothing before the first resume()', () => {
    const sink = testSink<never>();
    pipe(empty(), assertProtocol()).connect(sink);
    expect(sink.completeCount).toBe(0);
  });

  it('completes exactly once no matter how often resume() is called', async () => {
    await exhaustiveTest((oracle) => {
      const sink = testSink<never>({ oracle });
      const s = pipe(empty(), assertProtocol()).connect(sink);
      s.resume();
      s.resume();
      s.resume();
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('dispose before resume: nothing is delivered', () => {
    const sink = testSink<never>();
    const s = pipe(empty(), assertProtocol()).connect(sink);
    s[Symbol.dispose]();
    s.resume();
    expect(sink.completeCount).toBe(0);
  });

  it('shares one instance but each connection is independent', () => {
    const a = testSink<never>();
    const b = testSink<never>();
    empty().connect(a).resume();
    const sb = empty().connect(b);
    expect(a.completeCount).toBe(1);
    expect(b.completeCount).toBe(0);
    sb.resume();
    expect(b.completeCount).toBe(1);
  });
});
