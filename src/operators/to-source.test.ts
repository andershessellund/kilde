// ---------------------------------------------------------------------------
// toSource — tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import type { Source, Sink } from '../types.js';
import { stream, pipe } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { map } from './map.js';
import { toArray } from './to-array.js';
import { toSource } from './to-source.js';
import { testSink } from '../testing/test-sink.js';
import { assertProtocol } from '../testing/protocol.js';

describe('toSource()', () => {
  it('wraps the pipeline without consuming it', () => {
    let connects = 0;
    const src: Source<number> = {
      connect(sink: Sink<number>) {
        connects++;
        return fromArray([1, 2, 3]).connect(sink);
      },
    };
    const wrapped = stream(src, map((x) => x * 2), toSource());
    expect(connects).toBe(0);
    expect(stream(wrapped, toArray())).toEqual([2, 4, 6]);
    expect(connects).toBe(1);
  });

  it('bug 12: a second resume() emits only one source and one complete()', () => {
    const sink = testSink<Source<number>>();
    const s = pipe(fromArray([1]), toSource(), assertProtocol()).connect(sink);
    s.resume();
    s.resume();
    expect(sink.values).toHaveLength(1);
    expect(sink.completeCount).toBe(1);
  });

  it('emits nothing after dispose', () => {
    const sink = testSink<Source<number>>();
    const s = pipe(fromArray([1]), toSource()).connect(sink);
    s[Symbol.dispose]();
    s.resume();
    expect(sink.values).toHaveLength(0);
    expect(sink.completeCount).toBe(0);
  });
});
