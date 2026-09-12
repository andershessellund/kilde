// ---------------------------------------------------------------------------
// lines — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe, stream } from '../stream.js';
import { fromArray } from '../sources/from-array.js';
import { createRelay } from '../relay.js';
import { lines } from './lines.js';
import { toArray } from './to-array.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';
import { assertProtocol } from '../testing/protocol.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 80) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

function check(chunks: (string | Uint8Array)[], expected: string[]) {
  exhaustiveTest((oracle) => {
    const src = testSource(chunks, { oracle });
    const sink = testSink<string>({ oracle });
    const s = pipe(src, lines(), assertProtocol()).connect(sink);
    drive(s, sink);
    expect(sink.values).toEqual(expected);
    expect(sink.completeCount).toBe(1);
  });
}

describe('lines (exhaustive)', () => {
  it('single chunk with two lines', () => check(['hello\nworld\n'], ['hello', 'world']));
  it('multiple lines in one chunk', () => check(['a\nb\nc\n'], ['a', 'b', 'c']));
  it('partial lines across chunks', () => check(['hel', 'lo\nwor', 'ld\n'], ['hello', 'world']));
  it('\\r\\n line endings', () => check(['hello\r\nworld\r\n'], ['hello', 'world']));
  it('\\r at chunk boundary', () => check(['hello\r', '\nworld\n'], ['hello', 'world']));
  it('trailing data without newline', () => check(['hello\nworld'], ['hello', 'world']));
  it('trailing data in its own chunk', () => check(['a\n', 'b\n', 'c'], ['a', 'b', 'c']));
  it('empty lines preserved', () => check(['a\n\nb\n'], ['a', '', 'b']));
  it('single line no trailing newline', () => check(['hello'], ['hello']));
  it('empty chunks', () => check(['', 'hello\n', ''], ['hello']));
  it('empty source', () => check([], []));
  it('many small chunks — one byte at a time', () => check('hi\n'.split(''), ['hi']));
  it('Uint8Array input', () =>
    check([new TextEncoder().encode('hello\nworld\n')], ['hello', 'world']));

  it('Uint8Array — multi-byte character split across chunks', () => {
    // "café\n" in UTF-8: [99, 97, 102, 195, 169, 10]
    // Split between the two bytes of "é" (0xC3 0xA9)
    const full = new TextEncoder().encode('café\n');
    check([full.slice(0, 4), full.slice(4)], ['café']);
  });

  it('Uint8Array — trailing partial line is flushed on complete', () => {
    const full = new TextEncoder().encode('a\nbé');
    check([full.slice(0, 3), full.slice(3)], ['a', 'bé']);
  });
});

describe('lines (bug 1 — completes after trailing data)', () => {
  it('stream(fromArray(["abc"]), lines(), toArray()) yields ["abc"]', () => {
    expect(stream(fromArray(['abc']), lines(), toArray())).toEqual(['abc']);
  });

  it('completes on the very resume that delivers the trailing line (no extra resume needed)', () => {
    const sink = testSink<string>();
    const s = pipe(fromArray(['a\nb']), lines(), assertProtocol()).connect(sink);
    s.resume(); // exactly one resume
    expect(sink.values).toEqual(['a', 'b']);
    expect(sink.completeCount).toBe(1);
  });

  it('a trailing "\\r" alone produces no line', () => {
    expect(stream(fromArray(['a\n\r']), lines(), toArray())).toEqual(['a']);
  });
});

describe('lines (buffering)', () => {
  it('holds complete() until queued lines have been delivered', () => {
    const relay = createRelay<string>();
    let pauseNext = true;
    const sink = testSink<string>({ oracle: { integer: () => (pauseNext ? 1 : 0) } });
    const s = pipe(relay, lines(), assertProtocol()).connect(sink);
    s.resume();
    relay.next('a\nb\nc\n'); // 'a' delivered, sink pauses, 'b' and 'c' queued
    relay.complete();
    expect(sink.values).toEqual(['a']);
    expect(sink.completeCount).toBe(0);

    pauseNext = false;
    s.resume();
    expect(sink.values).toEqual(['a', 'b', 'c']);
    expect(sink.completeCount).toBe(1);
  });

  it('holds error() until queued lines have been delivered', () => {
    const relay = createRelay<string>();
    let pauseNext = true;
    const sink = testSink<string>({ oracle: { integer: () => (pauseNext ? 1 : 0) } });
    const s = pipe(relay, lines(), assertProtocol()).connect(sink);
    s.resume();
    relay.next('a\nb\n');
    relay.error(new Error('boom'));
    expect(sink.errors).toHaveLength(0);

    pauseNext = false;
    s.resume();
    expect(sink.values).toEqual(['a', 'b']);
    expect(sink.errors).toHaveLength(1);
  });
});
