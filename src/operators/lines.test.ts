// ---------------------------------------------------------------------------
// lines — exhaustive tests
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import { pipe } from '../stream.js';
import { lines } from './lines.js';
import { testSource } from '../testing/test-source.js';
import { testSink } from '../testing/test-sink.js';
import { exhaustiveTest } from '../testing/exhaustive.js';

function drive(s: { resume(): void }, sink: { completeCount: number }, max = 80) {
  for (let i = 0; i < max && !sink.completeCount; i++) s.resume();
}

describe('lines (exhaustive)', () => {
  it('single chunk with two lines', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(['hello\nworld\n'], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(src, lines()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['hello', 'world']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('multiple lines in one chunk', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(['a\nb\nc\n'], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(src, lines()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['a', 'b', 'c']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('partial lines across chunks', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(['hel', 'lo\nwor', 'ld\n'], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(src, lines()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['hello', 'world']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('\\r\\n line endings', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(['hello\r\nworld\r\n'], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(src, lines()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['hello', 'world']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('\\r at chunk boundary', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(['hello\r', '\nworld\n'], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(src, lines()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['hello', 'world']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('trailing data without newline', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(['hello\nworld'], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(src, lines()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['hello', 'world']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty lines preserved', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(['a\n\nb\n'], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(src, lines()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['a', '', 'b']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('single line no trailing newline', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(['hello'], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(src, lines()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['hello']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty chunks', () => {
    exhaustiveTest((oracle) => {
      const src = testSource(['', 'hello\n', ''], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(src, lines()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['hello']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('empty source', () => {
    exhaustiveTest((oracle) => {
      const src = testSource<string>([], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(src, lines()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual([]);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('Uint8Array input', () => {
    const encoder = new TextEncoder();
    exhaustiveTest((oracle) => {
      const src = testSource<string | Uint8Array>([encoder.encode('hello\nworld\n')], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(src, lines()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['hello', 'world']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('Uint8Array — multi-byte character split across chunks', () => {
    // "café\n" in UTF-8: [99, 97, 102, 195, 169, 10]
    // Split between the two bytes of "é" (0xC3 0xA9)
    const full = new TextEncoder().encode('café\n');
    const chunk1 = full.slice(0, 4); // [99, 97, 102, 195] — partial é
    const chunk2 = full.slice(4); // [169, 10] — rest of é + \n

    exhaustiveTest((oracle) => {
      const src = testSource<string | Uint8Array>([chunk1, chunk2], { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(src, lines()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['café']);
      expect(sink.completeCount).toBe(1);
    });
  });

  it('many small chunks — one byte at a time', () => {
    const chunks = 'hi\n'.split(''); // ['h', 'i', '\n']
    exhaustiveTest((oracle) => {
      const src = testSource(chunks, { oracle });
      const sink = testSink<string>({ oracle });
      const s = pipe(src, lines()).connect(sink);
      drive(s, sink);
      expect(sink.values).toEqual(['hi']);
      expect(sink.completeCount).toBe(1);
    });
  });
});
