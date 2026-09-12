// ---------------------------------------------------------------------------
// lines — split chunks into lines
//
// Accepts string or Uint8Array chunks (binary decoded via TextDecoder
// with streaming mode for correct multi-byte handling). Emits one
// string per line. Handles \n and \r\n. Trailing data without a
// newline is emitted on complete.
//
// One-to-many: a single chunk may produce multiple lines. If downstream
// returns PAUSE during emission, remaining lines are queued and drained
// on resume.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator, PAUSE } from '../types.js';
import { PAUSE as PAUSE_SYM } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class LinesStream implements Stream, Sink<string | Uint8Array> {
  #upstream!: Stream;
  #decoder = new TextDecoder();
  #partial = '';
  #queue: string[] = [];
  #disposed = false;
  #pendingComplete = false;
  #pendingError: unknown;
  #hasPendingError = false;

  constructor(private readonly sink: Sink<string>) {}

  _setUpstream(upstream: Stream): void {
    this.#upstream = upstream;
  }

  // --- Sink<string | Uint8Array> ---

  next(chunk: string | Uint8Array): undefined | PAUSE {
    if (this.#disposed) return PAUSE_SYM;

    const text = typeof chunk === 'string' ? chunk : this.#decoder.decode(chunk, { stream: true });

    this.#partial += text;
    const parts = this.#partial.split('\n');
    this.#partial = parts.pop()!; // last element is the incomplete tail

    // Strip trailing \r for \r\n support
    for (let i = 0; i < parts.length; i++) {
      if (parts[i].endsWith('\r')) {
        parts[i] = parts[i].slice(0, -1);
      }
    }

    this.#queue.push(...parts);
    return this.#drain();
  }

  complete(): void {
    if (this.#disposed) return;

    // Flush the TextDecoder
    const remaining = this.#decoder.decode(new Uint8Array(0));
    if (remaining) this.#partial += remaining;

    // Emit trailing data as a final line
    if (this.#partial) {
      // Strip trailing \r
      if (this.#partial.endsWith('\r')) {
        this.#partial = this.#partial.slice(0, -1);
      }
      if (this.#partial) {
        this.#queue.push(this.#partial);
      }
      this.#partial = '';
    }

    if (this.#queue.length > 0) {
      this.#pendingComplete = true;
      // Drain what we can — if PAUSE, complete fires on resume
      this.#drain();
    } else {
      this.sink.complete();
    }
  }

  error(error: unknown): void {
    if (this.#disposed) return;
    if (this.#queue.length > 0) {
      this.#hasPendingError = true;
      this.#pendingError = error;
    } else {
      this.sink.error(error);
    }
  }

  // --- Stream ---

  resume(): void {
    if (this.#disposed) return;

    // Drain queued lines first
    if (this.#queue.length > 0) {
      if (this.#drain() === PAUSE_SYM) return;
    }

    // Check deferred terminal events
    if (this.#pendingComplete) {
      this.sink.complete();
      return;
    }
    if (this.#hasPendingError) {
      this.sink.error(this.#pendingError);
      return;
    }

    // Resume upstream
    this.#upstream.resume();
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
    this.#queue.length = 0;
    this.#upstream[Symbol.dispose]();
  }

  // --- Private ---

  #drain(): undefined | PAUSE {
    while (this.#queue.length > 0) {
      const line = this.#queue.shift()!;
      const result = this.sink.next(line);
      if (result === PAUSE_SYM) return PAUSE_SYM;
    }
    return undefined;
  }
}

class LinesSource extends AbstractSource<string> {
  constructor(private readonly source: Source<string | Uint8Array>) {
    super();
  }

  connect(sink: Sink<string>): Stream {
    const linesStream = new LinesStream(sink);
    const upstream = this.source.connect(linesStream);
    linesStream._setUpstream(upstream);
    return linesStream;
  }
}

/**
 * Split chunks into lines, emitting one string per line.
 *
 * Accepts `string` or `Uint8Array` chunks. Binary chunks are decoded
 * using `TextDecoder` with streaming mode (handles multi-byte characters
 * split across chunks correctly).
 *
 * Handles both `\n` and `\r\n` line endings. Trailing data without a
 * final newline is emitted when the source completes.
 *
 * Supports backpressure: if downstream returns `PAUSE` during emission
 * of multiple lines from a single chunk, remaining lines are queued
 * and drained on `resume()`.
 *
 * @example
 * ```ts
 * // Read lines from a file
 * const fileLines = stream(
 *   fromReadable(fs.createReadStream('data.csv')),
 *   lines(),
 *   toArray(),
 * );
 *
 * // Pipeline with processing
 * stream(
 *   fromReadableStream(response.body!),
 *   lines(),
 *   filter(line => line.length > 0),
 *   map(line => JSON.parse(line)),
 *   toArray(),
 * );
 * ```
 */
export function lines(): Operator<string | Uint8Array, string> {
  return (source) => new LinesSource(source);
}
