// ---------------------------------------------------------------------------
// lines — split chunks into lines
//
// Accepts string or Uint8Array chunks (binary decoded via TextDecoder
// with streaming mode for correct multi-byte handling). Emits one
// string per line. Handles \n and \r\n. Trailing data without a
// newline is emitted on complete.
//
// One-to-many: a single chunk may produce multiple lines. Lines the
// downstream cannot take yet are queued in a PauseBuffer and drained on
// resume; a terminal event is delivered once the queue is empty.
// ---------------------------------------------------------------------------

import type { Sink, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { OperatorStream, OperatorSource } from '../internal/operator-stream.js';
import { PauseBuffer } from '../internal/pause-buffer.js';

function stripCarriageReturn(line: string): string {
  return line.endsWith('\r') ? line.slice(0, -1) : line;
}

class LinesStream extends OperatorStream<string | Uint8Array, string> {
  readonly #buffer: PauseBuffer<string>;
  #decoder = new TextDecoder();
  #partial = '';

  constructor(sink: Sink<string>) {
    super(sink);
    this.#buffer = new PauseBuffer<string>({
      next: (line: string) => this.emit(line),
      complete: () => this.emitComplete(),
      error: (error: unknown) => this.emitError(error),
    });
  }

  protected onValue(chunk: string | Uint8Array): undefined | PAUSE {
    const text = typeof chunk === 'string' ? chunk : this.#decoder.decode(chunk, { stream: true });

    this.#partial += text;
    const parts = this.#partial.split('\n');
    this.#partial = parts.pop()!; // last element is the incomplete tail

    for (const part of parts) {
      this.#buffer.push(stripCarriageReturn(part));
    }

    // Ask the upstream to wait whenever the downstream is paused, whether
    // or not this chunk produced a line; resume() drains and then resumes.
    return this.#buffer.paused ? PAUSE : undefined;
  }

  protected onComplete(): void {
    // Flush the TextDecoder
    this.#partial += this.#decoder.decode();

    // Emit trailing data as a final line
    if (this.#partial) {
      const last = stripCarriageReturn(this.#partial);
      this.#partial = '';
      if (last) this.#buffer.push(last);
    }

    // Delivered now if nothing is queued, otherwise after the drain.
    this.#buffer.complete();
  }

  protected onError(error: unknown): void {
    this.#buffer.error(error);
  }

  protected onResume(): void {
    if (this.#buffer.resume()) this.upstream.resume();
  }

  protected onDispose(): void {
    this.#buffer.dispose();
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
 * and drained on `resume()`. Completion (or an error) is delivered only
 * after every queued line has been delivered.
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
  return (source) => new OperatorSource(source, (sink) => new LinesStream(sink));
}
