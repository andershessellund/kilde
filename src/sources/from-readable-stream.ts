// ---------------------------------------------------------------------------
// fromReadableStream — Web ReadableStream → kilde Source
//
// Bridges the Web Streams API into the kilde push-based protocol.
// PAUSE ↔ stop pulling from the reader. resume() ↔ restart the pull loop.
//
// Zero buffering in the adapter — the ReadableStream's internal queue
// is the only buffer.
// ---------------------------------------------------------------------------

import type { Sink, Stream, StreamableSource } from '../types.js';
import { PAUSE as PAUSE_SYM } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class FromReadableStreamConnection<T> implements Stream {
  #reader: ReadableStreamDefaultReader<T>;
  #sink: Sink<T>;
  #disposed = false;
  #pulling = false;

  constructor(reader: ReadableStreamDefaultReader<T>, sink: Sink<T>) {
    this.#reader = reader;
    this.#sink = sink;
  }

  resume(): void {
    if (this.#disposed || this.#pulling) return;
    this.#pulling = true;
    this.#pull();
  }

  async #pull(): Promise<void> {
    try {
      while (!this.#disposed) {
        const { value, done } = await this.#reader.read();
        if (this.#disposed) return;
        if (done) {
          this.#sink.complete();
          return;
        }
        const result = this.#sink.next(value!);
        if (result === PAUSE_SYM) {
          this.#pulling = false;
          return;
        }
      }
    } catch (err) {
      if (!this.#disposed) {
        this.#sink.error(err);
      }
    }
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
    this.#reader.cancel().catch(() => {});
  }
}

class FromReadableStreamSource<T> extends AbstractSource<T> {
  constructor(private readonly readable: ReadableStream<T>) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    const reader = this.readable.getReader();
    return new FromReadableStreamConnection(reader, sink);
  }
}

/**
 * Create a `Source<T>` from a Web `ReadableStream<T>`.
 *
 * The adapter adds zero buffering — the ReadableStream's internal queue
 * is the only buffer. Backpressure maps 1:1:
 *
 * - `resume()` → start/continue the async pull loop
 * - Sink returns `PAUSE` → stop pulling
 * - Stream done → `sink.complete()`
 * - Stream error → `sink.error(err)`
 * - `[Symbol.dispose]` → `reader.cancel()`
 *
 * @example
 * ```ts
 * const response = await fetch('/api/data');
 * const src = fromReadableStream(response.body!);
 * const data = stream(src, lines(), toArray());
 * ```
 */
export function fromReadableStream<T>(readable: ReadableStream<T>): StreamableSource<T> {
  return new FromReadableStreamSource(readable);
}
