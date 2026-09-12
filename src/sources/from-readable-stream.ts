// ---------------------------------------------------------------------------
// fromReadableStream — Web ReadableStream → kilde Source
//
// Bridges the Web Streams API into the kilde push-based protocol.
// PAUSE ↔ stop pulling from the reader. resume() ↔ restart the pull loop.
//
// Zero buffering in the adapter — the ReadableStream's internal queue
// is the only buffer.
//
// A ReadableStream can have only one reader, so the source is single-use:
// a second connect() (while the first connection holds the lock) yields a
// stream that delivers a clear error on resume().
// ---------------------------------------------------------------------------

import type { Sink, Stream, StreamableSource } from '../types.js';
import { PAUSE as PAUSE_SYM } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class FromReadableStreamConnection<T> implements Stream {
  #reader: ReadableStreamDefaultReader<T>;
  #sink: Sink<T>;
  #disposed = false;
  #terminated = false;
  #pulling = false;

  constructor(reader: ReadableStreamDefaultReader<T>, sink: Sink<T>) {
    this.#reader = reader;
    this.#sink = sink;
  }

  get #active(): boolean {
    return !this.#disposed && !this.#terminated;
  }

  resume(): void {
    if (!this.#active || this.#pulling) return;
    this.#pulling = true;
    // Exceptions thrown by the sink itself are not caught here — they
    // surface as a rejection of the pull loop, exactly as a sink throwing
    // into any other producer would.
    void this.#pull();
  }

  async #pull(): Promise<void> {
    while (this.#active) {
      let result: ReadableStreamReadResult<T>;
      try {
        result = await this.#reader.read();
      } catch (err) {
        if (this.#active) {
          this.#terminated = true;
          this.#sink.error(err);
        }
        return;
      }
      if (!this.#active) return;
      if (result.done) {
        this.#terminated = true;
        this.#sink.complete();
        return;
      }
      const signal = this.#sink.next(result.value);
      if (signal === PAUSE_SYM) {
        this.#pulling = false;
        return;
      }
    }
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    if (!this.#terminated) {
      this.#reader.cancel().catch(() => {});
    }
  }
}

/** Connection handed out when the stream's lock could not be acquired. */
class LockedConnection implements Stream {
  #done = false;

  constructor(
    private readonly sink: Sink<never>,
    private readonly error: unknown,
  ) {}

  resume(): void {
    if (this.#done) return;
    this.#done = true;
    this.sink.error(this.error);
  }

  [Symbol.dispose](): void {
    this.#done = true;
  }
}

class FromReadableStreamSource<T> extends AbstractSource<T> {
  constructor(private readonly readable: ReadableStream<T>) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    let reader: ReadableStreamDefaultReader<T>;
    try {
      reader = this.readable.getReader();
    } catch (cause) {
      const err = new TypeError(
        'fromReadableStream(): the ReadableStream is locked — it can only be connected once',
        { cause },
      );
      return new LockedConnection(sink, err);
    }
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
 * **Single-use.** A `ReadableStream` can be locked by only one reader, so
 * the source can be connected once. A second `connect()` while the first
 * connection is alive returns a stream that delivers a `TypeError` to
 * `sink.error()` on `resume()`.
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
