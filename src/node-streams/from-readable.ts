// ---------------------------------------------------------------------------
// fromReadable — Node Readable → kilde Source
//
// Bridges Node.js readable streams into the kilde push-based protocol.
// Backpressure maps 1:1:
//   resume()        → readable.resume() (flowing mode)
//   Sink → PAUSE    → readable.pause()
//   'end' event     → sink.complete()
//   'error' event   → sink.error()
//   [Symbol.dispose]       → readable.destroy()
//
// Zero buffering in the adapter — Node's internal highWaterMark buffer
// is the only buffer.
// ---------------------------------------------------------------------------

import type { Readable } from 'node:stream';
import type { Source, Sink, Stream } from '../types.js';
import { PAUSE as PAUSE_SYM } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class FromReadableConnection<T> implements Stream {
  #disposed = false;

  readonly #onData: (chunk: T) => void;
  readonly #onEnd: () => void;
  readonly #onError: (err: Error) => void;

  constructor(
    private readonly readable: Readable,
    private readonly sink: Sink<T>,
  ) {
    this.#onData = (chunk: T) => {
      if (this.#disposed) return;
      const result = this.sink.next(chunk);
      if (result === PAUSE_SYM) {
        this.readable.pause();
      }
    };

    this.#onEnd = () => {
      if (!this.#disposed) this.sink.complete();
    };

    this.#onError = (err: Error) => {
      if (!this.#disposed) this.sink.error(err);
    };

    readable.on('data', this.#onData);
    readable.on('end', this.#onEnd);
    readable.on('error', this.#onError);
    readable.pause(); // Start paused — kilde protocol
  }

  resume(): void {
    if (!this.#disposed) {
      this.readable.resume();
    }
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.readable.removeListener('data', this.#onData);
    this.readable.removeListener('end', this.#onEnd);
    this.readable.removeListener('error', this.#onError);
    if (!this.readable.destroyed) {
      this.readable.destroy();
    }
  }
}

class FromReadableSource<T> extends AbstractSource<T> {
  constructor(private readonly readable: Readable) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    return new FromReadableConnection<T>(this.readable, sink);
  }
}

/**
 * Create a `Source<T>` from a Node.js `Readable` stream.
 *
 * The adapter adds zero buffering — Node's internal `highWaterMark`
 * buffer is the only buffer. Backpressure maps 1:1:
 *
 * - `resume()` → `readable.resume()` (flowing mode, `'data'` events fire)
 * - Sink returns `PAUSE` → `readable.pause()` (stops `'data'` events)
 * - `'end'` event → `sink.complete()`
 * - `'error'` event → `sink.error(err)`
 * - `[Symbol.dispose]` → `readable.destroy()`
 *
 * @example
 * ```ts
 * import { createReadStream } from 'node:fs';
 *
 * const src = fromReadable<Buffer>(createReadStream('data.csv'));
 * const allLines = stream(src, lines(), toArray());
 * ```
 */
export function fromReadable<T = Buffer>(readable: Readable): Source<T> {
  return new FromReadableSource<T>(readable);
}
