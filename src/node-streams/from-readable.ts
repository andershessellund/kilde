// ---------------------------------------------------------------------------
// fromReadable — Node Readable → kilde Source
//
// Bridges Node.js readable streams into the kilde push-based protocol.
// Backpressure maps 1:1:
//   resume()             → readable.resume() (flowing mode)
//   Sink → PAUSE         → readable.pause()
//   'end' event          → sink.complete()
//   'error' event        → sink.error(err)
//   'close' before 'end' → sink.error(PrematureCloseError)
//   [Symbol.dispose]     → readable.destroy()
//
// Zero buffering in the adapter — Node's internal highWaterMark buffer
// is the only buffer.
//
// Nothing is attached to the readable before the first resume(). At that
// point a readable that has already ended or been destroyed is reported
// right away. After a terminal event or dispose every listener is removed,
// so a late 'error' after 'end' never reaches the sink; a no-op 'error'
// listener is left behind so such an event cannot crash the process.
// ---------------------------------------------------------------------------

import type { Readable } from 'node:stream';
import type { Source, Sink, Stream } from '../types.js';
import { PAUSE as PAUSE_SYM } from '../types.js';
import { AbstractSource } from '../abstract-source.js';
import { PrematureCloseError } from './premature-close.js';

const noop = () => {};

class FromReadableConnection<T> implements Stream {
  #started = false;
  #terminated = false;
  #disposed = false;

  readonly #onData: (chunk: T) => void;
  readonly #onEnd: () => void;
  readonly #onError: (err: Error) => void;
  readonly #onClose: () => void;

  constructor(
    private readonly readable: Readable,
    private readonly sink: Sink<T>,
  ) {
    this.#onData = (chunk: T) => {
      if (!this.#active) return;
      const result = this.sink.next(chunk);
      if (result === PAUSE_SYM) {
        this.readable.pause();
      }
    };
    this.#onEnd = () => this.#complete();
    this.#onError = (err: Error) => this.#fail(err);
    this.#onClose = () => this.#fail(new PrematureCloseError());
  }

  get #active(): boolean {
    return !this.#terminated && !this.#disposed;
  }

  resume(): void {
    if (!this.#active) return;

    if (!this.#started) {
      this.#started = true;
      // The readable may already be finished before anyone connected.
      if (this.readable.readableEnded) {
        this.#complete();
        return;
      }
      if (this.readable.destroyed) {
        this.#fail(this.readable.errored ?? new PrematureCloseError());
        return;
      }
      this.readable.on('data', this.#onData);
      this.readable.on('end', this.#onEnd);
      this.readable.on('error', this.#onError);
      this.readable.on('close', this.#onClose);
    }

    this.readable.resume();
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    if (this.#terminated) return; // listeners already gone
    this.#detach();
    if (!this.readable.destroyed) {
      this.readable.destroy();
    }
  }

  #complete(): void {
    if (!this.#active) return;
    this.#terminated = true;
    this.#detach();
    this.sink.complete();
  }

  #fail(err: unknown): void {
    if (!this.#active) return;
    this.#terminated = true;
    this.#detach();
    this.sink.error(err);
  }

  #detach(): void {
    if (!this.#started) return;
    this.readable.removeListener('data', this.#onData);
    this.readable.removeListener('end', this.#onEnd);
    this.readable.removeListener('error', this.#onError);
    this.readable.removeListener('close', this.#onClose);
    // Keep a late 'error' (e.g. after 'end', or from destroy()) from
    // becoming an uncaught exception now that ours is gone.
    this.readable.on('error', noop);
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
 * - `'close'` before `'end'` (e.g. `readable.destroy()`) →
 *   `sink.error(PrematureCloseError)` (`code: 'ERR_STREAM_PREMATURE_CLOSE'`)
 * - `[Symbol.dispose]` → `readable.destroy()`
 *
 * A readable that has already ended or been destroyed when the stream is
 * first resumed completes / errors immediately. After a terminal event the
 * adapter detaches from the readable; a late `'error'` is swallowed by a
 * no-op listener rather than delivered after `complete()`.
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
