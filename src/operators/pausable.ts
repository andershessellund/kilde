// ---------------------------------------------------------------------------
// pausable — insert a buffer between source and sink for backpressure
//
// Accumulates values when downstream is paused. Drains buffer on resume.
// A terminal event is held until the buffer has drained.
// ---------------------------------------------------------------------------

import type { Sink, Operator, PAUSE } from '../types.js';
import { OperatorStream, OperatorSource } from '../internal/operator-stream.js';
import { PauseBuffer } from '../internal/pause-buffer.js';

class PausableStream<T> extends OperatorStream<T, T> {
  readonly #buffer: PauseBuffer<T>;

  constructor(sink: Sink<T>) {
    super(sink);
    // Route the buffer's output through the base helpers so terminal
    // bookkeeping stays in one place.
    this.#buffer = new PauseBuffer<T>({
      next: (value: T) => this.emit(value),
      complete: () => this.emitComplete(),
      error: (error: unknown) => this.emitError(error),
    });
  }

  protected onValue(value: T): undefined | PAUSE {
    return this.#buffer.push(value);
  }

  protected onComplete(): void {
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
 * Insert a buffer between source and sink for backpressure management.
 *
 * Accumulates values in an internal buffer when the downstream sink returns
 * PAUSE. On `resume()`, drains the buffer before resuming upstream. A
 * `complete()` or `error()` that arrives while values are buffered is
 * delivered only after the buffer has drained.
 *
 * The same buffering logic backs the per-subscriber buffers of
 * `createRelay()` and the queues inside `lines()` and `scheduleOn()`.
 */
export function pausable<T>(): Operator<T, T> {
  return (source) => new OperatorSource(source, (sink) => new PausableStream<T>(sink));
}
