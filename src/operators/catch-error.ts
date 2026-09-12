// ---------------------------------------------------------------------------
// catchError — on upstream error, switch to a fallback source
//
// The fallback is connected when the upstream error arrives, but it is
// only resumed when the downstream is unpaused: an error may arrive while
// the downstream is paused (terminal events are not governed by PAUSE),
// and the fallback must not deliver into a paused sink.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { OperatorStream, OperatorSource } from '../internal/operator-stream.js';

class CatchErrorStream<T> extends OperatorStream<T, T> {
  #downstreamPaused = true; // streams start paused
  #fallback: Stream | undefined;

  constructor(
    sink: Sink<T>,
    private readonly handler: (error: unknown) => Source<T>,
  ) {
    super(sink);
  }

  protected onValue(value: T): undefined | PAUSE {
    return this.#forward(value);
  }

  protected onError(error: unknown): void {
    let fallbackSource: Source<T>;
    try {
      fallbackSource = this.handler(error);
    } catch (handlerError) {
      this.emitError(handlerError);
      return;
    }

    this.#fallback = fallbackSource.connect({
      next: (value: T) => this.#forward(value),
      complete: () => this.emitComplete(),
      error: (err: unknown) => this.emitError(err),
    });

    // Resume the fallback now only if the downstream can take values;
    // otherwise the next downstream resume() starts it.
    if (!this.#downstreamPaused) this.#fallback.resume();
  }

  protected onResume(): void {
    this.#downstreamPaused = false;
    if (this.#fallback) {
      this.#fallback.resume();
    } else {
      this.upstream.resume();
    }
  }

  protected onDispose(): void {
    this.#fallback?.[Symbol.dispose]();
  }

  #forward(value: T): undefined | PAUSE {
    const result = this.emit(value);
    if (result === PAUSE) this.#downstreamPaused = true;
    return result;
  }
}

/**
 * On upstream error, switch to a fallback source produced by `handler`.
 *
 * The fallback is connected as soon as the error arrives and is started
 * once the downstream is ready for values (immediately if it is not
 * paused, otherwise on its next `resume()`). Later `resume()` calls are
 * forwarded to the fallback.
 *
 * If `handler` throws, the error is sent downstream.
 *
 * @example
 * ```ts
 * pipe(
 *   riskySource,
 *   catchError((err) => fromArray([fallbackValue])),
 * )
 * ```
 */
export function catchError<T>(handler: (error: unknown) => Source<T>): Operator<T, T> {
  return (source) => new OperatorSource(source, (sink) => new CatchErrorStream(sink, handler));
}
