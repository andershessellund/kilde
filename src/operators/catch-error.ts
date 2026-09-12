// ---------------------------------------------------------------------------
// catchError — on upstream error, switch to a fallback source
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class CatchErrorStream<T> implements Stream, Sink<T> {
  #disposed = false;
  #upstream!: Stream;
  #fallbackStream: Stream | undefined;

  constructor(
    private readonly sink: Sink<T>,
    private readonly handler: (error: unknown) => Source<T>,
  ) {}

  _setUpstream(upstream: Stream): void {
    this.#upstream = upstream;
  }

  // --- Sink<T> ---

  next(value: T): undefined | typeof PAUSE {
    if (this.#disposed) return PAUSE;
    return this.sink.next(value);
  }

  complete(): void {
    if (!this.#disposed) {
      this.sink.complete();
    }
  }

  error(error: unknown): void {
    if (this.#disposed) return;

    try {
      const fallbackSource = this.handler(error);
      // Connect to fallback source
      this.#fallbackStream = fallbackSource.connect(this.sink);
      this.#fallbackStream.resume();
    } catch (handlerError) {
      this.sink.error(handlerError);
    }
  }

  // --- Stream ---

  resume(): void {
    if (this.#fallbackStream) {
      this.#fallbackStream.resume();
    } else {
      this.#upstream.resume();
    }
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
    this.#upstream[Symbol.dispose]();
    this.#fallbackStream?.[Symbol.dispose]();
  }
}

class CatchErrorSource<T> extends AbstractSource<T> {
  constructor(
    private readonly source: Source<T>,
    private readonly handler: (error: unknown) => Source<T>,
  ) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    const catchStream = new CatchErrorStream(sink, this.handler);
    const upstream = this.source.connect(catchStream);
    catchStream._setUpstream(upstream);
    return catchStream;
  }
}

/**
 * On upstream error, switch to a fallback source produced by `handler`.
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
  return (source) => new CatchErrorSource(source, handler);
}
