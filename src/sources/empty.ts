// ---------------------------------------------------------------------------
// empty — source that immediately completes
// ---------------------------------------------------------------------------

import type { Sink, Stream, StreamableSource } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class EmptyStream implements Stream {
  #disposed = false;

  constructor(private readonly sink: Sink<unknown>) {}

  resume(): void {
    if (!this.#disposed) {
      this.sink.complete();
    }
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
  }
}

class EmptySource<T> extends AbstractSource<T> {
  connect(sink: Sink<T>): Stream {
    return new EmptyStream(sink);
  }
}

const EMPTY_SOURCE = new EmptySource<any>();

/**
 * Create a source that immediately completes on resume, emitting no values.
 *
 * @example
 * ```ts
 * stream(empty(), toArray()) // []
 * ```
 */
export function empty<T = never>(): StreamableSource<T> {
  return EMPTY_SOURCE;
}
