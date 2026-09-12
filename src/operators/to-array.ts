// ---------------------------------------------------------------------------
// toArray — collect all values into an array, emit on complete
// ---------------------------------------------------------------------------

import type { Sink, Operator, PAUSE } from '../types.js';
import { OperatorStream, OperatorSource } from '../internal/operator-stream.js';

class ToArrayStream<T> extends OperatorStream<T, T[]> {
  #buffer: T[] = [];

  constructor(sink: Sink<T[]>) {
    super(sink);
  }

  protected onValue(value: T): undefined | PAUSE {
    this.#buffer.push(value);
    return undefined; // Never pause — collect everything
  }

  protected onComplete(): void {
    // The array is handed over as-is; the downstream may dispose us while
    // still holding it, so it is never cleared here.
    this.emit(this.#buffer);
    this.emitComplete();
  }
}

/**
 * Collect all upstream values into an array, emit the array on complete.
 *
 * As a collector with `stream()`:
 * ```ts
 * stream(fromArray([1, 2, 3]), toArray()) // [1, 2, 3]
 * ```
 *
 * Mid-pipeline:
 * ```ts
 * pipe(source, toArray()) // Source<T[]>
 * ```
 */
export function toArray<T>(): Operator<T, T[]> {
  return (source) => new OperatorSource(source, (sink) => new ToArrayStream<T>(sink));
}
