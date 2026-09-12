// ---------------------------------------------------------------------------
// take — emit the first N values, then complete
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';
import { completeOnResume } from '../internal/complete-on-resume.js';
import { OperatorStream, connectOperator } from '../internal/operator-stream.js';

class TakeStream<T> extends OperatorStream<T, T> {
  #remaining: number;

  constructor(sink: Sink<T>, count: number) {
    super(sink);
    this.#remaining = count;
  }

  protected onValue(value: T): undefined | PAUSE {
    this.#remaining--;
    const result = this.emit(value);
    if (this.#remaining <= 0) {
      // Satisfied: release the upstream and complete exactly once.
      this.finish();
      return PAUSE;
    }
    return result;
  }
}

/** `take(0)`: never connects upstream, completes once on the first resume. */
class TakeSource<T> extends AbstractSource<T> {
  constructor(
    private readonly source: Source<T>,
    private readonly count: number,
  ) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    if (this.count <= 0) return completeOnResume(sink);
    return connectOperator(this.source, new TakeStream(sink, this.count));
  }
}

/**
 * Emit the first `n` values from the source, then dispose upstream
 * and complete.
 *
 * `take(0)` never connects to the upstream at all: it completes on the
 * first `resume()` and does nothing on later ones.
 *
 * @example
 * ```ts
 * stream(fromArray([1, 2, 3, 4, 5]), take(3), toArray()) // [1, 2, 3]
 * ```
 */
export function take<T>(n: number): Operator<T, T> {
  return (source) => new TakeSource(source, n);
}
