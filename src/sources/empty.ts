// ---------------------------------------------------------------------------
// empty — source that immediately completes
// ---------------------------------------------------------------------------

import type { Sink, Stream, StreamableSource } from '../types.js';
import { AbstractSource } from '../abstract-source.js';
import { completeOnResume } from '../internal/complete-on-resume.js';

class EmptySource<T> extends AbstractSource<T> {
  connect(sink: Sink<T>): Stream {
    return completeOnResume(sink);
  }
}

const EMPTY_SOURCE = new EmptySource<any>();

/**
 * Create a source that completes on its first `resume()`, emitting no
 * values. Later `resume()` calls do nothing.
 *
 * @example
 * ```ts
 * stream(empty(), toArray()) // []
 * ```
 */
export function empty<T = never>(): StreamableSource<T> {
  return EMPTY_SOURCE;
}
