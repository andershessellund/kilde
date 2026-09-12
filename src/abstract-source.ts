// ---------------------------------------------------------------------------
// AbstractSource<T> — base class for all source implementations
//
// Provides `.stream()` for ergonomic left-to-right composition.
// Subclasses only need to implement `connect(sink)`.
//
// The `.stream()` method is the consumer-facing API. Raw `.connect()`
// is internal — used by operators and the `stream()` entry point.
// ---------------------------------------------------------------------------

import type { Sink, Stream, StreamableSource, Operator } from './types.js';
import { stream } from './stream.js';

/**
 * Base class for source implementations. Provides `.stream()` for
 * left-to-right operator composition.
 *
 * Subclasses implement `connect(sink)` — the internal protocol.
 * Consumer code uses `.stream(op1, op2, ...)` instead of calling
 * `connect()` directly.
 *
 * @example
 * ```ts
 * class MySource extends AbstractSource<number> {
 *   connect(sink: Sink<number>): Stream { ... }
 * }
 *
 * const src = new MySource();
 * const result = src.stream(map(x => x * 2), reduce((a, b) => a + b, 0));
 * ```
 */
export abstract class AbstractSource<T> implements StreamableSource<T> {
  abstract connect(sink: Sink<T>): Stream;

  stream(...operators: Operator<any, any>[]): any {
    return stream(this, ...operators);
  }
}
