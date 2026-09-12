// ---------------------------------------------------------------------------
// toAsync — produce a lazy async thunk from a stream pipeline
//
// Returns an Operator<T, () => Promise<T>>. The thunk, when called,
// connects to the source, registers with the owner ambient *at that
// moment*, and returns a promise for the first emitted value.
//
// That late binding is the point: the pipeline can be built in one place
// and run under whatever owner (request scope, task, page) calls it.
// ---------------------------------------------------------------------------

import type { Sink, Stream, Operator } from '../types.js';
import type { OwnedOptions } from '../owner.js';
import { stream } from '../stream.js';
import { toPromise } from './to-promise.js';
import { SingleValueStream } from '../internal/single-value-stream.js';

/**
 * Produce a lazy async thunk from a stream pipeline.
 *
 * The thunk connects to the upstream source and returns a `Promise<T>`
 * that resolves with the first emitted value (see `toPromise`), or rejects
 * on error. Owner registration happens when the thunk is *called*, so it
 * picks up whatever owner is ambient at that point (or `opts.owner`).
 * Each call of the thunk opens a fresh connection.
 *
 * @example
 * ```ts
 * const load = stream(source, map(f), toAsync());
 *
 * // later, under some owner
 * const value = await withOwner(scope, load);
 * ```
 */
export function toAsync<T>(opts?: OwnedOptions): Operator<T, () => Promise<T>> {
  return (source) => ({
    connect(sink: Sink<() => Promise<T>>): Stream {
      return new SingleValueStream(sink, () => (): Promise<T> => stream(source, toPromise<T>(opts)));
    },
  });
}
