// ---------------------------------------------------------------------------
// assertProtocol — an operator that checks the stream protocol
//
// Insert it directly after the source or operator under test. It watches
// the traffic between that source and the downstream sink and throws a
// ProtocolViolationError on:
//
//   - any call before the first resume()
//   - next() while the downstream is paused
//   - next(), complete() or error() after a terminal event
//   - any call after dispose
// ---------------------------------------------------------------------------

import type { Operator, Sink, Source, Stream } from '../types.js';
import { PAUSE } from '../types.js';

/** Thrown by {@link assertProtocol} and `testSink` when the stream protocol is broken. */
export class ProtocolViolationError extends Error {
  constructor(message: string) {
    super(`Stream protocol violation: ${message}`);
    this.name = 'ProtocolViolationError';
  }
}

class CheckedSource<T> implements Source<T> {
  constructor(private readonly source: Source<T>) {}

  connect(sink: Sink<T>): Stream {
    let started = false;
    let paused = false;
    let terminated = false;
    let disposed = false;

    const check = (what: string) => {
      if (disposed) throw new ProtocolViolationError(`${what} after dispose`);
      if (!started) throw new ProtocolViolationError(`${what} before the first resume()`);
      if (terminated) throw new ProtocolViolationError(`${what} after a terminal event`);
    };

    const upstream = this.source.connect({
      next(value: T) {
        check('next()');
        if (paused) throw new ProtocolViolationError('next() while paused');
        const result = sink.next(value);
        if (result === PAUSE) paused = true;
        return result;
      },
      complete() {
        check('complete()');
        terminated = true;
        sink.complete();
      },
      error(error: unknown) {
        check('error()');
        terminated = true;
        sink.error(error);
      },
    });

    return {
      resume() {
        if (disposed || terminated) return;
        started = true;
        paused = false;
        upstream.resume();
      },
      [Symbol.dispose]() {
        disposed = true;
        upstream[Symbol.dispose]();
      },
    };
  }
}

/**
 * Operator that asserts the stream protocol between the source it is
 * applied to and its downstream sink. Use it in tests:
 *
 * ```ts
 * const s = pipe(testSource([1, 2, 3], { oracle }), myOperator(), assertProtocol()).connect(sink);
 * ```
 */
export function assertProtocol<T>(): Operator<T, T> {
  return (source) => new CheckedSource(source);
}
