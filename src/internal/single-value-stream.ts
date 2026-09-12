// ---------------------------------------------------------------------------
// SingleValueStream — emit one value and complete, exactly once
//
// The edge operators (toPromise, toCallback, toAsyncIterable, intoChannel,
// toReadableStream, intoWritableStream, toAsync, toSource, and the Node
// bridges) all hand the downstream a single handle and complete. The handle
// is produced on the first resume() — that is when the upstream is
// connected — so a second resume() must not produce a second handle or a
// second complete(), and a sink that disposes the stream from inside next()
// must not receive complete() afterwards.
// ---------------------------------------------------------------------------

import type { Sink, Stream } from '../types.js';

export class SingleValueStream<T> implements Stream {
  #started = false;
  #disposed = false;

  /**
   * @param sink      downstream
   * @param produce   builds the one value; runs on the first resume()
   * @param onDispose optional teardown for whatever `produce` started
   */
  constructor(
    private readonly sink: Sink<T>,
    private readonly produce: () => T,
    private readonly onDispose?: () => void,
  ) {}

  resume(): void {
    if (this.#started || this.#disposed) return;
    this.#started = true;
    const value = this.produce();
    this.sink.next(value);
    // A terminal event may follow a PAUSE (protocol rule 3), but not a
    // dispose (rule 4).
    if (!this.#disposed) this.sink.complete();
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.onDispose?.();
  }
}
