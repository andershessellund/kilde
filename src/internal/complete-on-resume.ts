// ---------------------------------------------------------------------------
// completeOnResume — a Stream that completes on its first resume()
//
// Used wherever a connection has nothing to deliver: empty(), take(0), a
// combineLatest over no sources, and a store that is already disposed.
// ---------------------------------------------------------------------------

import type { Sink, Stream } from '../types.js';

export function completeOnResume(sink: Sink<never>): Stream {
  let done = false;
  return {
    resume() {
      if (done) return;
      done = true;
      sink.complete();
    },
    [Symbol.dispose]() {
      done = true;
    },
  };
}
