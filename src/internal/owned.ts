// ---------------------------------------------------------------------------
// registerThenConnect — owner registration for hot connections
//
// Hot constructs (connect(), toPromise(), toCallback(), toAsyncIterable(),
// intoChannel(), ...) register a teardown with their owner. The registration
// must exist before the upstream is connected: a source may deliver a
// terminal event synchronously during the first resume(), and the sink then
// unregisters — so the handle has to be there already.
// ---------------------------------------------------------------------------

import type { Owner, OwnerHandle } from '../owner.js';
import { currentOwner } from '../owner.js';

export interface OwnedRegistration {
  /** Remove the registration (no-op once removed). */
  unregister(): void;
}

/**
 * Register `teardown` with `owner` (else the ambient owner). Returns a handle
 * whose `unregister()` is safe to call any number of times.
 */
export function registerWithOwner(
  owner: Owner | undefined,
  name: string,
  teardown: () => void,
): OwnedRegistration {
  const handle: OwnerHandle | undefined = (owner ?? currentOwner()).register(
    { [Symbol.dispose]: teardown },
    name,
  );
  let done = false;
  return {
    unregister() {
      if (done) return;
      done = true;
      handle?.unregister();
    },
  };
}
