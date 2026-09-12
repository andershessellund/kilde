// ---------------------------------------------------------------------------
// link() + linkedSignal() — writable signals with reactive derivation
//
// link(signal, computation, opts?)
//   Attaches a reactive derivation to an existing writable signal.
//   The computation receives the current signal value (untracked snapshot)
//   and returns a new value. Any signal reads inside the computation body
//   are auto-tracked dependencies — when they change, the computation
//   reruns and the result is written back to the signal.
//
//   Two complementary mechanisms keep the signal in sync:
//   - Pull: a 'read' hook on the signal pulls the derived computation
//     fresh on every read, ensuring the value is always current.
//   - Push: when the signal is observed, a value observer on the derived
//     pushes changes through to the signal via set().
//
// linkedSignal(computation, opts?)
//   Creates a writable signal with a built-in reactive derivation.
//   Thin wrapper: createSignal(init) + link(signal, computation, { registerResource: false }).
//   Self-contained and GC-safe — no resource registration needed.
// ---------------------------------------------------------------------------

import type { WritableSignal } from './types.js';
import { createSignal } from './signal.js';
import type { CreateSignalOptions } from './signal.js';
import { computed, untracked } from './signal.js';
import { currentOwner } from './owner.js';
import type { OwnedOptions } from './owner.js';

/** Sentinel for "no previous pull result yet". */
const SENTINEL: unique symbol = Symbol('link.sentinel');

export interface LinkOptions extends OwnedOptions {
  /**
   * Register the link with an owner (`owner`, else the ambient owner).
   *
   * - `true` (default) — the link is torn down when the owner disposes.
   *   Use when linking a signal you don't own to a scoped derivation.
   * - `false` — the link's lifecycle is tied to the signal itself (GC-safe).
   *   Use when the signal and link are created together (e.g., linkedSignal).
   */
  registerResource?: boolean;
  /** @internal Signal already holds the correct initial value (set by linkedSignal). */
  _preSeeded?: boolean;
}

/**
 * Attach a reactive derivation to an existing writable signal.
 *
 * The `computation` receives the current signal value as a snapshot and
 * returns a new value. Signal reads inside the computation body are
 * auto-tracked — when they change, the computation reruns and the result
 * is written to the signal via `set()`.
 *
 * The link is demand-driven: the internal computed is only active when
 * the writable signal has observers. When unobserved, the computed goes
 * dormant — zero overhead.
 *
 * @returns An unlink function that detaches the derivation.
 *
 * @example
 * ```ts
 * const options = createSignal(['Ground', 'Air', 'Sea']);
 * const selected = createSignal('Ground');
 *
 * link(selected, prev => {
 *   const opts = options();           // tracked
 *   return opts.includes(prev)        // prev is untracked snapshot
 *     ? prev
 *     : opts[0];
 * });
 *
 * options.set(['Email', 'Postal']);
 * selected(); // 'Email' — prev ('Ground') wasn't in new list
 *
 * selected.set('Postal');
 * selected(); // 'Postal' — manual override sticks
 *
 * options.set(['Postal', 'Courier']);
 * selected(); // 'Postal' — prev ('Postal') is still valid
 * ```
 */
export function link<T>(
  signal: WritableSignal<T>,
  computation: (previous: T) => T,
  opts?: LinkOptions,
): () => void {
  const shouldRegister = opts?.registerResource !== false;
  let unsub: (() => void) | null = null;
  let disposed = false;

  // Internal computed: tracks dependencies in computation body,
  // reads signal value untracked (snapshot for `previous`).
  const derived = computed(() => {
    const prev = untracked(() => signal());
    return computation(prev);
  });

  // Read hook: pull the derived computation fresh on every signal read.
  // Only overrides the stored value when the derived actually recomputed
  // to a new result — this preserves manual set() overrides until tracked
  // dependencies change.
  //
  // The `pulling` guard prevents cycles: derived evaluates → reads signal
  // (via untracked) → coordinatorRead → read hook → derived() → cycle.
  // When re-entered, the hook returns the current stored value unchanged.
  let pulling = false;
  let lastPulled: T | typeof SENTINEL = SENTINEL;

  // Pre-seed: when the signal was initialized with the correct value
  // (e.g., by linkedSignal), force-evaluate derived to align lastPulled
  // so the read hook won't override the initial value on first read.
  if (opts?._preSeeded) {
    pulling = true;
    try { lastPulled = derived(); } finally { pulling = false; }
  }

  const offRead = signal.observe('read', (current) => {
    if (pulling) return current;
    pulling = true;
    try {
      const val = derived();
      if (val !== lastPulled) {
        lastPulled = val;
        return val;
      }
      return current;
    } finally { pulling = false; }
  });

  function activate(): void {
    if (disposed || unsub) return;
    unsub = derived.observe('value', (v) => signal.set(v));
  }

  function deactivate(): void {
    unsub?.();
    unsub = null;
  }

  // Wire up demand-driven activation on the target signal
  const offActivate = signal.observe('activate', activate);
  const offDeactivate = signal.observe('deactivate', deactivate);

  // If signal is already observed, activate immediately
  if (signal.observed) activate();

  function unlink(): void {
    if (disposed) return;
    disposed = true;
    deactivate();
    offRead();
    offActivate();
    offDeactivate();
  }

  // Register with the owner for automatic cleanup
  if (shouldRegister) {
    (opts?.owner ?? currentOwner()).register({ [Symbol.dispose]: unlink }, 'link');
  }

  return unlink;
}

/**
 * Create a writable signal with a built-in reactive derivation.
 *
 * The `computation` receives the previous value (`undefined` on first call)
 * and returns a new value. Signal reads inside the computation are
 * auto-tracked — when they change, the computation reruns.
 *
 * The signal is fully writable: `set()` and `update()` override the
 * computed value. On the next source change, the computation sees the
 * user-written value as `previous`.
 *
 * Self-contained and GC-safe — no resource registration. The link's
 * lifecycle is tied to the signal itself.
 *
 * @example
 * ```ts
 * const options = createSignal(['Ground', 'Air', 'Sea']);
 *
 * const selected = linkedSignal(prev => {
 *   const opts = options();
 *   return prev !== undefined && opts.includes(prev) ? prev : opts[0];
 * });
 *
 * selected();             // 'Ground'
 * selected.set('Sea');
 * selected();             // 'Sea'
 *
 * options.set(['Email', 'Sea', 'Postal']);
 * selected();             // 'Sea' — preserved (still in list)
 *
 * options.set(['Email', 'Postal']);
 * selected();             // 'Email' — reset (Sea gone)
 * ```
 */
export function linkedSignal<T>(
  computation: (previous: T | undefined) => T,
  opts?: CreateSignalOptions<T>,
): WritableSignal<T> {
  const init = computation(undefined);
  const sig = createSignal(init, opts);
  link(sig, computation as (prev: T) => T, { registerResource: false, _preSeeded: true });
  return sig;
}
