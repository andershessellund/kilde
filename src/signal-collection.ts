// ---------------------------------------------------------------------------
// SignalDeduplicator — keyed cache of signals with structural key equality
//
// Caches signals by structural key (deepHash + deepEqual via HashMap).
// Auto-evicts entries when their signal becomes unobserved via observe('deactivate').
//
// Designed for use inside computed() — calling a cached signal registers
// the dependency automatically. The factory must produce a signal that has
// its initial value synchronously (e.g. createSignal, computed).
// ---------------------------------------------------------------------------

import { HashMap } from 'valsem';
import type { Signal } from './types.js';

/** Options for {@link SignalDeduplicator}. */
export interface SignalDeduplicatorOptions<K, V> {
  /**
   * Called when a signal is evicted from the cache (automatically
   * via observe('deactivate'), or manually via delete/clear).
   */
  onEvict?: (key: K, signal: Signal<V>) => void;
}

/**
 * A keyed cache of signals with structural key equality and auto-eviction.
 *
 * `getOrCreate(key, factory)` returns a cached signal or creates one via the
 * factory. Signals are automatically evicted when they become unobserved
 * (their `'deactivate'` event).
 *
 * Eviction therefore needs observation: a cached signal that is only ever
 * read by an *unobserved* computed never activates, so it never deactivates
 * either. Use the cache from computeds that are observed (a live query, a
 * rendered view), or call `delete()` / `clear()` yourself.
 *
 * Designed for use inside `computed()`:
 *
 * ```ts
 * const cache = new SignalDeduplicator<QueryKey, Row[]>();
 *
 * const view = computed(() => {
 *   const entries = cache.getOrCreate(
 *     { table: 'entries', limit: 20 },
 *     (key) => createSignal(db.query(key)),
 *   );
 *   return entries().map(e => {
 *     const lines = cache.getOrCreate(
 *       { table: 'lines', entryId: e.id },
 *       (key) => createSignal(db.query(key)),
 *     );
 *     return { ...e, lines: lines() };
 *   });
 * });
 *
 * // After a transaction:
 * // 1. Update affected signals (e.g. signal.set(newRows))
 * // 2. Computed re-evaluates, calling cache.getOrCreate() for what it needs
 * // 3. Signals no longer read by any computed are auto-evicted
 * ```
 */
export class SignalDeduplicator<K, V> {
  readonly #onEvict: ((key: K, signal: Signal<V>) => void) | undefined;
  readonly #cache: HashMap<K, Signal<V>>;

  constructor(options?: SignalDeduplicatorOptions<K, V>) {
    this.#onEvict = options?.onEvict;
    this.#cache = new HashMap<K, Signal<V>>();
  }

  #remove(key: K): void {
    const signal = this.#cache.get(key);
    if (!signal) return;
    this.#cache.delete(key);
    this.#onEvict?.(key, signal);
  }

  /**
   * Get or create a signal for the given key.
   *
   * If a signal for a structurally equal key is already cached, returns it.
   * Otherwise, calls the factory to create a new one, caches it, and
   * registers `observe('deactivate')` for automatic eviction.
   */
  getOrCreate(key: K, factory: (key: K) => Signal<V>): Signal<V> {
    return this.#cache.getOrCreate(key, (k) => {
      const signal = factory(k);
      signal.observe('deactivate', () => this.#remove(k));
      return signal;
    });
  }

  /**
   * Remove a specific signal from the cache by key.
   * Returns `true` if the signal was found and removed.
   */
  delete(key: K): boolean {
    const signal = this.#cache.get(key);
    if (!signal) return false;
    this.#cache.delete(key);
    this.#onEvict?.(key, signal);
    return true;
  }

  /** Remove all cached signals. */
  clear(): void {
    if (this.#onEvict) {
      for (const [key, signal] of this.#cache) {
        this.#onEvict(key, signal);
      }
    }
    this.#cache.clear();
  }

  /** Number of cached signals. */
  get size(): number {
    return this.#cache.size;
  }

  /** Iterate over all cached `[key, signal]` pairs. */
  *entries(): IterableIterator<[K, Signal<V>]> {
    yield* this.#cache.entries();
  }

  /** Iterate over all cached keys. */
  *keys(): IterableIterator<K> {
    yield* this.#cache.keys();
  }

  /** Iterate over all cached signals. */
  *values(): IterableIterator<Signal<V>> {
    yield* this.#cache.values();
  }
}
