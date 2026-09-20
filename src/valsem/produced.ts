// ---------------------------------------------------------------------------
// produced — a computed whose result is a canonical valsem value
//
// The recipe runs inside a valsem `produce()` session with no base. It may
// read signals (tracked as usual), call `draft(x)` on any value it wants to
// edit with mutable syntax, and return a value. valsem finalises the return
// value: nested drafts resolve, the whole thing is canonicalised, and every
// draft is revoked. Because the result is canonical, the signal's
// `Object.is` default already is structural equality, and so is every
// downstream computed's.
// ---------------------------------------------------------------------------

import { produce } from 'valsem';
import type { Undraft } from 'valsem';
import type { Signal } from '../types.js';
import { computed } from '../signal.js';
import type { ComputedOptions } from '../signal.js';

/**
 * A `computed` whose result is a canonical value.
 *
 * Inside `recipe`, read signals as usual and call valsem's `draft(value)`
 * on anything you want to edit in place; return the result. When the
 * inputs are themselves canonical (the output of another `produced`, or
 * anything passed through `intern`), untouched parts keep their identity
 * through structural sharing, so downstream computeds that read only
 * unchanged parts do not recompute. A plain, uncanonical input is
 * canonicalised on the way out, which costs a walk and a fresh identity.
 *
 * Two rules. A draft must not escape the recipe except through the return
 * value; valsem revokes drafts when the recipe ends, so a leaked draft
 * throws on first use. And `draft()` belongs in the `produced` recipe
 * itself, never in a plain `computed` that the recipe reads: such a computed
 * would evaluate inside the recipe's session the first time, succeed, and
 * cache a draft that is revoked the moment the recipe ends.
 *
 * The result must be a value: primitives, plain objects and arrays, and
 * valsem's collections, nested freely. An object that contains a function,
 * a signal, a `Map` or a class instance cannot be canonicalised and makes
 * the recipe throw. Keep such things in ordinary `computed`s.
 *
 * @example
 * ```ts
 * import { draft } from 'valsem';
 *
 * const visible = produced(() => todos().filter((t) => !t.done));
 *
 * const withTotals = produced(() => {
 *   const order = draft(currentOrder());
 *   order.total = order.lines.reduce((sum, l) => sum + l.price, 0);
 *   return order;
 * });
 * ```
 */
export function produced<T>(
  recipe: () => T,
  options?: ComputedOptions<Undraft<T>>,
): Signal<Undraft<T>> {
  // Base-less produce: the recipe builds the whole value. Typed loosely
  // because valsem's overloads describe the draft-of-base shape.
  const run = produce as unknown as (base: undefined, recipe: () => unknown) => Undraft<T>;
  return computed(() => run(undefined, recipe), options);
}
