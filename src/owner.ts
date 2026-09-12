// ---------------------------------------------------------------------------
// Owner — who tears down a hot resource when it is no longer wanted
//
// Most of kilde is owner-agnostic: signals, computeds, cold sources and
// operators clean up on their own when the last observer leaves. A few
// constructs are *hot* — they connect immediately and stay connected until
// someone disposes them: connect(), toPromise(), toCallback(),
// toAsyncIterable(), toAsyncSignal({ hot: true }), link() and
// deriveResource(). Those ask for an Owner.
//
// Ownership is resolved in this order:
//   1. An explicit `owner` option on the call.
//   2. The innermost withOwner() scope on the current (synchronous) stack.
//   3. The installed OwnerProvider — an integration hook for frameworks and
//      DI containers that already have a notion of "current scope".
//   4. noopOwner — nothing is registered; the caller keeps the handle.
//
// The provider and the withOwner() stack live on globalThis under a
// Symbol.for key, so two copies of kilde in one process share them.
// ---------------------------------------------------------------------------

/** Returned by {@link Owner.register}. Lets the resource leave early. */
export interface OwnerHandle {
  /** Remove the resource from the owner without disposing it. */
  unregister(): void;
}

/** A supervised async task, returned by {@link Owner.spawn}. */
export interface OwnedTask<T> {
  /** Settles with the task's result. */
  readonly promise: Promise<T>;
  /** Give up on the task. The owner decides what that means. */
  abort(): void;
}

/**
 * Something that owns hot resources and disposes them when it is done.
 *
 * `register` is required. `spawn` is optional — owners that supervise async
 * work (drain on shutdown, abort on cancellation, observability) implement
 * it; kilde runs the task directly when it is absent.
 */
export interface Owner {
  /**
   * Adopt a resource. Returns a handle so the resource can unregister itself
   * when it finishes on its own, or `undefined` if the owner declines
   * (the resource is then untracked).
   */
  register(resource: Disposable | AsyncDisposable, name?: string): OwnerHandle | undefined;

  /** Run an async task under this owner's supervision. */
  spawn?<T>(task: () => Promise<T>, name?: string): OwnedTask<T>;
}

/**
 * Integration hook: answers "what is the current owner?" for code that is not
 * inside a {@link withOwner} scope. Install one with {@link installOwnerProvider}.
 */
export interface OwnerProvider {
  /** The ambient owner, or `undefined` to fall through to {@link noopOwner}. */
  current(): Owner | undefined;
}

/** Option accepted by every hot construct: who adopts the resource. */
export interface OwnedOptions {
  /**
   * Owner that adopts the resource. Defaults to the ambient owner
   * (see {@link currentOwner}) at the moment the resource is created.
   */
  owner?: Owner;
}

/** An owner that adopts nothing. Hot resources stay with their caller. */
export const noopOwner: Owner = Object.freeze({
  register(): undefined {
    return undefined;
  },
});

// ---------------------------------------------------------------------------
// Process-wide state (shared across duplicate copies of the package)
// ---------------------------------------------------------------------------

interface OwnerState {
  provider: OwnerProvider | undefined;
  stack: (Owner | undefined)[];
}

const STATE_KEY = Symbol.for('kilde.owner-state');

function state(): OwnerState {
  const g = globalThis as unknown as Record<symbol, OwnerState | undefined>;
  return (g[STATE_KEY] ??= { provider: undefined, stack: [] });
}

/**
 * Resolve the ambient owner: the innermost {@link withOwner} scope, else the
 * installed provider's answer, else {@link noopOwner}.
 */
export function currentOwner(): Owner {
  const s = state();
  if (s.stack.length > 0) {
    return s.stack[s.stack.length - 1] ?? noopOwner;
  }
  return s.provider?.current() ?? noopOwner;
}

/**
 * Install (or, with `undefined`, remove) the process-wide owner provider.
 * Returns the previously installed provider so callers can restore it.
 *
 * This is the seam an integration package uses to make hot resources attach
 * to its own scopes automatically. Application code rarely calls it.
 */
export function installOwnerProvider(
  provider: OwnerProvider | undefined,
): OwnerProvider | undefined {
  const s = state();
  const previous = s.provider;
  s.provider = provider;
  return previous;
}

/**
 * Run `fn` with `owner` as the ambient owner. Pass `undefined` to run with
 * no owner at all (hot resources created inside stay untracked).
 *
 * The scope is **synchronous**: it ends when `fn` returns, and is not
 * carried across `await`. Pass an explicit `owner` option to a hot construct
 * when it is created after an `await`.
 */
export function withOwner<R>(owner: Owner | undefined, fn: () => R): R {
  const s = state();
  s.stack.push(owner);
  try {
    return fn();
  } finally {
    s.stack.pop();
  }
}

// ---------------------------------------------------------------------------
// createOwner — a plain collecting owner
// ---------------------------------------------------------------------------

/** An {@link Owner} that collects resources and disposes them together. */
export interface OwnerScope extends Owner, Disposable, AsyncDisposable {
  /** Optional name, for diagnostics. */
  readonly name: string | undefined;
  /** Whether {@link OwnerScope.dispose} has been called. */
  readonly disposed: boolean;
  /** Number of resources currently registered. */
  readonly size: number;
  /**
   * Dispose every registered resource in reverse registration order.
   * Synchronous disposers run immediately; the returned promise settles when
   * asynchronous disposers have finished. Idempotent.
   */
  dispose(): Promise<void>;
}

interface Entry {
  resource: Disposable | AsyncDisposable;
  name: string | undefined;
}

/**
 * Create a standalone owner: a scope that adopts hot resources and tears
 * them all down when disposed.
 *
 * @example
 * ```ts
 * const scope = createOwner('page');
 * withOwner(scope, () => {
 *   link(selected, (prev) => (options().includes(prev) ? prev : options()[0]));
 * });
 * // later
 * await scope.dispose();
 * ```
 */
export function createOwner(name?: string): OwnerScope {
  const entries: Entry[] = [];
  let disposed = false;

  function disposeOne(entry: Entry, pending: Promise<void>[], errors: unknown[]): void {
    const r = entry.resource as Partial<Disposable> & Partial<AsyncDisposable>;
    const sync = r[Symbol.dispose];
    const async = r[Symbol.asyncDispose];
    try {
      if (typeof sync === 'function') {
        sync.call(r);
      } else if (typeof async === 'function') {
        pending.push(Promise.resolve(async.call(r)).then(() => undefined));
      }
    } catch (err) {
      errors.push(err);
    }
  }

  function disposeAll(): { pending: Promise<void>[]; errors: unknown[] } {
    const pending: Promise<void>[] = [];
    const errors: unknown[] = [];
    if (disposed) return { pending, errors };
    disposed = true;
    for (const entry of entries.splice(0).reverse()) {
      disposeOne(entry, pending, errors);
    }
    return { pending, errors };
  }

  function raise(errors: unknown[]): void {
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) throw new AggregateError(errors, `${name ?? 'owner'}: disposal failed`);
  }

  const scope: OwnerScope = {
    name,
    get disposed() {
      return disposed;
    },
    get size() {
      return entries.length;
    },
    register(resource, resourceName) {
      const entry: Entry = { resource, name: resourceName };
      if (disposed) {
        // The scope is already gone — tear the newcomer down right away.
        const pending: Promise<void>[] = [];
        const errors: unknown[] = [];
        disposeOne(entry, pending, errors);
        raise(errors);
        return { unregister() {} };
      }
      entries.push(entry);
      return {
        unregister() {
          const i = entries.indexOf(entry);
          if (i >= 0) entries.splice(i, 1);
        },
      };
    },
    async dispose() {
      const { pending, errors } = disposeAll();
      const settled = await Promise.allSettled(pending);
      for (const s of settled) if (s.status === 'rejected') errors.push(s.reason);
      raise(errors);
    },
    [Symbol.dispose]() {
      const { pending, errors } = disposeAll();
      if (pending.length > 0) void Promise.all(pending);
      raise(errors);
    },
    [Symbol.asyncDispose]() {
      return scope.dispose();
    },
  };
  return scope;
}
