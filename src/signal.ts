// ---------------------------------------------------------------------------
// Signal — synchronous, pull-based reactive values with auto-tracking
//
// Architecture: 3-set per-scheduler flush + epoch-memoized pull
//
// Three entry points:
//   createSignal(initial, opts?)  — writable signal (callable)
//   toSignal(opts)                — operator: shared upstream → Signal (callable, read-only)
//   computed(fn, opts?)           — derived signal with auto-dependency-tracking (callable)
//
// Signals are callable functions: `signal()` reads the current value.
// Inside `computed()`, calling a signal auto-registers it as a dependency.
//
// When a writable signal is set():
//   1. Value stored, version bumped, global epoch bumped
//   2. For each scheduler S with interested descendants: add to changed[S]
//   3. Schedule flush(S) via S.schedule() (idempotent)
//
// Each scheduler maintains 3 sets (SchedulerFlushState):
//   changed          — nodes whose values changed, dependents not yet expanded
//   pendingRecompute — computed nodes that need freshening
//   pendingNotify    — subscriptions ready for delivery
//
// When a scheduler flushes (3-phase loop):
//   Phase 1 — Expand: for each node in changed, collect computed dependents
//             into pendingRecompute, subscription dependents into pendingNotify.
//   Phase 2 — Recompute: for each computed in pendingRecompute, ensureFresh.
//             If value changed, add to changed for ALL interested schedulers
//             (cross-pollination — ensures every scheduler sees the update).
//   Repeat Phases 1+2 until both sets are empty.
//   Phase 3 — Notify: deliver to all subscriptions in pendingNotify.
//
// Cross-scheduler correctness: when scheduler A's Phase 2 bumps a computed's
// version, it adds that computed to changed[B]. When B flushes, Phase 1
// expands it, collecting B's subscriptions into pendingNotify. No per-node
// cross-scheduler bookkeeping needed.
//
// ensureFresh() pulls from ALL dependencies (not just the triggering one),
// catching cross-scheduler changes. Epoch marking prevents double-evaluation.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, WritableSignal, Signal, Scheduler } from './types.js';
import { PAUSE } from './types.js';
import { deepEqual } from 'valsem';

// ---------------------------------------------------------------------------
// Sentinel — distinguishes "never evaluated" from any user value
// ---------------------------------------------------------------------------

const UNSET: unique symbol = Symbol('signal.unset');

// ---------------------------------------------------------------------------
// Brand symbol — used for duck-type signal detection without Source coupling
// ---------------------------------------------------------------------------

export const SIGNAL_BRAND: unique symbol = Symbol('signal.brand');

/**
 * Type guard: is the value a Signal?
 *
 * Uses a brand symbol — does not rely on Source/connect presence.
 */
export function isSignal(value: unknown): value is Signal<unknown> {
  return typeof value === 'function' && SIGNAL_BRAND in (value as object);
}

// ---------------------------------------------------------------------------
// Max re-flush passes — prevents infinite loops from reentrant set()
// during delivery. Allows legitimate one-level re-entrancy (common case)
// while catching real bugs.
// ---------------------------------------------------------------------------

const MAX_FLUSH_ITERATIONS = 100;

// ---------------------------------------------------------------------------
// Built-in schedulers
// ---------------------------------------------------------------------------

/**
 * Immediate (synchronous) scheduler — calls callback inline.
 * Used by `observe()` as default scheduler.
 */
export const immediateScheduler: Scheduler = {
  schedule(callback) { callback(); },
};

/**
 * Build a scheduler that batches every callback handed to it into the next
 * tick of `defer`. Each callback runs exactly once, in the order scheduled,
 * as the `Scheduler` contract requires — two independent users of the same
 * scheduler instance (say, the signal coordinator and a `scheduleOn`) must
 * not overwrite each other.
 */
function batchingScheduler(defer: (tick: () => void) => void): Scheduler {
  let queue: (() => void)[] = [];
  let pending = false;
  return {
    schedule(callback: () => void) {
      queue.push(callback);
      if (pending) return;
      pending = true;
      defer(() => {
        pending = false;
        const batch = queue;
        queue = [];
        for (const cb of batch) cb();
      });
    },
  };
}

/**
 * Microtask scheduler — defers to the microtask queue. Callbacks scheduled
 * in the same tick run together in one microtask.
 */
export const microtaskScheduler: Scheduler = batchingScheduler(queueMicrotask);

/**
 * Animation frame scheduler — defers to requestAnimationFrame.
 * Falls back to setTimeout in non-browser environments.
 */
export const animationFrameScheduler: Scheduler = batchingScheduler(
  typeof requestAnimationFrame === 'function'
    ? (cb) => requestAnimationFrame(() => cb())
    : (cb) => setTimeout(cb, 16),
);

// ---------------------------------------------------------------------------
// Dependency tracking — global stack for computed() auto-tracking
// ---------------------------------------------------------------------------

/** Current tracking set. `null` when not inside a computed evaluation. */
let tracking: Set<SignalNode<any>> | null = null;

/**
 * Record a dependency read. Called from read() when tracking is active.
 * @internal
 */
function trackRead(node: SignalNode<any>): void {
  tracking?.add(node);
}

// ---------------------------------------------------------------------------
// SignalNode — thin data node for all signal types
//
// Writable signals: fn is null, value is set directly.
// Computed signals: fn returns fresh value (lazy evaluation).
//
// Dependents are downstream nodes (computeds or subscriptions) in the graph.
// schedulerRefs tracks which schedulers have interested descendants.
// ---------------------------------------------------------------------------

/** Downstream node — either a ComputedNode or a SignalSubscription. */
type Dependent = ComputedNode<any> | SignalSubscription<any>;

class SignalNode<T> {
  _value: T | typeof UNSET;
  _version = 0;
  _equals: (a: T, b: T) => boolean;

  /** The epoch at which this node was last verified fresh. */
  _epoch = -1;

  /** Downstream dependents — computeds and subscriptions. */
  _dependents = new Set<Dependent>();

  /**
   * Ref-counted scheduler interest from descendant subscriptions.
   * Used by set() to know which schedulers' dirty root lists to add to,
   * and by pushDown to prune branches with no relevant subscribers.
   */
  _schedulerRefs: Map<Scheduler, number> | null = null;

  _onUnobservedCallbacks: Set<() => void> | null = null;

  /** Callbacks fired when dependents goes 0→1. */
  _onObservedCallbacks: Set<() => void> | null = null;

  /**
   * Read hooks — called during coordinatorRead before the value is returned.
   * Each hook receives the current value and returns a (possibly modified)
   * value. Used by link() to pull the derived computation fresh on read.
   */
  _readHooks: Set<(current: T) => T> | null = null;
  _insideReadHook = false;

  constructor(
    initial: T | typeof UNSET,
    equals: (a: T, b: T) => boolean,
  ) {
    this._value = initial;
    this._equals = equals;
  }
}

// ---------------------------------------------------------------------------
// ComputedNode — extends SignalNode with tracking + lazy evaluation
// ---------------------------------------------------------------------------

class ComputedNode<T> extends SignalNode<T> {
  _fn: () => T;

  /** Upstream dependencies → version at last evaluation. */
  _depsVersions = new Map<SignalNode<any>, number>();

  /** Whether this node has ever been evaluated. */
  _initialized = false;

  constructor(fn: () => T, equals: (a: T, b: T) => boolean) {
    super(UNSET, equals);
    this._fn = fn;
  }
}

// ---------------------------------------------------------------------------
// SignalSubscription — leaf node in the dependency graph
//
// Lives as a dependent on the subscribed node. Found during push-down
// traversal — no coordinator-level registry needed.
// ---------------------------------------------------------------------------

class SignalSubscription<T> {
  _node: SignalNode<T>;
  _sink: Sink<T>;
  _scheduler: Scheduler;
  _lastDeliveredVersion = -1;
  /** Last value handed to the sink; a scheduled flush skips an equal one. */
  _lastDeliveredValue: T | typeof UNSET = UNSET;
  _paused = true;
  _disposed = false;

  constructor(node: SignalNode<T>, sink: Sink<T>, scheduler: Scheduler) {
    this._node = node;
    this._sink = sink;
    this._scheduler = scheduler;
  }
}

// ---------------------------------------------------------------------------
// SignalCoordinator — centralized behavior for all signals
//
// Module-level singleton. Owns: global epoch, per-scheduler flush state,
// flush scheduling, ensureFresh with epoch memoization, evaluate with
// dependency tracking, 3-phase flush.
// ---------------------------------------------------------------------------

/** Global epoch — incremented on every set(). */
let globalEpoch = 0;

/** Per-scheduler flush state: 3 sets driving the flush loop. */
interface SchedulerFlushState {
  changed: Set<SignalNode<any>>;
  pendingRecompute: Set<ComputedNode<any>>;
  pendingNotify: Set<SignalSubscription<any>>;
}

const schedulerStates = new Map<Scheduler, SchedulerFlushState>();

function getSchedulerState(scheduler: Scheduler): SchedulerFlushState {
  let state = schedulerStates.get(scheduler);
  if (!state) {
    state = { changed: new Set(), pendingRecompute: new Set(), pendingNotify: new Set() };
    schedulerStates.set(scheduler, state);
  }
  return state;
}

/** Schedulers with a pending flush callback. */
const pendingFlush = new Set<Scheduler>();

/** The scheduler currently being flushed (reentrancy guard). */
let flushingScheduler: Scheduler | null = null;

// --- Core operations ---

function coordinatorSet<T>(node: SignalNode<T>, value: T): void {
  if (node._value !== UNSET && node._equals(value, node._value as T)) return;
  node._value = value;
  node._version++;
  globalEpoch++;

  // Add to changed set for each interested scheduler
  if (node._schedulerRefs) {
    for (const scheduler of node._schedulerRefs.keys()) {
      getSchedulerState(scheduler).changed.add(node);

      if (scheduler === flushingScheduler) {
        // Current flush loop will pick it up
      } else if (!pendingFlush.has(scheduler)) {
        pendingFlush.add(scheduler);
        scheduler.schedule(() => flush(scheduler));
      }
    }
  }
}

function coordinatorRead<T>(node: SignalNode<T>): T {
  ensureFresh(node);

  // Fire read hooks (e.g. link pull-through) with re-entrancy guard
  if (node._readHooks?.size && !node._insideReadHook) {
    node._insideReadHook = true;
    try {
      let val = node._value as T;
      for (const hook of node._readHooks) {
        val = hook(val);
      }
      // Apply the result — only bump version if value actually changed
      if (node._value === UNSET || !node._equals(val, node._value as T)) {
        node._value = val;
        node._version++;
        globalEpoch++;
      }
    } finally {
      node._insideReadHook = false;
    }
  }

  trackRead(node);
  return node._value as T;
}

/**
 * Epoch-memoized recursive pull. Ensures a node's value is fresh.
 * For writables: always fresh (value is set directly).
 * For computeds: recurse into deps, re-evaluate if any dep changed.
 */
function ensureFresh(node: SignalNode<any>): void {
  if (!(node instanceof ComputedNode)) return; // writable — always fresh
  if (node._epoch === globalEpoch) return; // already verified this epoch
  node._epoch = globalEpoch;

  // Recurse into all dependencies
  let stale = !node._initialized;
  for (const [dep, ver] of node._depsVersions) {
    ensureFresh(dep);
    if (!stale && dep._version !== ver) {
      stale = true;
    }
  }

  if (stale) {
    try {
      evaluate(node);
    } catch (err) {
      // Not verified: the next read must evaluate (and throw) again rather
      // than hand out the stale or UNSET value.
      node._epoch = -1;
      throw err;
    }
  }
}

/**
 * Evaluate a computed node: run fn() with dependency tracking,
 * diff dependencies, update value/version.
 *
 * Registration on dependencies (the push path) happens only while the node
 * is observed — it has dependents of its own. An unobserved computed relies
 * on the pull path alone (version comparison in ensureFresh), so a plain
 * read never makes a signal `observed`, never fires `activate`, and never
 * retains the computed from its dependencies.
 */
function evaluate<T>(node: ComputedNode<T>): void {
  const prevTracking = tracking;
  const deps = new Set<SignalNode<any>>();
  tracking = deps;

  let value: T;
  try {
    value = node._fn();
  } finally {
    tracking = prevTracking;
  }

  // Commit the result first. Edge maintenance below fires activate callbacks
  // that may set() a dependency and flush synchronously; that flush must see
  // the new dependency map (so a nested re-evaluation diffs against it, not
  // against the old one) and the new version numbers (so a write during the
  // callback is noticed by the next ensureFresh).
  const previousDeps = node._depsVersions;
  node._depsVersions = new Map();
  for (const dep of deps) {
    node._depsVersions.set(dep, dep._version);
  }
  node._initialized = true;
  if (node._value === UNSET || !node._equals(value, node._value as T)) {
    node._value = value;
    node._version++;
  }

  // Diff dependency edges. Registration (and the scheduler refs that travel
  // with it) only exists while the node is observed. Each edge is checked
  // against the live graph rather than the captured sets, because a nested
  // evaluation triggered by a callback may already have done the work.
  const active = node._dependents.size > 0;

  for (const old of previousDeps.keys()) {
    if (!deps.has(old)) {
      if (old._dependents.has(node)) {
        retractSchedulerRefs(node, old);
        detachFromDep(node, old);
      }
    }
  }

  if (active) {
    for (const dep of deps) {
      if (!previousDeps.has(dep) && !dep._dependents.has(node)) {
        propagateSchedulerRefs(node, dep);
        attachToDep(node, dep);
      }
    }
  }

  // An activate callback may have written to a dependency that had no
  // scheduler interest yet, so no flush will come to correct the value just
  // committed. Re-evaluate now; the edges are attached, so this converges.
  for (const [dep, ver] of node._depsVersions) {
    if (dep._version !== ver) {
      evaluate(node);
      return;
    }
  }
}

/** Push `node`'s scheduler interest down a new dependency edge. */
function propagateSchedulerRefs(node: ComputedNode<any>, dep: SignalNode<any>): void {
  if (!node._schedulerRefs) return;
  for (const [sched, count] of node._schedulerRefs) {
    adjustSchedulerRef(dep, sched, count);
  }
}

/** Withdraw `node`'s scheduler interest from a removed dependency edge. */
function retractSchedulerRefs(node: ComputedNode<any>, dep: SignalNode<any>): void {
  if (!node._schedulerRefs) return;
  for (const [sched, count] of node._schedulerRefs) {
    adjustSchedulerRef(dep, sched, -count);
  }
}

/**
 * Register `node` as a dependent of `dep`. Fires `dep`'s activate callbacks
 * and activates `dep` itself (if computed) when this is its first dependent.
 */
function attachToDep(node: ComputedNode<any>, dep: SignalNode<any>): void {
  const wasEmpty = dep._dependents.size === 0;
  dep._dependents.add(node);
  if (wasEmpty) {
    if (dep instanceof ComputedNode) activateComputed(dep);
    if (dep._onObservedCallbacks?.size) {
      for (const cb of dep._onObservedCallbacks) cb();
    }
  }
}

/**
 * Unregister `node` from `dep`. Fires `dep`'s deactivate callbacks and
 * deactivates `dep` itself (if computed) when this was its last dependent.
 */
function detachFromDep(node: ComputedNode<any>, dep: SignalNode<any>): void {
  dep._dependents.delete(node);
  if (dep._dependents.size === 0) {
    if (dep._onUnobservedCallbacks?.size) {
      for (const cb of dep._onUnobservedCallbacks) cb();
    }
    if (dep instanceof ComputedNode) deactivateComputed(dep);
  }
}

// --- Flush: 3-phase per-scheduler loop ---

function flush(scheduler: Scheduler): void {
  pendingFlush.delete(scheduler);
  const state = getSchedulerState(scheduler);
  let iterations = 0;
  // Exceptions thrown by computed functions or observers are collected so
  // that one faulty node cannot starve its siblings of the update. They are
  // rethrown together once delivery is done.
  const errors: unknown[] = [];

  try {
    flushingScheduler = scheduler;

    // Outer loop: re-enters when delivery causes reentrant set()
    while (true) {
      if (++iterations > MAX_FLUSH_ITERATIONS) {
        throw new Error(
          'Signal update loop detected: set() was called more than ' +
            MAX_FLUSH_ITERATIONS +
            ' times during a single flush pass',
        );
      }

      // Phase 1 + 2: Expand changed → recompute → repeat until stable
      while (state.changed.size > 0 || state.pendingRecompute.size > 0) {
        // Phase 1 — Expand: changed nodes → pendingRecompute + pendingNotify
        if (state.changed.size > 0) {
          const nodes = [...state.changed];
          state.changed.clear();
          for (const node of nodes) {
            for (const dep of node._dependents) {
              if (dep instanceof ComputedNode) {
                if (dep._schedulerRefs?.has(scheduler)) {
                  state.pendingRecompute.add(dep);
                }
              } else {
                const sub = dep as SignalSubscription<any>;
                if (sub._scheduler === scheduler && !sub._paused && !sub._disposed) {
                  state.pendingNotify.add(sub);
                }
              }
            }
          }
        }

        // Phase 2 — Recompute: freshen computeds, cross-pollinate on change
        if (state.pendingRecompute.size > 0) {
          const computeds = [...state.pendingRecompute];
          state.pendingRecompute.clear();
          for (const comp of computeds) {
            const oldVersion = comp._version;
            try {
              ensureFresh(comp);
            } catch (err) {
              // ensureFresh left the node unverified; the next read will
              // evaluate again and surface the error to the reader.
              errors.push(err);
              continue;
            }
            if (comp._version !== oldVersion) {
              // Value changed — cross-pollinate to ALL interested schedulers
              if (comp._schedulerRefs) {
                for (const sched of comp._schedulerRefs.keys()) {
                  getSchedulerState(sched).changed.add(comp);
                  if (sched !== flushingScheduler && !pendingFlush.has(sched)) {
                    pendingFlush.add(sched);
                    sched.schedule(() => flush(sched));
                  }
                }
              }
            }
          }
        }
      }

      // Phase 3 — Notify: deliver to subscriptions
      if (state.pendingNotify.size > 0) {
        const subs = [...state.pendingNotify];
        state.pendingNotify.clear();
        for (const sub of subs as SignalSubscription<any>[]) {
          if (sub._disposed) continue;
          try {
            ensureFresh(sub._node);
            if (sub._node._version > sub._lastDeliveredVersion) {
              sub._lastDeliveredVersion = sub._node._version;
              const value = sub._node._value;
              // A coalescing scheduler may see a value change and change
              // back before it runs; the sink is owed nothing then.
              if (
                sub._lastDeliveredValue !== UNSET &&
                sub._node._equals(value, sub._lastDeliveredValue)
              ) {
                continue;
              }
              sub._lastDeliveredValue = value;
              const result = sub._sink.next(value);
              if (result === PAUSE) {
                sub._paused = true;
              }
            }
          } catch (err) {
            errors.push(err);
          }
        }
      }

      // If delivery caused reentrant set(), loop back
      if (state.changed.size === 0) break;
    }
  } finally {
    flushingScheduler = null;
    state.changed.clear();
    state.pendingRecompute.clear();
    state.pendingNotify.clear();
  }

  if (errors.length === 1) throw errors[0];
  if (errors.length > 1) {
    throw new AggregateError(errors, `${errors.length} errors were thrown during signal delivery`);
  }
}

// --- Scheduler ref propagation ---

/**
 * Adjust the scheduler ref count on a node and propagate upward
 * through its dependency chain (computed → its dependencies).
 */
function adjustSchedulerRef(node: SignalNode<any>, scheduler: Scheduler, delta: number): void {
  if (!node._schedulerRefs) node._schedulerRefs = new Map();
  const current = node._schedulerRefs.get(scheduler) ?? 0;
  const next = current + delta;

  if (next <= 0) {
    node._schedulerRefs.delete(scheduler);
    if (node._schedulerRefs.size === 0) node._schedulerRefs = null;
  } else {
    node._schedulerRefs.set(scheduler, next);
  }

  // Propagate upward along this node's registered dependency edges. An
  // unregistered edge (the node is unobserved, or a nested evaluation has
  // not attached it yet) carries no refs.
  if (node instanceof ComputedNode) {
    for (const dep of node._depsVersions.keys()) {
      if (dep._dependents.has(node)) adjustSchedulerRef(dep, scheduler, delta);
    }
  }
}

// --- Subscribe / Unsubscribe ---

function coordinatorSubscribe<T>(
  node: SignalNode<T>,
  sink: Sink<T>,
  scheduler: Scheduler,
): SignalSubscription<T> {
  const sub = new SignalSubscription(node, sink, scheduler);
  const wasEmpty = node._dependents.size === 0;
  node._dependents.add(sub);

  // Propagate scheduler ref upward along registered edges. For a computed
  // that is being activated there are none yet; activateComputed carries the
  // ref down each edge as it registers it.
  adjustSchedulerRef(node, scheduler, +1);

  if (wasEmpty) {
    // First dependent: a computed registers on its dependencies now
    if (node instanceof ComputedNode) activateComputed(node);
    if (node._onObservedCallbacks?.size) {
      for (const cb of node._onObservedCallbacks) cb();
    }
  }

  return sub;
}

function coordinatorUnsubscribe<T>(sub: SignalSubscription<T>): void {
  if (sub._disposed) return;
  sub._disposed = true;

  const node = sub._node;
  node._dependents.delete(sub);

  // Retract scheduler ref
  adjustSchedulerRef(node, sub._scheduler, -1);

  if (node._dependents.size === 0) {
    if (node._onUnobservedCallbacks?.size) {
      for (const cb of node._onUnobservedCallbacks) cb();
    }
    if (node instanceof ComputedNode) deactivateComputed(node);
  }
}

// --- Computed liveness ---

/**
 * A computed gained its first dependent: register it on every dependency it
 * read during its last evaluation, carrying its scheduler refs with it.
 * Scheduler refs exist only along registered edges, so the graph walk in
 * adjustSchedulerRef and the edge maintenance here never double count.
 */
function activateComputed(node: ComputedNode<any>): void {
  for (const dep of node._depsVersions.keys()) {
    if (dep._dependents.has(node)) continue;
    propagateSchedulerRefs(node, dep);
    attachToDep(node, dep);
  }
}

/** A computed lost its last dependent: unregister it from its dependencies. */
function deactivateComputed(node: ComputedNode<any>): void {
  for (const dep of node._depsVersions.keys()) {
    if (!dep._dependents.has(node)) continue;
    retractSchedulerRefs(node, dep);
    detachFromDep(node, dep);
  }
}

// ---------------------------------------------------------------------------
// observe() — low-level subscribe helper shared by all signal factories
//
// Creates a SignalSubscription via the coordinator, delivers current value
// immediately, then delivers on subsequent changes. Returns an unsubscribe
// function. No Sink/Stream/PAUSE overhead for consumers.
// ---------------------------------------------------------------------------

function observeNode<T>(
  node: SignalNode<T>,
  callback: (value: T) => void,
  scheduler: Scheduler,
): () => void {
  const sink: Sink<T> = {
    next: (value: T) => { callback(value); return undefined; },
    complete: () => {},
    error: () => {},
  };
  const sub = coordinatorSubscribe(node, sink, scheduler);
  // Start unpaused — observe delivers immediately, no resume dance
  sub._paused = false;

  // Deliver current value immediately
  ensureFresh(node);
  if (node._value !== UNSET && node._version > sub._lastDeliveredVersion) {
    sub._lastDeliveredVersion = node._version;
    sub._lastDeliveredValue = node._value;
    callback(node._value as T);
  }

  return () => coordinatorUnsubscribe(sub);
}

/**
 * Unified dispatch for signal.observe(type, callback, scheduler?).
 *
 * - 'value'      → subscribe to value changes via observeNode
 * - 'activate'   → callback fires on 0→1 dependents transition
 * - 'deactivate' → callback fires on N→0 dependents transition
 */
function observeDispatch<T>(
  node: SignalNode<T>,
  type: string,
  callback: Function,
  scheduler?: Scheduler,
): () => void {
  switch (type) {
    case 'value':
      return observeNode(node, callback as (value: T) => void, scheduler ?? immediateScheduler);
    case 'activate': {
      const cb = callback as () => void;
      if (!node._onObservedCallbacks) node._onObservedCallbacks = new Set();
      node._onObservedCallbacks.add(cb);
      return () => { node._onObservedCallbacks?.delete(cb); };
    }
    case 'deactivate': {
      const cb = callback as () => void;
      if (!node._onUnobservedCallbacks) node._onUnobservedCallbacks = new Set();
      node._onUnobservedCallbacks.add(cb);
      return () => { node._onUnobservedCallbacks?.delete(cb); };
    }
    case 'read': {
      const cb = callback as (current: T) => T;
      if (!node._readHooks) node._readHooks = new Set();
      node._readHooks.add(cb);
      return () => { node._readHooks?.delete(cb); };
    }
    default:
      throw new Error(`Unknown observe type: ${type}`);
  }
}

// ---------------------------------------------------------------------------
// TrackerNode — thin wrapper around ComputedNode for track() API
//
// Exposes _addDependent/_removeDependent for consumer code (reconciler,
// track tests). Delegates all behavior to the coordinator.
// ---------------------------------------------------------------------------

class TrackerNode<T> {
  #node: ComputedNode<T>;

  constructor(fn: () => T, equals: (a: T, b: T) => boolean) {
    this.#node = new ComputedNode(fn, equals);
  }

  get signal(): ComputedNode<T> {
    return this.#node;
  }

  get value(): T {
    return coordinatorRead(this.#node);
  }

  get observed(): boolean {
    return this.#node._dependents.size > 0;
  }

  get _version(): number {
    return this.#node._version;
  }

  runIfDirty(): T {
    return coordinatorRead(this.#node);
  }

  get _dependents(): Set<Dependent> {
    return this.#node._dependents;
  }

  _addDependent(dep: { _markDirty(): void }): void {
    // A dependent is modelled as a subscription on the immediate scheduler
    // whose delivery calls _markDirty. Going through the coordinator keeps
    // liveness and scheduler refs correct.
    let sub = this.#bridges.get(dep);
    if (sub) return; // already registered

    const sink: Sink<T> = {
      next: () => { dep._markDirty(); return undefined; },
      complete: () => {},
      error: () => {},
    };
    sub = coordinatorSubscribe(this.#node, sink, immediateScheduler);
    // Start unpaused so pushDown can deliver
    sub._paused = false;
    this.#bridges.set(dep, sub);

    // Check if deps changed while inactive (re-activate scenario)
    if (this.#node._depsVersions.size > 0) {
      let stale = false;
      for (const [d, ver] of this.#node._depsVersions) {
        ensureFresh(d);
        if (d._version !== ver) { stale = true; break; }
      }
      if (stale) {
        dep._markDirty();
      }
    }
  }

  _removeDependent(dep: { _markDirty(): void }): void {
    const sub = this.#bridges.get(dep);
    if (!sub) return;
    this.#bridges.delete(dep);
    coordinatorUnsubscribe(sub);
  }

  _markDirty(): void {
    // A tracker has no dirty flag of its own: freshness is decided by
    // version comparison on read. Present so a tracker satisfies the
    // Dependent shape and can itself be registered on another tracker.
  }

  #bridges = new Map<{ _markDirty(): void }, SignalSubscription<T>>();
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export interface CreateSignalOptions<T> {
  /** Custom equality function. Default: `deepEqual`. */
  equals?: (a: T, b: T) => boolean;
}

export interface ToSignalOptions<T> {
  /** Initial value before upstream emits. */
  initial: T;

  /** Custom equality function. Default: `deepEqual`. */
  equals?: (a: T, b: T) => boolean;

  /**
   * Keep upstream connected after last subscriber disconnects.
   *
   * - `true`   — keep alive forever (never auto-disconnect)
   * - `number` — milliseconds grace period before disconnecting
   * - `false`  — disconnect immediately (default)
   */
  keepAlive?: boolean | number;
}

export interface ComputedOptions<T> {
  /** Custom equality function. Default: `deepEqual`. */
  equals?: (a: T, b: T) => boolean;
}

/**
 * Create a writable signal.
 *
 * Signals are callable — invoke `signal()` to read the value.
 * Always holds a value. Replays the latest value to new subscribers
 * on `resume()`. Skips emission when the new value is deeply equal
 * to the current one.
 *
 * Calling `signal()` inside a `computed()` callback automatically registers
 * this signal as a dependency.
 *
 * @example
 * ```ts
 * const count = createSignal(0);
 *
 * count();              // 0
 * count.set(1);         // subscribers notified
 * count.set(1);         // skipped — same value
 * count.update(n => n + 1); // subscribers notified with 2
 * ```
 */
export function createSignal<T>(initial: T, opts?: CreateSignalOptions<T>): WritableSignal<T> {
  const node = new SignalNode<T>(initial, opts?.equals ?? deepEqual);
  const fn = function (this: void): T {
    return coordinatorRead(node);
  };
  Object.defineProperties(fn, {
    [SIGNAL_BRAND]: { value: true, configurable: true },
    observed: { get() { return node._dependents.size > 0; }, configurable: true },
    observe: {
      value: (type: string, callback: Function, scheduler?: Scheduler) =>
        observeDispatch(node, type, callback, scheduler),
      configurable: true,
    },
    set: { value: (v: T) => coordinatorSet(node, v), configurable: true },
    update: {
      value: (updater: (current: T) => T) => {
        ensureFresh(node);
        coordinatorSet(node, updater(node._value as T));
      },
      configurable: true,
    },
  });
  return fn as unknown as WritableSignal<T>;
}

/**
 * Create a read-only signal from an upstream source.
 *
 * Connects to upstream when the first subscriber arrives. Disconnects
 * when the last subscriber leaves (respecting `keepAlive`). Automatically
 * reconnects if a new subscriber arrives after disconnection.
 *
 * The upstream is never paused — signal always wants the latest value.
 * Upstream completion and errors are absorbed; signal keeps its last value.
 *
 * @example
 * ```ts
 * const sig = toSignal({ initial: 0 })(
 *   pipe(events$, scan(reducer, initialState)),
 * );
 *
 * sig(); // current value
 * const s = sig.connect(mySink);
 * s.resume(); // receives current value immediately, then updates
 * ```
 */
export function toSignal<T>(opts: ToSignalOptions<T>): (source: Source<T>) => Signal<T> {
  return (source) => {
    const equals = opts.equals ?? deepEqual;
    const keepAlive = opts.keepAlive ?? false;
    const node = new SignalNode<T>(opts.initial, equals);

    let upstream: Stream | null = null;
    let keepAliveTimer: ReturnType<typeof setTimeout> | null = null;

    function ensureUpstream(): void {
      if (keepAliveTimer !== null) {
        clearTimeout(keepAliveTimer);
        keepAliveTimer = null;
      }
      if (upstream) return;

      upstream = source.connect({
        next: (value: T): undefined => {
          coordinatorSet(node, value);
          return undefined;
        },
        complete: () => { upstream = null; },
        error: () => { upstream = null; },
      });
      upstream.resume();
    }

    function scheduleDisconnect(): void {
      if (keepAlive === true) return;
      if (typeof keepAlive === 'number' && keepAlive > 0) {
        keepAliveTimer = setTimeout(() => {
          keepAliveTimer = null;
          disconnect();
        }, keepAlive);
      } else {
        disconnect();
      }
    }

    function disconnect(): void {
      if (upstream) {
        upstream[Symbol.dispose]();
        upstream = null;
      }
    }

    // Use observe('activate'/'deactivate') for upstream lifecycle
    if (!node._onObservedCallbacks) node._onObservedCallbacks = new Set();
    node._onObservedCallbacks.add(() => ensureUpstream());
    if (!node._onUnobservedCallbacks) node._onUnobservedCallbacks = new Set();
    node._onUnobservedCallbacks.add(() => scheduleDisconnect());

    const fn = function (this: void): T {
      return coordinatorRead(node);
    };
    Object.defineProperties(fn, {
      [SIGNAL_BRAND]: { value: true, configurable: true },
      observed: { get() { return node._dependents.size > 0; }, configurable: true },
      observe: {
        value: (type: string, callback: Function, scheduler?: Scheduler) =>
          observeDispatch(node, type, callback, scheduler),
        configurable: true,
      },
    });
    return fn as unknown as Signal<T>;
  };
}

/**
 * Create a derived signal that auto-tracks dependencies.
 *
 * The computation function `fn` is **not** evaluated until first use
 * (calling the signal or `.connect()` + `resume()`). Any signal reads
 * during evaluation are automatically tracked.
 *
 * When a dependency changes, the computed is marked dirty. The value
 * is lazily recomputed on the next read. If the recomputed value is
 * equal to the previous one (deep equality by default), dependents
 * are not notified.
 *
 * Computed signals are also `Source<T>` — they can be subscribed to
 * via `connect()` like any other signal.
 *
 * @example
 * ```ts
 * const first = createSignal('John');
 * const last = createSignal('Doe');
 * const full = computed(() => `${first()} ${last()}`);
 *
 * full(); // 'John Doe'
 *
 * first.set('Jane');
 * full(); // 'Jane Doe'
 * ```
 */
export function computed<T>(fn: () => T, opts?: ComputedOptions<T>): Signal<T> {
  const node = new ComputedNode(fn, opts?.equals ?? deepEqual);
  const callable = function (this: void): T {
    return coordinatorRead(node);
  };
  Object.defineProperties(callable, {
    [SIGNAL_BRAND]: { value: true, configurable: true },
    observed: { get() { return node._dependents.size > 0; }, configurable: true },
    observe: {
      value: (type: string, callback: Function, scheduler?: Scheduler) =>
        observeDispatch(node, type, callback, scheduler),
      configurable: true,
    },
  });
  return callable as unknown as Signal<T>;
}

// ---------------------------------------------------------------------------
// track() — dependency tracking without signal creation
// ---------------------------------------------------------------------------

/**
 * A track controller is a reactive computation node in the dependency graph.
 * It tracks which signals a function reads, caches the result, and provides
 * notification when dependencies change.
 *
 * Implements both Trackable (downstream can depend on it) and Dependent
 * (receives dirty from upstream). Liveness-based: does not register with
 * dependencies until it has dependents of its own. When all dependents
 * leave, it unregisters (zero overhead when not observed).
 *
 * - `value` returns the cached result, freshening lazily if dirty.
 * - `runIfDirty()` same as `value` — runs the function only if deps changed.
 * - `observed` whether this node has dependents.
 */
export type TrackController<T> = TrackerNode<T>;

/**
 * Create a track controller for a function.
 *
 * Tracks which signals the function reads and caches the result.
 * The node participates in the reactive graph: downstream nodes
 * can register as dependents via `_addDependent()` to receive
 * dirty notifications and drive liveness.
 *
 * Does not register with dependencies until it has dependents —
 * zero overhead when not observed.
 *
 * @example
 * ```ts
 * const ctrl = track(() => users().filter(u => u.active));
 *
 * // First evaluation
 * let result = ctrl.runIfDirty();
 *
 * // Register as a dependent to receive dirty notifications
 * const dep: Dependent = {
 *   _markDirty() { result = ctrl.runIfDirty(); },
 * };
 * ctrl._addDependent(dep);
 * ```
 */
export function track<T>(fn: () => T, opts?: { equals?: (a: T, b: T) => boolean }): TrackerNode<T> {
  return new TrackerNode(fn, opts?.equals ?? deepEqual);
}

// ---------------------------------------------------------------------------
// untracked() — run a function outside the current tracking context
// ---------------------------------------------------------------------------

/**
 * Execute `fn` without registering any signal reads as dependencies
 * of the current tracking context. Returns the value produced by `fn`.
 *
 * Useful when mounting DOM or performing side-effects inside a computed
 * callback where signal reads should not create spurious dependencies.
 *
 * @example
 * ```ts
 * const c = computed(() => {
 *   const val = someSignal();         // tracked dependency
 *   const other = untracked(() => otherSignal()); // NOT tracked
 *   return val + other;
 * });
 * ```
 */
export function untracked<T>(fn: () => T): T {
  const prev = tracking;
  tracking = null;
  try {
    return fn();
  } finally {
    tracking = prev;
  }
}
