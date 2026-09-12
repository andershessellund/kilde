// ---------------------------------------------------------------------------
// kilde — barrel exports and Stream namespace
//
// The Stream namespace provides all stream functions in one place:
//   Stream.fromArray, Stream.map, Stream.reduce, Stream.stream, etc.
//
// Individual functions are also exported for direct import:
//   import { fromArray, map, reduce, stream } from 'kilde';
// ---------------------------------------------------------------------------

// --- Ownership (who tears down hot resources) ---
export {
  currentOwner,
  installOwnerProvider,
  withOwner,
  createOwner,
  noopOwner,
} from './owner.js';
export type {
  Owner,
  OwnerHandle,
  OwnedTask,
  OwnerProvider,
  OwnedOptions,
  OwnerScope,
} from './owner.js';

// --- Types ---
export type {
  Source,
  StreamableSource,
  Sink,
  Stream as StreamConnection,
  Operator,
  Deferred,
  Relay,
  Scheduler,
  Signal,
  WritableSignal,
  Store,
} from './types.js';
export { PAUSE } from './types.js';

// --- Base class ---
export { AbstractSource } from './abstract-source.js';

// --- Entry points ---
export { stream, pipe, comp } from './stream.js';

// --- Sources ---
export { fromArray, of } from './sources/from-array.js';
export { fromIterator } from './sources/from-iterator.js';
export { empty } from './sources/empty.js';
export { deferred } from './sources/deferred.js';
export { fromReadableStream } from './sources/from-readable-stream.js';
export { fromChannel } from './sources/from-channel.js';
export { fromSignal } from './sources/from-signal.js';
export { fromPromise, fromAsyncFn } from './sources/from-promise.js';

// --- Operators ---
export { map } from './operators/map.js';
export { filter } from './operators/filter.js';
export { take } from './operators/take.js';
export { scan } from './operators/scan.js';
export { reduce } from './operators/reduce.js';
export { flatten } from './operators/flatten.js';
export { merge } from './operators/merge.js';
export { catchError } from './operators/catch-error.js';
export { pausable } from './operators/pausable.js';
export { switchMap } from './operators/switch-map.js';
export { scheduleOn } from './operators/schedule-on.js';
export { toArray } from './operators/to-array.js';
export { toSource } from './operators/to-source.js';
export { toPromise } from './operators/to-promise.js';
export { toCallback } from './operators/to-callback.js';
export { toAsync } from './operators/to-async.js';
export { toAsyncIterable } from './operators/to-async-iterable.js';
export { toAsyncSignal } from './operators/to-async-signal.js';
export type { ToAsyncSignalOptions } from './operators/to-async-signal.js';
export { toReadableStream } from './operators/to-readable-stream.js';
export { lines } from './operators/lines.js';

// --- Sinks ---
export { intoWritableStream, sourceIntoWritableStream } from './into-writable-stream.js';

// --- Combinators ---
export { combineLatest } from './combine-latest.js';
export type { CombineLatestResult } from './combine-latest.js';

// --- Signals ---
export { createSignal, toSignal, computed, track, untracked, isSignal, SIGNAL_BRAND } from './signal.js';
export type { CreateSignalOptions, ToSignalOptions, ComputedOptions, TrackController } from './signal.js';
export { immediateScheduler, microtaskScheduler, animationFrameScheduler } from './signal.js';
export { link, linkedSignal } from './linked-signal.js';
export type { LinkOptions } from './linked-signal.js';

// --- Signal Deduplicator ---
export { SignalDeduplicator } from './signal-collection.js';
export type { SignalDeduplicatorOptions } from './signal-collection.js';

// --- AsyncValue / AsyncState ---
export {
  unavailable,
  loading,
  available,
  errored,
  isUnavailable,
  isLoading,
  isAvailable,
  isErrored,
  valueOr,
  mapValue,
  combineValues,
} from './async-value.js';
export type {
  Unavailable,
  Loading,
  Available,
  Errored,
  AsyncValue,
} from './async-value.js';
export {
  computedAsync,
  deriveResource,
  alwaysAvailable,
  createAsyncSignal,
  mapAsync,
  combineAsync,
  switchMapAsync,
  asAsyncSignal,
} from './async-state.js';
export type {
  AsyncSignal,
  ReloadableAsyncSignal,
  ManagedAsyncSignal,
  ComputedAsyncOptions,
  ComputedAsyncReturn,
  DerivedValue,
  DeriveResourceOptions,
  DeriveResourceReturn,
} from './async-state.js';

// --- Store ---
export { createStore, intoStore } from './store.js';
export type { CreateStoreOptions } from './store.js';

// --- Relay ---
export { createRelay } from './relay.js';

// --- Tracked connection (manual boundary API) ---
export { connect } from './connect.js';
export { StreamDisposedError } from './stream-disposed-error.js';

// --- Channels ---
export type { Choice, ChoiceAwaitValue } from './choice.js';
export { select, UnhandledCloseError, UnhandledRejectionError } from './select.js';
export type { SelectMap, SelectBranch, SelectResult } from './select.js';
export type { Channel, ReadChannel, WriteChannel, ChannelBuffer } from './channel.js';
export { createChannel, droppingBuffer, slidingBuffer, unboundedBuffer } from './channel.js';
export { take as channelTake } from './choices/take.js';
export type { TakeResult } from './choices/take.js';
export { put } from './choices/put.js';
export { closed } from './choices/closed.js';
export { timeout } from './choices/timeout.js';
export { resolved, rejected } from './choices/promise.js';
export { defaultChoice } from './choices/default.js';
export { intoChannel } from './operators/into-channel.js';
export type { IntoChannelOptions } from './operators/into-channel.js';

// ---------------------------------------------------------------------------
// Stream namespace — all functions in one object
// ---------------------------------------------------------------------------

import { stream as _stream, pipe as _pipe, comp as _comp } from './stream.js';
import { fromArray as _fromArray, of as _of } from './sources/from-array.js';
import { fromIterator as _fromIterator } from './sources/from-iterator.js';
import { empty as _empty } from './sources/empty.js';
import { deferred as _deferred } from './sources/deferred.js';
import { fromReadableStream as _fromReadableStream } from './sources/from-readable-stream.js';
import { fromChannel as _fromChannel } from './sources/from-channel.js';
import { fromSignal as _fromSignal } from './sources/from-signal.js';
import { fromPromise as _fromPromise, fromAsyncFn as _fromAsyncFn } from './sources/from-promise.js';
import { map as _map } from './operators/map.js';
import { filter as _filter } from './operators/filter.js';
import { take as _take } from './operators/take.js';
import { scan as _scan } from './operators/scan.js';
import { reduce as _reduce } from './operators/reduce.js';
import { flatten as _flatten } from './operators/flatten.js';
import { merge as _merge } from './operators/merge.js';
import { catchError as _catchError } from './operators/catch-error.js';
import { pausable as _pausable } from './operators/pausable.js';
import { switchMap as _switchMap } from './operators/switch-map.js';
import { scheduleOn as _scheduleOn } from './operators/schedule-on.js';
import { toArray as _toArray } from './operators/to-array.js';
import { toSource as _toSource } from './operators/to-source.js';
import { toPromise as _toPromise } from './operators/to-promise.js';
import { toCallback as _toCallback } from './operators/to-callback.js';
import { toAsync as _toAsync } from './operators/to-async.js';
import { toAsyncIterable as _toAsyncIterable } from './operators/to-async-iterable.js';
import { toReadableStream as _toReadableStream } from './operators/to-readable-stream.js';
import { lines as _lines } from './operators/lines.js';
import { intoWritableStream as _intoWritableStream } from './into-writable-stream.js';
import { combineLatest as _combineLatest } from './combine-latest.js';
import { createSignal as _createSignal, toSignal as _toSignal, computed as _computed, track as _track, isSignal as _isSignal, SIGNAL_BRAND as _SIGNAL_BRAND, immediateScheduler as _immediateScheduler, microtaskScheduler as _microtaskScheduler, animationFrameScheduler as _animationFrameScheduler } from './signal.js';
import { createStore as _createStore, intoStore as _intoStore } from './store.js';
import { createRelay as _createRelay } from './relay.js';
import { connect as _connect } from './connect.js';
import { select as _select } from './select.js';
import { createChannel as _createChannel, droppingBuffer as _droppingBuffer, slidingBuffer as _slidingBuffer, unboundedBuffer as _unboundedBuffer } from './channel.js';
import { take as _channelTake } from './choices/take.js';
import { put as _put } from './choices/put.js';
import { closed as _closed } from './choices/closed.js';
import { timeout as _timeout } from './choices/timeout.js';
import { resolved as _resolved, rejected as _rejected } from './choices/promise.js';
import { defaultChoice as _defaultChoice } from './choices/default.js';
import { intoChannel as _intoChannel } from './operators/into-channel.js';

/**
 * Stream namespace — all stream functions gathered in one place.
 *
 * @example
 * ```ts
 * import { Stream } from 'kilde';
 *
 * const result = Stream.stream(
 *   Stream.fromArray([1, 2, 3]),
 *   Stream.map(x => x * 2),
 *   Stream.reduce((a, b) => a + b, 0),
 * ); // 12
 * ```
 */
export const Stream = {
  // Entry points
  stream: _stream,
  pipe: _pipe,
  comp: _comp,

  // Sources
  fromArray: _fromArray,
  of: _of,
  fromIterator: _fromIterator,
  empty: _empty,
  deferred: _deferred,
  fromReadableStream: _fromReadableStream,
  fromChannel: _fromChannel,
  fromSignal: _fromSignal,
  fromPromise: _fromPromise,
  fromAsyncFn: _fromAsyncFn,

  // Operators
  map: _map,
  filter: _filter,
  take: _take,
  scan: _scan,
  reduce: _reduce,
  flatten: _flatten,
  merge: _merge,
  catchError: _catchError,
  pausable: _pausable,
  switchMap: _switchMap,
  scheduleOn: _scheduleOn,
  toArray: _toArray,
  toSource: _toSource,
  toPromise: _toPromise,
  toCallback: _toCallback,
  toAsync: _toAsync,
  toAsyncIterable: _toAsyncIterable,
  toReadableStream: _toReadableStream,
  lines: _lines,

  // Sinks
  intoWritableStream: _intoWritableStream,

  // Combinators
  combineLatest: _combineLatest,

  // Signals
  createSignal: _createSignal,
  toSignal: _toSignal,
  computed: _computed,
  track: _track,
  isSignal: _isSignal,
  SIGNAL_BRAND: _SIGNAL_BRAND,
  immediateScheduler: _immediateScheduler,
  microtaskScheduler: _microtaskScheduler,
  animationFrameScheduler: _animationFrameScheduler,

  // Store
  createStore: _createStore,
  intoStore: _intoStore,

  // Relay
  createRelay: _createRelay,

  // Tracked connection (manual boundary API)
  connect: _connect,

  // Channels
  select: _select,
  createChannel: _createChannel,
  droppingBuffer: _droppingBuffer,
  slidingBuffer: _slidingBuffer,
  unboundedBuffer: _unboundedBuffer,
  channelTake: _channelTake,
  put: _put,
  closed: _closed,
  timeout: _timeout,
  resolved: _resolved,
  rejected: _rejected,
  defaultChoice: _defaultChoice,
  intoChannel: _intoChannel,
} as const;
