// ---------------------------------------------------------------------------
// OperatorStream — base class for one-in/one-out operator connections
//
// Sits between an upstream Source<In> and a downstream Sink<Out>. Owns the
// bookkeeping every operator needs and enforces the protocol in one place:
//
//   - nothing is forwarded downstream after a terminal event or dispose
//   - exceptions thrown by the operator's own logic (`onValue`, `onComplete`,
//     `onError`) are routed to the downstream `error()` and the upstream is
//     disposed; exceptions thrown by the downstream sink itself (inside
//     `emit()`, `emitComplete()`, `emitError()`, `finish()` or `fail()`) are
//     NOT caught and propagate to the producer, exactly as they would
//     without the operator in the chain
//   - `resume()` after a terminal event or dispose is a no-op
//
// Subclasses implement `onValue` and may override `onComplete`, `onError`,
// `onResume` and `onDispose`.
//
// The "did the downstream throw?" distinction is a flag set by the emit
// helpers. A hook that catches an exception from an emit helper and then
// throws something of its own would be misclassified as a downstream throw;
// no hook should do that. `onResume` and `onDispose` are not routed: they
// run on the downstream's call stack, so their exceptions go to the caller.
// ---------------------------------------------------------------------------

import type { Sink, Source, Stream } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

export abstract class OperatorStream<In, Out> implements Stream, Sink<In> {
  protected upstream!: Stream;
  #terminated = false;
  #disposed = false;
  // Set when an exception escaping `onValue` originated in the downstream
  // sink (via one of the emit helpers) rather than in the operator itself.
  #downstreamThrew = false;

  constructor(protected readonly sink: Sink<Out>) {}

  /** @internal Called by `connectOperator` once the upstream connection exists. */
  _setUpstream(upstream: Stream): void {
    this.upstream = upstream;
  }

  /** Whether the downstream has received `complete()` or `error()`. */
  protected get terminated(): boolean {
    return this.#terminated;
  }

  /** Whether `[Symbol.dispose]` has been called. */
  protected get disposed(): boolean {
    return this.#disposed;
  }

  /** Whether the connection can still deliver anything downstream. */
  protected get active(): boolean {
    return !this.#terminated && !this.#disposed;
  }

  // --- Sink<In> (receives from upstream) ---

  next(value: In): undefined | PAUSE {
    if (!this.active) return PAUSE;
    this.#downstreamThrew = false;
    try {
      return this.onValue(value);
    } catch (err) {
      this.#route(err);
      return PAUSE;
    }
  }

  complete(): void {
    if (!this.active) return;
    this.#downstreamThrew = false;
    try {
      this.onComplete();
    } catch (err) {
      this.#route(err);
    }
  }

  error(error: unknown): void {
    if (!this.active) return;
    this.#downstreamThrew = false;
    try {
      this.onError(error);
    } catch (err) {
      this.#route(err);
    }
  }

  /**
   * An exception escaped a hook. If the downstream sink threw it, let it
   * propagate to the producer untouched; otherwise it is an operator
   * failure and the downstream is told.
   */
  #route(err: unknown): void {
    if (this.#downstreamThrew) {
      this.#downstreamThrew = false;
      throw err;
    }
    this.fail(err);
  }

  // --- Stream (exposed to downstream) ---

  resume(): void {
    if (!this.active) return;
    this.onResume();
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.onDispose();
    this.upstream[Symbol.dispose]();
  }

  // --- Hooks for subclasses ---

  /** Handle one upstream value. Return `PAUSE` to pause the upstream. */
  protected abstract onValue(value: In): undefined | PAUSE;

  /** Upstream completed. Default: forward. */
  protected onComplete(): void {
    this.emitComplete();
  }

  /** Upstream errored. Default: forward. */
  protected onError(error: unknown): void {
    this.emitError(error);
  }

  /** Downstream resumed. Default: resume upstream. */
  protected onResume(): void {
    this.upstream.resume();
  }

  /** Disposed by downstream. Default: nothing (upstream is disposed by the base). */
  protected onDispose(): void {}

  // --- Helpers for subclasses ---

  /** Forward a value downstream. Returns the downstream's pause decision. */
  protected emit(value: Out): undefined | PAUSE {
    if (!this.active) return PAUSE;
    try {
      return this.sink.next(value);
    } catch (err) {
      this.#downstreamThrew = true;
      throw err;
    }
  }

  /** Complete the downstream. Idempotent. */
  protected emitComplete(): void {
    if (!this.active) return;
    this.#terminated = true;
    try {
      this.sink.complete();
    } catch (err) {
      this.#downstreamThrew = true;
      throw err;
    }
  }

  /** Error the downstream. Idempotent. */
  protected emitError(error: unknown): void {
    if (!this.active) return;
    this.#terminated = true;
    try {
      this.sink.error(error);
    } catch (err) {
      this.#downstreamThrew = true;
      throw err;
    }
  }

  /** Complete the downstream and release the upstream (e.g. `take` is satisfied). */
  protected finish(): void {
    if (!this.active) return;
    this.#terminated = true;
    this.upstream[Symbol.dispose]();
    try {
      this.sink.complete();
    } catch (err) {
      this.#downstreamThrew = true;
      throw err;
    }
  }

  /** Fail: release the upstream, then error the downstream. */
  protected fail(error: unknown): void {
    if (!this.active) return;
    this.#terminated = true;
    this.upstream[Symbol.dispose]();
    try {
      this.sink.error(error);
    } catch (err) {
      this.#downstreamThrew = true;
      throw err;
    }
  }
}

/** Connect `stream` to `source` and hand it its upstream handle. */
export function connectOperator<In, Out>(
  source: Source<In>,
  stream: OperatorStream<In, Out>,
): Stream {
  stream._setUpstream(source.connect(stream));
  return stream;
}

/**
 * A `Source<Out>` that, on every `connect()`, builds a fresh
 * {@link OperatorStream} via `create` and wires it to `source`. Saves each
 * operator from declaring its own Source subclass.
 */
export class OperatorSource<In, Out> extends AbstractSource<Out> {
  constructor(
    private readonly source: Source<In>,
    private readonly create: (sink: Sink<Out>) => OperatorStream<In, Out>,
  ) {
    super();
  }

  connect(sink: Sink<Out>): Stream {
    return connectOperator(this.source, this.create(sink));
  }
}
