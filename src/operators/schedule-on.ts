// ---------------------------------------------------------------------------
// scheduleOn — buffer emissions and deliver on a specific scheduler
//
// Upstream values are buffered. On the first buffered value the scheduler
// is asked to schedule a flush. When the scheduler fires the callback all
// buffered values are delivered to the downstream sink in order, followed
// by any deferred complete/error.
//
// Never returns PAUSE to upstream — appropriate for synchronous signal
// sources where the buffer is tiny and transient.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator, Scheduler } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';

class ScheduleOnStream<T> implements Stream, Sink<T> {
  #upstream!: Stream;
  #buffer: T[] = [];
  #disposed = false;
  #completed = false;
  #error: unknown;
  #hasError = false;
  #scheduled = false;

  constructor(
    private readonly sink: Sink<T>,
    private readonly scheduler: Scheduler,
  ) {}

  _setUpstream(upstream: Stream): void {
    this.#upstream = upstream;
  }

  // --- Sink<T> (receives from upstream) ---

  next(value: T): undefined {
    if (this.#disposed) return undefined;
    this.#buffer.push(value);
    this.#ensureScheduled();
    return undefined;
  }

  complete(): void {
    if (this.#disposed) return;
    this.#completed = true;
    this.#ensureScheduled();
  }

  error(error: unknown): void {
    if (this.#disposed) return;
    this.#hasError = true;
    this.#error = error;
    this.#ensureScheduled();
  }

  // --- Stream (exposed to downstream) ---

  resume(): void {
    this.#upstream.resume();
  }

  [Symbol.dispose](): void {
    this.#disposed = true;
    this.#buffer.length = 0;
    this.#upstream[Symbol.dispose]();
  }

  // --- Private ---

  #ensureScheduled(): void {
    if (this.#scheduled) return;
    this.#scheduled = true;
    this.scheduler.schedule(() => this.#flush());
  }

  #flush(): void {
    this.#scheduled = false;
    if (this.#disposed) return;

    // Drain buffer
    while (this.#buffer.length > 0) {
      const value = this.#buffer.shift()!;
      if (this.#disposed) return;
      const result = this.sink.next(value);
      if (result === PAUSE) {
        // Downstream paused — keep remaining buffer, re-schedule
        if (this.#buffer.length > 0 || this.#completed || this.#hasError) {
          this.#ensureScheduled();
        }
        return;
      }
    }

    // Terminal events
    if (this.#completed) {
      this.sink.complete();
    } else if (this.#hasError) {
      this.sink.error(this.#error);
    }
  }
}

class ScheduleOnSource<T> extends AbstractSource<T> {
  constructor(
    private readonly source: Source<T>,
    private readonly scheduler: Scheduler,
  ) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    const stream = new ScheduleOnStream(sink, this.scheduler);
    const upstream = this.source.connect(stream);
    stream._setUpstream(upstream);
    return stream;
  }
}

/**
 * Buffer upstream emissions and deliver them when the scheduler fires.
 *
 * All values emitted synchronously between scheduler ticks are collected
 * and delivered in a single flush. This is the foundation for batching
 * live query updates into a single message per transaction.
 *
 * Never applies backpressure to upstream — always returns `undefined`
 * from `next()`, which is correct for synchronous signal-based sources.
 */
export function scheduleOn<T>(scheduler: Scheduler): Operator<T, T> {
  return (source) => new ScheduleOnSource(source, scheduler);
}
