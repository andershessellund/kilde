// ---------------------------------------------------------------------------
// scheduleOn — buffer emissions and deliver on a specific scheduler
//
// Upstream values are collected. On the first collected value (or a
// terminal event) the scheduler is asked to schedule a flush. When the
// scheduler fires, the collected batch is handed to a PauseBuffer that
// delivers to the downstream sink in order — immediately while the
// downstream is unpaused, otherwise queued until the downstream calls
// resume(), which drains without waiting for another tick. A deferred
// complete/error follows the last value.
//
// Never returns PAUSE to upstream — appropriate for synchronous signal
// sources where the buffer is tiny and transient.
// ---------------------------------------------------------------------------

import type { Sink, Operator, Scheduler, PAUSE } from '../types.js';
import { OperatorStream, OperatorSource } from '../internal/operator-stream.js';
import { PauseBuffer } from '../internal/pause-buffer.js';
import type { Terminal } from '../internal/pause-buffer.js';


class ScheduleOnStream<T> extends OperatorStream<T, T> {
  readonly #buffer: PauseBuffer<T>;
  #pending: T[] = []; // values waiting for the next scheduler tick
  #terminal: Terminal | null = null;
  #scheduled = false;

  constructor(
    sink: Sink<T>,
    private readonly scheduler: Scheduler,
  ) {
    super(sink);
    this.#buffer = new PauseBuffer<T>({
      next: (value: T) => this.emit(value),
      complete: () => this.emitComplete(),
      error: (error: unknown) => this.emitError(error),
    });
  }

  protected onValue(value: T): undefined | PAUSE {
    this.#pending.push(value);
    this.#ensureScheduled();
    return undefined; // never applies backpressure upstream
  }

  protected onComplete(): void {
    this.#terminal = { kind: 'complete' };
    this.#ensureScheduled();
  }

  protected onError(error: unknown): void {
    this.#terminal = { kind: 'error', error };
    this.#ensureScheduled();
  }

  protected onResume(): void {
    // Drain whatever a previous tick could not deliver, then let the
    // upstream continue (it is never paused by us, so this only matters
    // for the first resume and for sources that wait to be resumed).
    if (this.#buffer.resume()) this.upstream.resume();
  }

  protected onDispose(): void {
    this.#pending.length = 0;
    this.#terminal = null;
    this.#buffer.dispose();
  }

  #ensureScheduled(): void {
    if (this.#scheduled) return;
    this.#scheduled = true;
    this.scheduler.schedule(() => this.#flush());
  }

  #flush(): void {
    this.#scheduled = false;
    if (!this.active) return;

    const batch = this.#pending;
    this.#pending = [];
    for (const value of batch) {
      // Delivered now if the downstream is unpaused, queued otherwise.
      this.#buffer.push(value);
      if (!this.active) return;
    }

    const terminal = this.#terminal;
    if (terminal) {
      this.#terminal = null;
      if (terminal.kind === 'complete') this.#buffer.complete();
      else this.#buffer.error(terminal.error);
    }
  }
}

/**
 * Buffer upstream emissions and deliver them when the scheduler fires.
 *
 * All values emitted synchronously between scheduler ticks are collected
 * and delivered in a single flush. This is the foundation for batching
 * live query updates into a single message per transaction.
 *
 * If the downstream returns `PAUSE` mid-flush, the remaining values stay
 * queued and are delivered when the downstream calls `resume()` — no
 * further scheduler tick is requested for them. A `complete()` or
 * `error()` is delivered after the last value.
 *
 * Never applies backpressure to upstream — always returns `undefined`
 * from `next()`, which is correct for synchronous signal-based sources.
 * The holding buffer is therefore unbounded; do not put it behind a source
 * that can outrun the scheduler. A terminal event also waits for the tick,
 * so it is delivered after every value that preceded it.
 */
export function scheduleOn<T>(scheduler: Scheduler): Operator<T, T> {
  return (source) => new OperatorSource(source, (sink) => new ScheduleOnStream(sink, scheduler));
}
