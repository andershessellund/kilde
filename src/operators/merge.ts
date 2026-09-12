// ---------------------------------------------------------------------------
// merge — concurrent inner source merging
//
// Accepts Source<Source<T>>, connects to all inner sources concurrently.
// All inner values are forwarded to the output. Completes when the outer
// source and all inner sources have completed.
//
// Unlike flatten (which serializes), merge runs all inner sources at the
// same time. The outer source is never paused — new inner sources are
// always accepted.
//
// Backpressure: all inner values pass through one PauseBuffer in front of
// the downstream sink. When the downstream pauses, every inner that
// delivers afterwards has its value queued and receives PAUSE, so at most
// one value per active inner is held. On resume() the queue drains, then
// the inners are resumed round-robin starting after the one that was
// resumed last, so a busy inner cannot starve the others. Inners that
// arrive while the downstream is unpaused are started immediately.
// ---------------------------------------------------------------------------

import type { Source, Sink, Stream, Operator } from '../types.js';
import { PAUSE } from '../types.js';
import { AbstractSource } from '../abstract-source.js';
import { PauseBuffer } from '../internal/pause-buffer.js';

class MergeStream<T> implements Stream {
  readonly #buffer: PauseBuffer<T>;
  readonly #outer: Stream;
  #inners = new Map<number, Stream>();
  #nextId = 0;
  #lastResumed = -1;
  #outerCompleted = false;
  #terminated = false; // a terminal event has been handed to the buffer
  #disposed = false;

  constructor(source: Source<Source<T>>, sink: Sink<T>) {
    this.#buffer = new PauseBuffer<T>(sink);
    this.#outer = source.connect({
      next: (innerSource: Source<T>): undefined | PAUSE => {
        if (!this.#active) return PAUSE;
        this.#subscribeInner(innerSource);
        return undefined; // never pause the outer
      },
      complete: () => {
        if (!this.#active) return;
        this.#outerCompleted = true;
        this.#completeIfDone();
      },
      error: (err: unknown) => this.#fail(err),
    });
  }

  get #active(): boolean {
    return !this.#terminated && !this.#disposed;
  }

  #subscribeInner(innerSource: Source<T>): void {
    const id = this.#nextId++;
    const innerStream = innerSource.connect({
      next: (value: T): undefined | PAUSE => {
        if (!this.#active) return PAUSE;
        // Queued (and PAUSE returned) while the downstream is paused.
        return this.#buffer.push(value);
      },
      complete: () => {
        if (!this.#active) return;
        this.#inners.delete(id);
        this.#completeIfDone();
      },
      error: (err: unknown) => this.#fail(err),
    });
    this.#inners.set(id, innerStream);

    // Start it now if the downstream can take values; otherwise the next
    // resume() picks it up in the round-robin.
    if (!this.#buffer.paused) {
      this.#lastResumed = id;
      innerStream.resume();
    }
  }

  #completeIfDone(): void {
    if (this.#outerCompleted && this.#inners.size === 0) {
      this.#terminated = true;
      this.#buffer.complete(); // after any queued values
    }
  }

  #fail(err: unknown): void {
    if (!this.#active) return;
    this.#terminated = true;
    this.#disposeSources();
    this.#buffer.error(err); // after any queued values
  }

  resume(): void {
    if (this.#disposed) return;
    // Drain first. False means: still paused, or a terminal was delivered.
    if (!this.#buffer.resume()) return;
    if (!this.#active) return;

    // The outer first — it is never paused by us, but it may not have
    // started yet or may be waiting to be resumed. It may add inners.
    this.#outer.resume();
    if (this.#buffer.paused || !this.#active) return;

    this.#resumeInnersRoundRobin();
  }

  #resumeInnersRoundRobin(): void {
    const ids = [...this.#inners.keys()];
    if (ids.length === 0) return;
    let start = ids.findIndex((id) => id > this.#lastResumed);
    if (start < 0) start = 0;
    for (let k = 0; k < ids.length; k++) {
      if (this.#buffer.paused || !this.#active) return;
      const id = ids[(start + k) % ids.length];
      const inner = this.#inners.get(id);
      if (!inner) continue; // completed meanwhile
      this.#lastResumed = id;
      inner.resume();
    }
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    this.#disposeSources();
    this.#buffer.dispose();
  }

  #disposeSources(): void {
    this.#outer[Symbol.dispose]();
    for (const innerStream of this.#inners.values()) {
      innerStream[Symbol.dispose]();
    }
    this.#inners.clear();
  }
}

class MergeSource<T> extends AbstractSource<T> {
  constructor(private readonly source: Source<Source<T>>) {
    super();
  }

  connect(sink: Sink<T>): Stream {
    return new MergeStream(this.source, sink);
  }
}

/**
 * Merge all inner sources concurrently into a single output stream.
 *
 * Accepts `Source<Source<T>>` and subscribes to each inner source as it
 * arrives. Values from all inner sources are forwarded to the output.
 * Completes when the outer source and all inner sources have completed.
 *
 * The outer source is **never paused** — new inner sources are always
 * accepted (infinite concurrency). Backpressure from downstream pauses
 * every active inner (a value that arrives while paused is held and the
 * inner receives `PAUSE`); `resume()` delivers held values and then
 * resumes the inners round-robin from where it left off, so no inner
 * can starve the others. Inners that arrive while the downstream is
 * unpaused start immediately.
 *
 * Contrast with `flatten()`, which serializes inner sources (waits for
 * each to complete before subscribing to the next).
 *
 * @example
 * ```ts
 * import { stream, fromArray, merge, toArray } from 'kilde';
 *
 * const sources = fromArray([fromArray([1, 2]), fromArray([3, 4])]);
 * stream(sources, merge(), toArray()) // [1, 2, 3, 4]
 * ```
 */
export function merge<T>(): Operator<Source<T>, T> {
  return (source) => new MergeSource(source);
}
