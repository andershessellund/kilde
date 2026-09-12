// ---------------------------------------------------------------------------
// Channel<T> — CSP-style coordination primitive
//
// Channels are NOT resources — no [Symbol.dispose]. They are open or closed.
// Anyone can close a channel, or it can live forever (common for UI events).
//
// createChannel(bufferOrSize?) — 0 = rendezvous (default), N = fixed buffer
// droppingBuffer(n)           — drop newest when full (never blocks sender)
// slidingBuffer(n)            — drop oldest when full (never blocks sender)
// unboundedBuffer()           — unlimited capacity, sender never blocks
//
// Channels expose their internal queues to choice implementations (take,
// put, closed) via package-internal symbols.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ChannelBuffer<T> — pluggable buffer strategy
// ---------------------------------------------------------------------------

/**
 * Buffer strategy for a channel. Determines how values are stored
 * and what happens when the buffer is "full".
 *
 * Built-in strategies:
 * - Fixed (default): blocks sender when full.
 * - Dropping: silently drops new values when full, sender never blocks.
 * - Sliding: drops oldest value when full, sender never blocks.
 */
export interface ChannelBuffer<T> {
  /** Add a value. For dropping/sliding, may discard values. */
  push(value: T): void;
  /** Remove and return the oldest value. */
  pop(): T;
  /** Number of items currently buffered. */
  readonly count: number;
  /** Whether the buffer cannot accept more values (sender should block). */
  readonly isFull: boolean;
}

// ---------------------------------------------------------------------------
// Built-in buffer strategies
// ---------------------------------------------------------------------------

class FixedBuffer<T> implements ChannelBuffer<T> {
  #items: T[] = [];
  #size: number;

  constructor(size: number) {
    this.#size = size;
  }

  push(value: T): void {
    this.#items.push(value);
  }

  pop(): T {
    return this.#items.shift()!;
  }

  get count(): number {
    return this.#items.length;
  }

  get isFull(): boolean {
    return this.#items.length >= this.#size;
  }
}

class DroppingBuffer<T> implements ChannelBuffer<T> {
  #items: T[] = [];
  #size: number;

  constructor(size: number) {
    this.#size = size;
  }

  push(value: T): void {
    // Drop if full — silently discard
    if (this.#items.length < this.#size) {
      this.#items.push(value);
    }
  }

  pop(): T {
    return this.#items.shift()!;
  }

  get count(): number {
    return this.#items.length;
  }

  // Dropping buffers never block the sender
  get isFull(): boolean {
    return false;
  }
}

class SlidingBuffer<T> implements ChannelBuffer<T> {
  #items: T[] = [];
  #size: number;

  constructor(size: number) {
    this.#size = size;
  }

  push(value: T): void {
    this.#items.push(value);
    // Evict oldest if over capacity
    if (this.#items.length > this.#size) {
      this.#items.shift();
    }
  }

  pop(): T {
    return this.#items.shift()!;
  }

  get count(): number {
    return this.#items.length;
  }

  // Sliding buffers never block the sender
  get isFull(): boolean {
    return false;
  }
}

/**
 * Create a dropping buffer. When full, new values are silently discarded.
 * The sender never blocks.
 *
 * @example
 * ```ts
 * const ch = createChannel<number>(droppingBuffer(100));
 * ```
 */
export function droppingBuffer<T>(size: number): ChannelBuffer<T> {
  if (size < 1 || !Number.isInteger(size)) {
    throw new Error(`Buffer size must be a positive integer, got ${size}`);
  }
  return new DroppingBuffer(size);
}

/**
 * Create a sliding buffer. When full, the oldest value is evicted to
 * make room for the new one. The sender never blocks.
 *
 * @example
 * ```ts
 * const ch = createChannel<number>(slidingBuffer(100));
 * ```
 */
export function slidingBuffer<T>(size: number): ChannelBuffer<T> {
  if (size < 1 || !Number.isInteger(size)) {
    throw new Error(`Buffer size must be a positive integer, got ${size}`);
  }
  return new SlidingBuffer(size);
}

// ---------------------------------------------------------------------------
// Unbounded buffer
// ---------------------------------------------------------------------------

class UnboundedBuffer<T> implements ChannelBuffer<T> {
  #items: T[] = [];

  push(value: T): void {
    this.#items.push(value);
  }

  pop(): T {
    return this.#items.shift()!;
  }

  get count(): number {
    return this.#items.length;
  }

  get isFull(): boolean {
    return false;
  }
}

/**
 * Create an unbounded buffer. Capacity is unlimited — the sender never
 * blocks. Use with care: an unbounded buffer can grow without limit if
 * the consumer cannot keep up.
 *
 * @example
 * ```ts
 * const ch = createChannel<Event>(unboundedBuffer());
 * ```
 */
export function unboundedBuffer<T>(): ChannelBuffer<T> {
  return new UnboundedBuffer();
}

// ---------------------------------------------------------------------------
// ReadChannel<T> / WriteChannel<T> / Channel<T> — public interfaces
// ---------------------------------------------------------------------------

/**
 * Read-only view of a channel. Can take values and observe closure,
 * but cannot send or close.
 */
export interface ReadChannel<T> {
  /** Whether the channel has been closed. */
  readonly closed: boolean;
  /** Whether a synchronous take would succeed right now. */
  readonly canTakeSync: boolean;
  /**
   * Take a value synchronously. Throws if no value is available
   * (check `canTakeSync` first, or use `take()` choice for async).
   */
  takeSync(): T;
}

/**
 * Write-only view of a channel. Can send values and close,
 * but cannot take.
 */
export interface WriteChannel<T> {
  /** Whether the channel has been closed. */
  readonly closed: boolean;
  /** Close the channel. Pending takers are notified. */
  close(): void;
  /** Whether a synchronous put would succeed without exceeding the buffer. */
  readonly canPutSync: boolean;
  /**
   * Put a value synchronously. If the buffer is full, the value is
   * force-pushed (exceeding the buffer limit) rather than blocking.
   * Throws if the channel is closed.
   */
  putSync(value: T): void;
}

/**
 * Bidirectional channel — extends both `ReadChannel<T>` and `WriteChannel<T>`.
 */
export interface Channel<T> extends ReadChannel<T>, WriteChannel<T> {}

// ---------------------------------------------------------------------------
// Internal symbols — used by take/put/closed choices
// ---------------------------------------------------------------------------

/** @internal */ export const ChanBuf: unique symbol = Symbol('chan.buf');
/** @internal */ export const ChanPendingSenders: unique symbol = Symbol('chan.pendingSenders');
/** @internal */ export const ChanPendingReceivers: unique symbol = Symbol('chan.pendingReceivers');
/** @internal */ export const ChanCloseListeners: unique symbol = Symbol('chan.closeListeners');
/** @internal */ export const ChanClosed: unique symbol = Symbol('chan.closed');

/** @internal A pending sender waiting for buffer space or a receiver. */
export interface PendingSender<T> {
  value: T;
  notify: () => void;
}

/** @internal A pending receiver waiting for a value. */
export interface PendingReceiver {
  notify: () => void;
}

/** @internal The full internal channel shape. */
export interface ChannelImpl<T> extends Channel<T> {
  [ChanBuf]: ChannelBuffer<T>;
  [ChanPendingSenders]: PendingSender<T>[];
  [ChanPendingReceivers]: PendingReceiver[];
  [ChanCloseListeners]: Set<() => void>;
  [ChanClosed]: boolean;
}

// ---------------------------------------------------------------------------
// createChannel()
// ---------------------------------------------------------------------------

/**
 * Create a channel for CSP-style coordination.
 *
 * - No argument or `0`: rendezvous — sender blocks until a receiver
 *   is ready, and vice versa.
 * - `number > 0`: fixed buffer — up to N values can be sent without blocking.
 * - `droppingBuffer(n)`: drop newest when full, sender never blocks.
 * - `slidingBuffer(n)`: drop oldest when full, sender never blocks.
 * - `unboundedBuffer()`: unlimited capacity, sender never blocks.
 *
 * Channels are not resources. They have no `[Symbol.dispose]` method. They are
 * either open or closed.
 *
 * @example
 * ```ts
 * const ch = createChannel<string>(1);           // fixed buffer
 * const drop = createChannel<Event>(droppingBuffer(100)); // lossy
 * const slide = createChannel<number>(slidingBuffer(1));  // latest-only
 * const unbounded = createChannel<number>(unboundedBuffer());
 * ```
 */
export function createChannel<T>(bufferOrSize: number | ChannelBuffer<T> = 0): Channel<T> {
  let buf: ChannelBuffer<T>;
  if (typeof bufferOrSize === 'number') {
    if (bufferOrSize < 0 || !Number.isInteger(bufferOrSize)) {
      throw new Error(`Channel buffer size must be a non-negative integer, got ${bufferOrSize}`);
    }
    buf = new FixedBuffer<T>(bufferOrSize);
  } else {
    buf = bufferOrSize;
  }

  const channel: ChannelImpl<T> = {
    [ChanBuf]: buf,
    [ChanPendingSenders]: [],
    [ChanPendingReceivers]: [],
    [ChanCloseListeners]: new Set(),
    [ChanClosed]: false,

    get closed(): boolean {
      return this[ChanClosed];
    },

    get canTakeSync(): boolean {
      return this[ChanBuf].count > 0 || this[ChanPendingSenders].length > 0;
    },

    takeSync(): T {
      const buf = this[ChanBuf];

      // Try buffer first
      if (buf.count > 0) {
        const value = buf.pop();
        // If there are pending senders, move one into the buffer
        if (this[ChanPendingSenders].length > 0) {
          const sender = this[ChanPendingSenders].shift()!;
          buf.push(sender.value);
          sender.notify();
        }
        return value;
      }

      // Try a pending sender directly (rendezvous)
      if (this[ChanPendingSenders].length > 0) {
        const sender = this[ChanPendingSenders].shift()!;
        sender.notify();
        return sender.value;
      }

      throw new Error('takeSync(): no value available');
    },

    get canPutSync(): boolean {
      if (this[ChanClosed]) return false;
      return this[ChanPendingReceivers].length > 0 || !this[ChanBuf].isFull;
    },

    putSync(value: T): void {
      if (this[ChanClosed]) {
        throw new Error('Cannot send on a closed channel');
      }

      const buf = this[ChanBuf];

      // Deliver to a pending receiver
      if (this[ChanPendingReceivers].length > 0) {
        buf.push(value);
        const receiver = this[ChanPendingReceivers].shift()!;
        receiver.notify();
        return;
      }

      // Force-push into buffer (exceeds limit if full)
      buf.push(value);
    },

    close(): void {
      if (this[ChanClosed]) return;
      this[ChanClosed] = true;

      // Notify all pending receivers that the channel is closed
      for (const receiver of this[ChanPendingReceivers]) {
        receiver.notify();
      }
      this[ChanPendingReceivers].length = 0;

      // Notify all pending senders that the channel is closed
      for (const sender of this[ChanPendingSenders]) {
        sender.notify();
      }
      this[ChanPendingSenders].length = 0;

      // Notify close listeners
      for (const fn of this[ChanCloseListeners]) {
        fn();
      }
      this[ChanCloseListeners].clear();
    },
  };

  return channel;
}
