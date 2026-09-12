/**
 * Error signalling that a stream was disposed externally
 * (e.g., because its owner was disposed).
 *
 * Edge operators like `toPromise()` and `toAsyncIterable()` reject with
 * this error when their owner is torn down before the stream
 * completes naturally.
 */
export class StreamDisposedError extends Error {
  constructor() {
    super('Stream disposed by its owner');
    this.name = 'StreamDisposedError';
  }
}
