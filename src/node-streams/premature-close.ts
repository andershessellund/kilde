// ---------------------------------------------------------------------------
// PrematureCloseError — a Node stream closed before it ended/finished
//
// Mirrors Node's own ERR_STREAM_PREMATURE_CLOSE (as raised by
// stream.finished()/pipeline()) so callers can match on `code`.
// ---------------------------------------------------------------------------

/** Thrown when a Node stream emits `'close'` before `'end'` / `'finish'`. */
export class PrematureCloseError extends Error {
  readonly code = 'ERR_STREAM_PREMATURE_CLOSE';

  constructor(message = 'Premature close') {
    super(message);
    this.name = 'PrematureCloseError';
  }
}
