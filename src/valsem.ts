// ---------------------------------------------------------------------------
// kilde/valsem — value semantics for kilde
//
// Requires the optional peer dependency `valsem`. Nothing in the core entry
// point imports it: kilde compares with `Object.is` everywhere, which for
// canonical values already is structural equality. This entry point is
// where valsem-specific conveniences live.
// ---------------------------------------------------------------------------

export { produced } from './valsem/produced.js';
export { SignalDeduplicator } from './valsem/signal-deduplicator.js';
export type { SignalDeduplicatorOptions } from './valsem/signal-deduplicator.js';
