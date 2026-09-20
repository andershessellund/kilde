// ---------------------------------------------------------------------------
// kilde/testing — helpers for testing sources, sinks, and operators
//
// testSource / testSink take a DecisionOracle so a test can be run across
// every interleaving of pause/resume decisions with exhaustiveTest(), which
// explores the decision space with stifinder, fewest deviations first.
// assertProtocol() checks the stream protocol between a source and its sink.
// ---------------------------------------------------------------------------

export { testSource } from './testing/test-source.js';
export type { TestSourceOptions } from './testing/test-source.js';
export { testSink } from './testing/test-sink.js';
export type { TestSink, TestSinkOptions } from './testing/test-sink.js';
export { exhaustiveTest } from './testing/exhaustive.js';
export { exploreTest } from './testing/explore.js';
export type { ExploreTestOptions, ExploreTestStats } from './testing/explore.js';
export type { DecisionOracle, DecisionLabel } from './testing/oracle.js';
export { assertProtocol, ProtocolViolationError } from './testing/protocol.js';
