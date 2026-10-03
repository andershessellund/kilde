# Experiment: running kilde's exhaustive tests on stifinder

*2026-09-13. Working tree on top of commit 052d62c.*

## Outcome

Decided after the experiment: stifinder (published, `^0.0.1`) is an
optional peer dependency required by `kilde/testing`; the depth-first
explorer and the `KILDE_EXPLORER` and `KILDE_EXPLORER_STATS` switches were
removed, so the "Reproduce" section below describes the tree as it was
during the experiment; the deviation and edge budgets default to unbounded.
A later round removed valsem from the core entirely (it is now an optional
peer of `kilde/valsem`, at 0.0.4), so the valsem statements below are
historical too. In stifinder 0.2.0 the adapter described below moved into
stifinder as `decisionModel` (its decision D36), with two of its defects
fixed: a body that threw before its first decision passed, and a body's
promise was not awaited. `src/testing/explore.ts` is now a call of
`check(decisionModel(body))`.

## Question

kilde's operator tests enumerate every pause/resume/terminal interleaving
of a test body through a `DecisionOracle`. The existing explorer is a
depth-first walk with a mixed-radix counter (`exhaustiveTest`). Could
stifinder, a budget-bounded, delay-bounding state-space explorer, replace
it, and what would it cost? The state under stifinder is necessarily the
sequence of decisions made so far (no collapsing), so the expectation was
"same coverage, somewhat slower, better counterexamples".

## What was built

`src/testing/explore.ts` adapts a `DecisionOracle` test body to stifinder:

- **State** is the array of picks made so far. **Event** is the next pick,
  offered in preference order with 0 first, so every non-zero pick is one
  deviation.
- **`applyEvent(prefix, k)`** runs the body with an oracle that replays
  `prefix + [k]` and answers 0 to every later decision point, recording
  each range it meets. One run therefore harvests the ranges of every state
  along its default continuation. A run that throws attributes the error to
  the edge into the deepest state it reached.
- Runs are in bijection with leaves of the decision tree (leaf ↔ its picks
  with trailing zeros stripped), so the number of body executions is
  exactly the depth-first explorer's count. The difference in cost is pure
  bookkeeping.

`exhaustiveTest` is now `async`, returns `ExploreTestStats`, and selects
the explorer with `KILDE_EXPLORER` (`stifinder`, the default, or `dfs`).
`KILDE_EXPLORER_STATS=<file>` appends one JSON line per call. All 92 call
sites in the suite were converted to `await`.

Dependencies: valsem was raised to ^0.0.3 (stifinder's floor) and stifinder
was added as `file:../stifinder`. Both share kilde's valsem instance.

## Results

### Parity

| | dfs | stifinder |
|---|---|---|
| Test files / tests | 48 / 811 pass | 48 / 811 pass |
| `exhaustiveTest` calls | 92 | 92 |
| Body runs | 2 287 | 2 287 (identical per call, 92/92) |
| States (decision prefixes) | | 4 480 |
| Edges computed | | 4 388 |
| Deviations needed to exhaust | | median 3, max 8 |

Every test passes under both, and every call executes the body the same
number of times.

### Cost on the real suite (single-threaded, per-call timers)

| | dfs | stifinder | ratio |
|---|---|---|---|
| Total across 92 calls | 76.9 ms | 152.5 ms | 1.98 |
| Per body run | 33.6 µs | 66.7 µs | |
| Per edge | | 34.8 µs | |
| Per-call ratio (calls over 0.2 ms) | | median 1.99, p90 2.44, max 3.40 | |
| Largest call (`pausable`, 243 runs, 485 states) | 5.2 ms | 10.4 ms | 2.00 |

The whole suite wall time is unchanged at about 1.3 s; the exhaustive
subset is a small fraction of it under either explorer.

### Pure explorer overhead (body is only decisions, no pipeline)

| depth | leaves | dfs µs/run | stifinder µs/edge | ratio |
|---|---|---|---|---|
| 4 | 16 | 1.0 | 12.3 | 22 |
| 8 | 256 | 0.6 | 6.1 | 22 |
| 12 | 4 096 | 0.9 | 5.3 | 11.5 |

stifinder's fixed cost is roughly 5 µs per edge: interning the prefix,
the cache lookup, the frontier update, and two promise hops. The depth-first
loop costs about 1 µs per run. On the real suite that fixed cost is
amortised against a body that costs 30 µs to run, which is why the observed
ratio is 2 rather than 10.

### Scaling on one body (`map` then `pausable` through `assertProtocol`)

| n values | runs | stifinder states | dfs ms | stifinder ms | ratio | stifinder, budget 2 deviations |
|---|---|---|---|---|---|---|
| 3 | 27 | 53 | 0.2 | 0.9 | 5.4 | 36 edges, 0.5 ms |
| 5 | 243 | 485 | 0.7 | 4.7 | 6.4 | 148 edges, 0.9 ms |
| 7 | 2 187 | 4 373 | 5.4 | 29.3 | 5.5 | 404 edges, 1.9 ms |

Both are exponential in n, as expected with no collapsing. The last column
is what depth-first enumeration cannot offer: at n = 7 a budget of two
deviations explores 9% of the edges in 6% of the time, in the order most
likely to find a bug.

### Counterexample quality

Three bugs, two of them the real ones fixed earlier in this tree (taken
from commit 052d62c), and one synthetic bug that needs two specific pauses
twelve decisions deep.

| Bug | dfs | stifinder |
|---|---|---|
| Old `lines()` never completes after trailing data | run 1, `[0/2, 0/2, 0/2]` | 0 deviations, 1 run |
| Old `catchError()` pushes fallback into a paused sink | run 4, `[0/2, 0/2, 1/2]` | 1 deviation `[1]`, 5 runs |
| Synthetic: drops a value iff paused on the 2nd and 4th values | run 1 621, 12 decisions | 2 deviations, 115 runs, same trace |

For zero- and one-deviation bugs the explorers are equivalent. For the
two-deviation bug stifinder reaches the same trace in 7% of the runs, and
its report says why the trace matters ("2 deviations") rather than which
permutation number it was. Depth-first order happened to find the
lexicographically first failure, which coincided with the minimal one here;
that is not guaranteed in general, whereas stifinder's minimality is.

## Assessment

- **Coverage is identical and cost is 2× on the real suite**, which is
  about 75 ms across the whole suite. Performance is not a reason to
  choose either way.
- **Bounded exploration and ranked counterexamples are real gains.** They
  matter most for the tests that do not exist yet: relay-driven and
  asynchronous models with larger spaces, where exhaustion is not an
  option and "fewest deviations" is the counterexample a reader wants.
- **No collapsing was attempted, and none is needed at these sizes.** The
  state count is about twice the run count. Collapsing would only pay once
  operators expose a canonical `inspect()` snapshot, which is a separate
  design decision tied to the inspectability goal.
- **The harvest trick matters.** Letting each run continue on the default
  schedule and recording every range it meets is what keeps body executions
  equal to the depth-first count. A naive adapter that suspends at the first
  new decision point would run the body once per edge instead, doubling the
  cost again.

## Consequences to decide

1. **`kilde/testing` now imports stifinder.** That entry point is published,
   so stifinder must become a real dependency (or an optional peer) and be
   published to npm before kilde can be. Today it is a `file:` link.
2. **Default explorer.** stifinder is the default in this tree; the
   depth-first explorer remains behind `KILDE_EXPLORER=dfs`. Keeping both
   costs one small file; dropping the depth-first one removes the
   synchronous variant of `exhaustiveTest`, which some downstream user may
   prefer for its simplicity.
3. **Deviation budget in CI.** With `maxDeviations` defaulting to 100 the
   suite exhausts every space. A default of, say, 4 would cap the cost of
   future large models while still finding everything the current suite
   finds (max needed today: 8, but all bugs seen so far needed 0–2).
4. **valsem 0.0.3** is now required by kilde itself, not only by the tests.

## Reproduce

```sh
KILDE_EXPLORER=dfs       KILDE_EXPLORER_STATS=/tmp/dfs.jsonl  npx vitest run --no-file-parallelism
KILDE_EXPLORER=stifinder KILDE_EXPLORER_STATS=/tmp/stif.jsonl npx vitest run --no-file-parallelism
```

The scaling, overhead and counterexample scripts live outside the repo in
the session scratchpad (`exp/scaling.test.ts`, `exp/overhead.test.ts`,
`exp/counterexample.test.ts`).
