# kilde

**Signals, streams, and channels for JavaScript.** *Kilde* is Danish for
"source" — a spring, and the source of a river.

```bash
npm install kilde
```

Three reactive primitives that share one vocabulary, and are cheap to move
between:

- **Signals** hold a current value and recompute derived values on demand.
  Reads are synchronous, dependencies are tracked automatically, and a change
  only propagates when the value is actually different.
- **Streams** push values through operators with cooperative backpressure. A
  sink can say *pause*; the source waits. Everything is cold and ref-counted
  until something subscribes.
- **Channels** coordinate concurrent code the CSP way: `put`, `take`, and a
  `select` that waits on several things at once.

On top of those, an **async state** layer models loading, errors and stale
data as a value (`AsyncValue`) rather than as a pile of booleans, and a
small **ownership** protocol says who tears a hot resource down — your own
scope, a framework's, or nobody's.

kilde has no dependencies. Two optional entry points have an optional peer
each: `kilde/valsem` needs [valsem](https://github.com/andershessellund/valsem)
for value semantics, and `kilde/testing` needs
[stifinder](https://github.com/andershessellund/stifinder) for state-space
exploration. Install them only if you import those.

## Signals

```ts
import { createSignal, computed } from 'kilde';

const first = createSignal('Ada');
const last = createSignal('Lovelace');
const full = computed(() => `${first()} ${last()}`);

full(); // 'Ada Lovelace'

first.set('Augusta');
full(); // 'Augusta Lovelace'
```

A signal is a function: call it to read. Reading inside `computed` registers a
dependency, so the graph builds itself. `set` and `update` write; a write of
the same value (`Object.is`) is dropped and nothing downstream runs.

`Object.is` is the default because it is O(1), predictable, and what every
other signal system uses. When you want structural equality, either pass
`{ equals }` with any predicate to `createSignal`, `computed`, `toSignal`,
`createStore` and `linkedSignal`, or produce canonical values, for which
`Object.is` already *is* structural equality. That is what the
`kilde/valsem` entry point is for (see below). One consequence to know:
a `toSignal` over a stream that rebuilds its value, a `scan` that spreads
or a `combineLatest` tuple, sees a new identity on every emission, so pass
`equals` there if the rebuilt value is often unchanged.

Observe changes with `observe`. The callback receives the current value at
once, then every change:

```ts
const stop = full.observe('value', (name) => document.title = name);
stop();
```

Delivery is scheduled. The default scheduler is immediate; pass
`microtaskScheduler` to coalesce a burst of writes into one notification, or
`animationFrameScheduler` to render at most once per frame:

```ts
import { microtaskScheduler } from 'kilde';

count.observe('value', render, microtaskScheduler);
count.set(1); count.set(2); count.set(3); // render runs once, with 3
```

Signals know whether anyone is watching. `signal.observed` is true while an
observer exists, and the `'activate'` and `'deactivate'` events fire on the
transitions. A plain read never counts: `computed(() => a() * 2)()` pulls the
value without registering anything, so `a` stays unobserved and an expensive
source behind `a` stays cold. Only `observe('value', ...)`, a connected
stream, or a computed that is itself observed, makes a signal observed. That
is what lets expensive sources stay cold until they are needed, and it also
means a computed you create and read once is not retained by its
dependencies.

If an observer callback throws, the other observers still receive the update;
the exception (or an `AggregateError` if several threw) is rethrown from the
`set` that triggered delivery, or from the scheduler callback.

A few more forms are worth knowing:

- `linkedSignal(prev => ...)` is a writable signal with a derivation: it
  follows its dependencies until you write to it, and resumes following when
  they change again. Good for "selected item" state that must stay valid as
  the list changes. `link(signal, fn)` attaches the same behaviour to an
  existing writable signal.
- `untracked(() => ...)` reads signals without registering dependencies.
- `track(fn)` is the building block under `computed`: a tracking node with
  an explicit dependent protocol, for code that integrates its own
  scheduler or reconciler.
- `observe('read', fn)` installs a hook that runs on every read of a signal.
  It is what `link` uses to pull a derivation through; you rarely need it.

## Streams

```ts
import { stream, fromArray, map, filter, reduce } from 'kilde';

stream(
  fromArray([1, 2, 3, 4]),
  filter((n) => n % 2 === 0),
  map((n) => n * 10),
  reduce((a, b) => a + b, 0),
); // 60
```

`stream(source, ...operators)` composes, connects, and returns the one
synchronous result. The pipeline must end in something that emits exactly one
value and completes synchronously, such as `toArray`, `reduce`, or one of the
bridges below; otherwise `stream` throws and releases the connection.
`pipe(source, ...operators)` composes without connecting and hands back a
`Source` for later. Operators are plain functions from `Source<T>` to
`Source<R>`, so writing your own needs no base class. Everything is also
available on one object, `Stream.map`, `Stream.fromArray` and so on, for code
that prefers a namespace to named imports.

The protocol underneath is small. A `Source<T>` has `connect(sink)`, which
returns a paused `Stream`; `resume()` starts delivery. The sink's `next(value)`
may return `PAUSE`, and the source then stops until the next `resume()`. That
one return value is the whole backpressure story: async iterables, Web
streams and Node streams all map onto it without buffering in between.

The rules in full: nothing reaches the sink before the first `resume()`;
after `PAUSE`, no `next()` until the next `resume()`; `complete()` and
`error()` are not held back by `PAUSE` and may arrive while the sink is
paused; after either of them nothing further arrives and `resume()` is a
no-op. Operators that buffer for a paused consumer deliver the terminal
event only after the buffer has drained. These are exactly the rules that `assertProtocol()` from
`kilde/testing` checks; put it after your own operator in a test.

```ts
import { stream, fromReadableStream, lines, toAsyncIterable } from 'kilde';

const body = stream(fromReadableStream(response.body!), lines(), toAsyncIterable());
for await (const line of body) {
  // the response is only read as fast as this loop runs
}
```

Bridges out of a pipeline: `toPromise()` (first value), `toCallback(fn)`
(promise that settles on completion), `toAsyncIterable()`, `toArray()`,
`toReadableStream()`, `intoWritableStream(w)`, `toSignal({ initial })` and
`toAsyncSignal()`. Bridges in: `fromArray`, `of`, `fromIterator`, `fromPromise`,
`fromAsyncFn`, `fromReadableStream`, `fromSignal`, `fromChannel`, and
`deferred()` — a source you resolve by hand.

Operators: `map`, `filter`, `take`, `scan`, `reduce`, `flatten`, `merge`,
`switchMap`, `combineLatest`, `catchError`, `pausable`, `scheduleOn`, `lines`.
Compose several into one with `comp(name, ...ops)`. `flatten` runs inner
sources one at a time; `merge` runs them concurrently, so values from
different inners may interleave while each inner's own order is kept.

`createRelay<T>()` is both a source and a sink: push with `next`, and every
connected subscriber receives the value through its own pausable buffer, so a
slow consumer never stalls a fast one.

## Channels

```ts
import { createChannel, put, channelTake, select, timeout, closed } from 'kilde';

const jobs = createChannel<Job>(8); // buffer of 8; 0 = rendezvous

// producer
await put(jobs, job);

// consumer
const result = await select({
  job: channelTake(jobs),
  idle: timeout(5_000),
  done: closed(jobs),
});
switch (result.tag) {
  case 'job':  handle(result.value); break;
  case 'idle': log('nothing for five seconds'); break;
  case 'done': return;
}
```

`select` takes an object; the keys become the `tag` of the result, and the
first choice that can proceed wins (ties go to key order). A branch may be a
single choice or an array of choices for fan-in. Choices are awaitable on
their own too: `await channelTake(ch)` gives the value directly. A `timeout`
holds a timer only while a `select` is waiting on it, so a loop that creates
one per iteration leaves nothing behind.

Choices: `channelTake(ch)` (named so it does not collide with the stream
operator `take`), `put(ch, value)`, `closed(ch)`, `timeout(ms)`,
`resolved(promise)`, `rejected(promise)`, `defaultChoice()` for a
non-blocking select. Buffers: a size for a fixed buffer, `droppingBuffer(n)`,
`slidingBuffer(n)`, `unboundedBuffer()`.

`ReadChannel<T>` and `WriteChannel<T>` are separate views, so an API can hand
out only the half a caller should have. `intoChannel(ch)` pipes a stream into
a channel and `fromChannel(ch)` reads one out.

## Async state

An `AsyncValue<T>` is one of `unavailable`, `loading`, `available(value)`, or
`errored(error)`. The non-available states can carry a `staleValue`, so a UI
can keep showing the last good result while a refresh is in flight. Every
async signal compares envelopes with `asyncValueEquals`: same status, same
payload (`Object.is`, or the `equals` option where a signal owns a payload),
same error; `loading()` after `loading()` notifies nobody, and
`combineAsync` compares its tuple element by element.

```ts
import {
  createSignal, createAsyncSignal, alwaysAvailable, switchMapAsync, computedAsync, isAvailable,
} from 'kilde';

const userId = createSignal(1);

// Cold: fetches when first observed, refetches on retry() or reload(),
// drops the request when the last observer leaves.
const user = switchMapAsync(alwaysAvailable(userId), (id) =>
  createAsyncSignal(() => api.user(id)),
);

const orders = createAsyncSignal(() => api.orders());

const summary = computedAsync([user, orders], async (u, o) => ({
  name: u.name,
  open: o.filter((x) => x.userId === u.id).length,
}));

summary.observe('value', (v) => {
  if (isAvailable(v)) render(v.value);
});
```

The algebra on values: `mapValue`, `combineValues`, `valueOr`, and the
guards `isLoading`, `isAvailable`, `isErrored`, `isUnavailable`. On signals:
`mapAsync`, `combineAsync`, `switchMapAsync`, `computedAsync`,
`alwaysAvailable`, `toAsyncSignal`. An `AsyncSignal` has `retry()`, which
chains upstream; signals that own work also have `reload()`.

`deriveResource` turns an async signal into a **hot** managed resource: it
runs a factory whenever the input becomes available, disposes the previous
result before installing the next, and tears itself down through its owner.

```ts
const db = deriveResource(config, async (cfg) => {
  const pool = await connect(cfg);
  return { value: pool, [Symbol.dispose]: () => pool.close() };
});
```

## Value semantics with valsem

```ts
import { produced } from 'kilde/valsem';
import { draft } from 'valsem';

const open = produced(() => todos().filter((t) => !t.done));

const totalled = produced(() => {
  const order = draft(currentOrder());
  order.total = order.lines.reduce((sum, l) => sum + l.price, 0);
  return order;
});
```

`produced` is a `computed` whose result is a canonical
[valsem](https://github.com/andershessellund/valsem) value: structurally
equal results are the same instance, so the `Object.is` default already
deduplicates them and everything downstream. Reads inside the recipe are
plain frozen values at native speed; call `draft()` only on the inputs you
want to edit with mutable syntax, and untouched parts keep their identity.
`SignalDeduplicator`, a keyed cache of signals with structural keys that
evicts entries on `'deactivate'`, lives here too. `kilde/valsem` needs
valsem installed; the core does not.

## Stores

`createStore(initial)` is a writable signal with a lifecycle: disposing it
completes its subscribers and rejects anything still feeding it.
`intoStore(store, reducer)` folds a stream into it.

## Ownership

Almost everything in kilde cleans up after itself: a computed with no
observers costs nothing, a cold source disconnects when its last subscriber
leaves. A handful of constructs are **hot** — they connect immediately and
stay connected until disposed:

`connect()`, `toPromise()`, `toCallback()`, `toAsyncIterable()`,
`toAsyncSignal({ hot: true })`, `link()`, `deriveResource()`.

Each of those hands its teardown to an `Owner`. Without one, you hold the
handle yourself, exactly as with a subscription in any other library. With
one, disposal is structured:

```ts
import { createOwner, withOwner, stream, toPromise } from 'kilde';

const page = createOwner('page');

const first = withOwner(page, () => stream(clicks, toPromise()));

await page.dispose(); // `first` rejects with StreamDisposedError
```

The owner is resolved in this order: an explicit `{ owner }` option on the
call, the innermost `withOwner` scope on the synchronous call stack, the
process-wide provider installed by `installOwnerProvider`, and finally no
owner at all. `withOwner` is synchronous and does not survive an `await`;
pass `{ owner }` explicitly after one.

`createOwner()` gives you a plain scope that disposes its resources in
reverse order and implements `Disposable`, so `using page = createOwner()`
works. `installOwnerProvider` is the integration seam: a framework or a
dependency-injection container that already has a notion of "current scope"
installs a provider once, and every hot construct in the process attaches to
the right scope automatically. An owner may also implement `spawn`, in which
case `deriveResource` runs its async work under the owner's supervision
(useful for drain-on-shutdown and cancellation).

## Node.js

```ts
import { fromReadable, toReadable, intoWritable } from 'kilde/node';
```

Bridges between kilde sources and Node's `Readable` and `Writable`, with
backpressure mapped both ways.

## Testing

```ts
import { testSource, testSink, exhaustiveTest } from 'kilde/testing';

await exhaustiveTest((oracle) => {
  const sink = testSink<number>({ oracle });
  const s = pipe(testSource([1, 2, 3], { oracle }), myOperator(), assertProtocol()).connect(sink);
  s.resume();
  while (sink.completeCount === 0) s.resume();
  expect(sink.values).toEqual([1, 2, 3]);
});
```

`testSource` and `testSink` consult a decision oracle at every point where
they could pause, resume, or deliver, including whether completion arrives
while the sink is paused; `exhaustiveTest` explores every decision sequence
with [stifinder](https://github.com/andershessellund/stifinder), fewest
departures from the plain schedule first, and reports the smallest failing
one with each departure named: "sink pauses after value #2", "source
completes while the sink is paused". A space too large to exhaust can be
bounded with `{ maxDeviations }`; the statistics it resolves with say whether
the search was `exhaustive`. `assertProtocol()` is an operator that throws on any breach of the
stream protocol. Put it after the operator under test and every
interleaving becomes a conformance check. If an operator has an ordering
bug, this finds it.

## Guarantees and requirements

- Signals never deliver the same value (`Object.is`, or your `equals`) twice
  in a row, and the async layer never delivers a redundant envelope.
- Cold sources do no work until connected, and release everything when the
  last connection is disposed.
- `PAUSE` is honoured by every built-in source and operator. A relay buffers
  per subscriber so a paused consumer does not stall the others.
- A terminal event may arrive while a consumer is paused, and never after
  another terminal event. Every built-in bridge is written to expect that.
- Hot constructs registered with an owner are torn down exactly once, and any
  promise waiting on them rejects with `StreamDisposedError`.

Runtime: Node.js 22 or later and current browsers. kilde uses the standard
`Symbol.dispose` and `Symbol.asyncDispose` and ships ES modules only.

kilde is pre-1.0. The shape is stable enough to build on; names may still
move.

## License

Apache-2.0
