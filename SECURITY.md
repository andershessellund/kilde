# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately, through GitHub:
**[Report a vulnerability](https://github.com/andershessellund/kilde/security/advisories/new)**.
Do not open a public issue or pull request for one.

A useful report says what an attacker controls, what they gain, and how to
reproduce it, ideally as a short script against a published version.

kilde is maintained by one person. Reports are read and acknowledged as
soon as possible, and you will be told whether the report is accepted and what
the plan is. Fixes are released as a patch version with a GitHub security
advisory, crediting the reporter unless they prefer otherwise.

## Supported versions

Security fixes go into the latest published version only.

## What counts

kilde runs code its caller wrote: the functions passed to `computed`,
operators, sinks and sources are the caller's own, and the values that flow
through them are the caller's own values. It is not a boundary for untrusted
data. That leaves a small surface. In scope, for example:

- a compromise of the published package or of the release pipeline described
  below;
- kilde writing to a prototype, or to anything outside its own state, given
  values of any shape;
- a resource kilde has been told to dispose (a connection, a subscription, an
  owner's scope) staying live after the disposal completes.

Out of scope, by design:

- whatever the caller's own callbacks do;
- unbounded memory from a source the caller left unbounded (an unbounded
  channel buffer, a sink that never pauses): bounding it is the caller's
  choice;
- the admission of values in `kilde/valsem`, which is
  [`valsem`](https://github.com/andershessellund/valsem)'s, and covered by
  [its policy](https://github.com/andershessellund/valsem/blob/main/SECURITY.md).

## How releases are protected

Published versions are built and staged by GitHub Actions from a tagged commit,
authenticated to npm by OIDC trusted publishing: no npm token exists. A staged
version goes live only after the maintainer approves it with a second factor,
and carries a provenance attestation linking it to its source commit and
workflow run. Release tags cannot be moved or deleted.
