// ---------------------------------------------------------------------------
// AsyncValue<T> — tagged union for asynchronous state
//
// Four states:
//   - unavailable: value not available (e.g. awaiting user input)
//   - loading:     value is being fetched/computed (optional stale value)
//   - available:   value is fresh and ready
//   - errored:     an unrecoverable error occurred (optional stale value)
//
// These are pure data types — no signals, no reactivity.
// Use with Signal<AsyncValue<T>> (aliased as AsyncState<T>) for reactivity.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// State types
// ---------------------------------------------------------------------------

/** Value is not available — no active subscription or required input missing. May carry stale data. */
export interface Unavailable<T = never> {
  readonly status: 'unavailable';
  readonly staleValue?: T;
}

/** Value is being fetched or computed. May have a stale previous value. */
export interface Loading<T> {
  readonly status: 'loading';
  readonly staleValue?: T;
}

/** Value is available and fresh. */
export interface Available<T> {
  readonly status: 'available';
  readonly value: T;
}

/** An error occurred. May have a stale previous value. */
export interface Errored<T> {
  readonly status: 'errored';
  readonly error: unknown;
  readonly staleValue?: T;
}

/**
 * Tagged union representing the state of an asynchronous value.
 *
 * ```ts
 * const state: AsyncValue<User> = { status: 'available', value: user };
 * ```
 */
export type AsyncValue<T> = Unavailable<T> | Loading<T> | Available<T> | Errored<T>;

// ---------------------------------------------------------------------------
// Constructors — concise factory functions
// ---------------------------------------------------------------------------

const UNAVAILABLE: Unavailable = Object.freeze({ status: 'unavailable' });

/** Create an `unavailable` value, optionally carrying a stale previous value. */
export function unavailable<T = never>(staleValue?: T): Unavailable<T> {
  return staleValue !== undefined
    ? { status: 'unavailable', staleValue }
    : (UNAVAILABLE as Unavailable<T>);
}

/** Create a `loading` value, optionally carrying a stale previous value. */
export function loading<T>(staleValue?: T): Loading<T> {
  return staleValue !== undefined ? { status: 'loading', staleValue } : { status: 'loading' };
}

/** Create an `available` value. */
export function available<T>(value: T): Available<T> {
  return { status: 'available', value };
}

/** Create an `errored` value, optionally carrying a stale previous value. */
export function errored<T>(error: unknown, staleValue?: T): Errored<T> {
  return staleValue !== undefined
    ? { status: 'errored', error, staleValue }
    : { status: 'errored', error };
}

// ---------------------------------------------------------------------------
// Type guards
// ---------------------------------------------------------------------------

/** Check if the value is `unavailable`. */
export function isUnavailable<T>(v: AsyncValue<T>): v is Unavailable<T> {
  return v.status === 'unavailable';
}

/** Check if the value is `loading`. */
export function isLoading<T>(v: AsyncValue<T>): v is Loading<T> {
  return v.status === 'loading';
}

/** Check if the value is `available`. */
export function isAvailable<T>(v: AsyncValue<T>): v is Available<T> {
  return v.status === 'available';
}

/** Check if the value is `errored`. */
export function isErrored<T>(v: AsyncValue<T>): v is Errored<T> {
  return v.status === 'errored';
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

/**
 * Extract the current or stale value, or return a fallback.
 *
 * Returns the `value` if available, `staleValue` if loading or errored,
 * or the fallback otherwise.
 */
export function valueOr<T>(v: AsyncValue<T>, fallback: T): T {
  switch (v.status) {
    case 'available':
      return v.value;
    case 'loading':
      return v.staleValue !== undefined ? v.staleValue : fallback;
    case 'errored':
      return v.staleValue !== undefined ? v.staleValue : fallback;
    case 'unavailable':
      return v.staleValue !== undefined ? v.staleValue : fallback;
  }
}

/**
 * Map the inner value of an `AsyncValue`. Preserves the status and stale values.
 *
 * ```ts
 * mapValue(available(1), x => x * 2) // available(2)
 * mapValue(loading(1), x => x * 2)   // loading(2)
 * mapValue(unavailable(), x => x)     // unavailable()
 * ```
 */
export function mapValue<T, U>(v: AsyncValue<T>, fn: (value: T) => U): AsyncValue<U> {
  switch (v.status) {
    case 'available':
      return available(fn(v.value));
    case 'loading':
      return v.staleValue !== undefined ? loading(fn(v.staleValue)) : loading<U>();
    case 'errored':
      return v.staleValue !== undefined
        ? errored<U>(v.error, fn(v.staleValue))
        : errored<U>(v.error);
    case 'unavailable':
      return v.staleValue !== undefined ? unavailable(fn(v.staleValue)) : (UNAVAILABLE as Unavailable<U>);
  }
}

// ---------------------------------------------------------------------------
// Combinators — pure value-level composition
// ---------------------------------------------------------------------------

/**
 * Combine multiple `AsyncValue`s into one, with precedence:
 *   unavailable > errored > loading > available
 *
 * If all inputs are `available`, returns `available` with a tuple of values.
 * Otherwise, returns the highest-precedence non-available state.
 *
 * Stale values are carried through: if a previous combined result is provided,
 * it becomes the stale value for `loading` or `errored` outputs.
 *
 * ```ts
 * combineValues(available(1), available('a'))         // available([1, 'a'])
 * combineValues(available(1), loading())              // loading()
 * combineValues(available(1), errored(new Error())) // errored(Error)
 * combineValues(unavailable(), errored(new Error())) // unavailable()
 * ```
 */
export function combineValues<T extends readonly unknown[]>(
  ...values: { readonly [K in keyof T]: AsyncValue<T[K]> }
): AsyncValue<T> {
  let worstStatus: 'available' | 'loading' | 'errored' | 'unavailable' = 'available';
  let firstError: unknown;

  for (const v of values) {
    switch (v.status) {
      case 'unavailable':
        worstStatus = 'unavailable';
        break;
      case 'errored':
        if (worstStatus !== 'unavailable') {
          if (worstStatus !== 'errored') firstError = v.error;
          worstStatus = 'errored';
        }
        break;
      case 'loading':
        if (worstStatus === 'available') worstStatus = 'loading';
        break;
      case 'available':
        break;
    }
  }

  switch (worstStatus) {
    case 'unavailable':
      return unavailable<T>();
    case 'errored':
      return errored<T>(firstError!);
    case 'loading':
      return loading<T>();
    case 'available':
      return available(values.map((v) => (v as Available<unknown>).value) as unknown as T);
  }
}
