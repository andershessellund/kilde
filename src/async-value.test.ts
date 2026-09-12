// ---------------------------------------------------------------------------
// AsyncValue — tests (pure data types)
// ---------------------------------------------------------------------------

import { describe, it, expect } from 'vitest';
import {
  unavailable,
  loading,
  available,
  errored,
  isUnavailable,
  isLoading,
  isAvailable,
  isErrored,
  valueOr,
  mapValue,
  combineValues,
} from './async-value.js';
import type { AsyncValue } from './async-value.js';

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

describe('AsyncValue constructors', () => {
  it('unavailable() returns frozen singleton', () => {
    const v = unavailable();
    expect(v.status).toBe('unavailable');
    expect(v).toBe(unavailable()); // same reference
    expect(Object.isFrozen(v)).toBe(true);
  });

  it('loading() without stale value', () => {
    const v = loading();
    expect(v.status).toBe('loading');
    expect('staleValue' in v).toBe(false);
  });

  it('loading() with stale value', () => {
    const v = loading(42);
    expect(v.status).toBe('loading');
    expect(v.staleValue).toBe(42);
  });

  it('loading() with explicit undefined does not set staleValue', () => {
    const v = loading(undefined);
    expect('staleValue' in v).toBe(false);
  });

  it('available() holds value', () => {
    const v = available('hello');
    expect(v.status).toBe('available');
    expect(v.value).toBe('hello');
  });

  it('errored() without stale value', () => {
    const err = new Error('boom');
    const v = errored(err);
    expect(v.status).toBe('errored');
    expect(v.error).toBe(err);
    expect('staleValue' in v).toBe(false);
  });

  it('errored() with stale value', () => {
    const err = new Error('boom');
    const v = errored(err, 99);
    expect(v.status).toBe('errored');
    expect(v.error).toBe(err);
    expect(v.staleValue).toBe(99);
  });
});

// ---------------------------------------------------------------------------
// Type guards
// ---------------------------------------------------------------------------

describe('AsyncValue type guards', () => {
  const cases: [AsyncValue<number>, string][] = [
    [unavailable(), 'unavailable'],
    [loading(), 'loading'],
    [loading(1), 'loading'],
    [available(1), 'available'],
    [errored('e'), 'errored'],
    [errored('e', 1), 'errored'],
  ];

  it.each(cases)('correctly identifies %o as %s', (v, expected) => {
    expect(isUnavailable(v)).toBe(expected === 'unavailable');
    expect(isLoading(v)).toBe(expected === 'loading');
    expect(isAvailable(v)).toBe(expected === 'available');
    expect(isErrored(v)).toBe(expected === 'errored');
  });
});

// ---------------------------------------------------------------------------
// valueOr
// ---------------------------------------------------------------------------

describe('valueOr', () => {
  it('returns value from available', () => {
    expect(valueOr(available(42), 0)).toBe(42);
  });

  it('returns staleValue from loading', () => {
    expect(valueOr(loading(42), 0)).toBe(42);
  });

  it('returns fallback from loading without stale', () => {
    expect(valueOr(loading(), 0)).toBe(0);
  });

  it('returns staleValue from errored', () => {
    expect(valueOr(errored('e', 42), 0)).toBe(42);
  });

  it('returns fallback from errored without stale', () => {
    expect(valueOr(errored('e'), 0)).toBe(0);
  });

  it('returns fallback from unavailable', () => {
    expect(valueOr(unavailable(), 0)).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// mapValue
// ---------------------------------------------------------------------------

describe('mapValue', () => {
  const double = (x: number) => x * 2;

  it('maps available value', () => {
    const v = mapValue(available(5), double);
    expect(v).toEqual(available(10));
  });

  it('maps stale value in loading', () => {
    const v = mapValue(loading(5), double);
    expect(v).toEqual(loading(10));
  });

  it('preserves loading without stale', () => {
    const v = mapValue(loading<number>(), double);
    expect(v.status).toBe('loading');
    expect('staleValue' in v).toBe(false);
  });

  it('maps stale value in errored', () => {
    const err = new Error('e');
    const v = mapValue(errored<number>(err, 5), double);
    expect(v.status).toBe('errored');
    expect((v as any).staleValue).toBe(10);
    expect((v as any).error).toBe(err);
  });

  it('preserves errored without stale', () => {
    const err = new Error('e');
    const v = mapValue(errored<number>(err), double);
    expect(v.status).toBe('errored');
    expect('staleValue' in v).toBe(false);
  });

  it('returns unavailable unchanged', () => {
    const v = mapValue(unavailable() as AsyncValue<number>, double);
    expect(v).toBe(unavailable());
  });
});

// ---------------------------------------------------------------------------
// combineValues
// ---------------------------------------------------------------------------

describe('combineValues', () => {
  it('combines all available into tuple', () => {
    const result = combineValues(available(1), available('a'), available(true));
    expect(result).toEqual(available([1, 'a', true]));
  });

  it('returns loading when any is loading', () => {
    const result = combineValues(available(1), loading<string>());
    expect(result.status).toBe('loading');
  });

  it('returns errored when any is errored (over loading)', () => {
    const err = new Error('fail');
    const result = combineValues(loading<number>(), errored<string>(err));
    expect(result.status).toBe('errored');
    expect((result as any).error).toBe(err);
  });

  it('returns unavailable over everything', () => {
    const err = new Error('fail');
    const result = combineValues(
      errored<number>(err),
      unavailable() as AsyncValue<string>,
      loading<boolean>(),
    );
    expect(result.status).toBe('unavailable');
  });

  it('handles single available input', () => {
    const result = combineValues(available(42));
    expect(result).toEqual(available([42]));
  });

  it('preserves first error when multiple errored', () => {
    const err1 = new Error('first');
    const err2 = new Error('second');
    const result = combineValues(errored<number>(err1), errored<string>(err2));
    expect(result.status).toBe('errored');
    expect((result as any).error).toBe(err1);
  });
});
