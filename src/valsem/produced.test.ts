import { describe, it, expect } from 'vitest';
import { draft, intern, isCanonical, deepEqual } from 'valsem';
import { createSignal, computed } from '../signal.js';
import { produced } from './produced.js';

describe('produced', () => {
  it('returns a canonical value, so Object.is downstream is structural equality', () => {
    const todos = createSignal([{ id: 1, done: false }, { id: 2, done: true }]);
    const open = produced(() => todos().filter((t) => !t.done));
    const first = open();
    expect(isCanonical(first)).toBe(true);
    expect(first).toEqual([{ id: 1, done: false }]);

    // A structurally identical rebuild is the same instance: nobody downstream moves
    let downstream = 0;
    const count = computed(() => { downstream++; return open().length; });
    count.observe('value', () => {});
    expect(downstream).toBe(1);
    todos.set([{ id: 1, done: false }, { id: 2, done: true }, { id: 3, done: true }]);
    expect(open()).toBe(first);
    expect(downstream).toBe(1);
  });

  it('lets the recipe edit an input through draft() without touching the input', () => {
    // Canonical input: untouched material keeps its identity through structural sharing
    const order = createSignal(intern({ lines: [{ price: 2 }, { price: 3 }], total: 0 }));
    const totalled = produced(() => {
      const d = draft(order());
      d.total = d.lines.reduce((s, l) => s + l.price, 0);
      return d;
    });
    const result = totalled();
    expect(result.total).toBe(5);
    expect(order().total).toBe(0);
    expect(result.lines).toBe(order().lines);
    expect(isCanonical(result)).toBe(true);
  });

  it('accepts a custom equals like computed', () => {
    const a = createSignal(1);
    let evaluations = 0;
    const p = produced(() => { evaluations++; return { n: a() % 2 }; }, { equals: deepEqual });
    const seen: number[] = [];
    p.observe('value', (v) => seen.push(v.n));
    a.set(3);
    expect(seen).toEqual([1]);
    expect(evaluations).toBe(2);
  });
});
