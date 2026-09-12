// ---------------------------------------------------------------------------
// stream(), pipe(), comp() — the three entry points for stream composition
//
// stream(source, ...ops) — apply operators, connect, resume, extract one
//                          synchronous value. The final source must emit
//                          exactly one value and complete synchronously;
//                          otherwise stream() throws and releases the
//                          connection.
//
// pipe(source, ...ops)   — pure lazy composition. Returns Source<R>.
//                          No connect, no consume.
//
// comp(name, ...ops)     — compose multiple operators into a single, named
//                          operator.
// ---------------------------------------------------------------------------

import type { Source, Sink, Operator } from './types.js';

// ---------------------------------------------------------------------------
// FinalReceiver — extracts one value synchronously
// ---------------------------------------------------------------------------

class FinalReceiver<T> implements Sink<T> {
  value: T | undefined;
  valueCount = 0;
  completed = false;
  failed = false;
  receivedError: unknown;

  next(value: T): undefined {
    this.value = value;
    this.valueCount++;
    return undefined;
  }

  complete(): void {
    this.completed = true;
  }

  error(error: unknown): void {
    this.failed = true;
    this.receivedError = error;
  }
}

// ---------------------------------------------------------------------------
// stream() — compose + connect + extract
// ---------------------------------------------------------------------------

export function stream<T>(source: Source<T>): T;
export function stream<T, R1>(source: Source<T>, op1: Operator<T, R1>): R1;
export function stream<T, T1, R2>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, R2>,
): R2;
export function stream<T, T1, T2, R3>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, R3>,
): R3;
export function stream<T, T1, T2, T3, R4>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, R4>,
): R4;
export function stream<T, T1, T2, T3, T4, R5>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, R5>,
): R5;
export function stream<T, T1, T2, T3, T4, T5, R6>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, R6>,
): R6;
export function stream<T, T1, T2, T3, T4, T5, T6, R7>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, T6>,
  op7: Operator<T6, R7>,
): R7;
export function stream<T, T1, T2, T3, T4, T5, T6, T7, R8>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, T6>,
  op7: Operator<T6, T7>,
  op8: Operator<T7, R8>,
): R8;
export function stream<T, T1, T2, T3, T4, T5, T6, T7, T8, R9>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, T6>,
  op7: Operator<T6, T7>,
  op8: Operator<T7, T8>,
  op9: Operator<T8, R9>,
): R9;
export function stream<T, T1, T2, T3, T4, T5, T6, T7, T8, T9, R10>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, T6>,
  op7: Operator<T6, T7>,
  op8: Operator<T7, T8>,
  op9: Operator<T8, T9>,
  op10: Operator<T9, R10>,
): R10;
export function stream(source: Source<any>, ...operators: Operator<any, any>[]): any;
export function stream(source: Source<any>, ...operators: Operator<any, any>[]): any {
  // Apply operators to produce the final source
  let s: Source<any> = source;
  for (const op of operators) {
    s = op(s);
  }

  // Connect and extract. The final source must emit exactly one value and
  // complete, all synchronously inside resume(). Anything else is a
  // programming error: the connection is released and an Error is thrown.
  const receiver = new FinalReceiver();
  const connection = s.connect(receiver);
  try {
    connection.resume();
  } catch (err) {
    connection[Symbol.dispose]();
    throw err;
  }

  if (receiver.failed) {
    connection[Symbol.dispose]();
    throw receiver.receivedError;
  }

  if (receiver.valueCount !== 1 || !receiver.completed) {
    connection[Symbol.dispose]();
    if (receiver.valueCount === 0) {
      throw new Error('stream(): source did not emit a value synchronously');
    }
    if (receiver.valueCount > 1) {
      throw new Error(
        `stream(): source emitted ${receiver.valueCount} values; ` +
          'end the pipeline with an operator that produces one (toArray, reduce, toPromise, ...)',
      );
    }
    throw new Error('stream(): source emitted a value but did not complete synchronously');
  }

  return receiver.value;
}

// ---------------------------------------------------------------------------
// pipe() — pure lazy composition, always returns Source<R>
// ---------------------------------------------------------------------------

export function pipe<T>(source: Source<T>): Source<T>;
export function pipe<T, R1>(source: Source<T>, op1: Operator<T, R1>): Source<R1>;
export function pipe<T, T1, R2>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, R2>,
): Source<R2>;
export function pipe<T, T1, T2, R3>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, R3>,
): Source<R3>;
export function pipe<T, T1, T2, T3, R4>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, R4>,
): Source<R4>;
export function pipe<T, T1, T2, T3, T4, R5>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, R5>,
): Source<R5>;
export function pipe<T, T1, T2, T3, T4, T5, R6>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, R6>,
): Source<R6>;
export function pipe<T, T1, T2, T3, T4, T5, T6, R7>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, T6>,
  op7: Operator<T6, R7>,
): Source<R7>;
export function pipe<T, T1, T2, T3, T4, T5, T6, T7, R8>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, T6>,
  op7: Operator<T6, T7>,
  op8: Operator<T7, R8>,
): Source<R8>;
export function pipe<T, T1, T2, T3, T4, T5, T6, T7, T8, R9>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, T6>,
  op7: Operator<T6, T7>,
  op8: Operator<T7, T8>,
  op9: Operator<T8, R9>,
): Source<R9>;
export function pipe<T, T1, T2, T3, T4, T5, T6, T7, T8, T9, R10>(
  source: Source<T>,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, T6>,
  op7: Operator<T6, T7>,
  op8: Operator<T7, T8>,
  op9: Operator<T8, T9>,
  op10: Operator<T9, R10>,
): Source<R10>;
export function pipe(source: Source<any>, ...operators: Operator<any, any>[]): Source<any>;
export function pipe(source: Source<any>, ...operators: Operator<any, any>[]): Source<any> {
  let s: Source<any> = source;
  for (const op of operators) {
    s = op(s);
  }
  return s;
}

// ---------------------------------------------------------------------------
// comp() — compose operators into a single operator
// ---------------------------------------------------------------------------

export function comp<T, R1>(name: string, op1: Operator<T, R1>): Operator<T, R1>;
export function comp<T, T1, R2>(
  name: string,
  op1: Operator<T, T1>,
  op2: Operator<T1, R2>,
): Operator<T, R2>;
export function comp<T, T1, T2, R3>(
  name: string,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, R3>,
): Operator<T, R3>;
export function comp<T, T1, T2, T3, R4>(
  name: string,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, R4>,
): Operator<T, R4>;
export function comp<T, T1, T2, T3, T4, R5>(
  name: string,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, R5>,
): Operator<T, R5>;
export function comp<T, T1, T2, T3, T4, T5, R6>(
  name: string,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, R6>,
): Operator<T, R6>;
export function comp<T, T1, T2, T3, T4, T5, T6, R7>(
  name: string,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, T6>,
  op7: Operator<T6, R7>,
): Operator<T, R7>;
export function comp<T, T1, T2, T3, T4, T5, T6, T7, R8>(
  name: string,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, T6>,
  op7: Operator<T6, T7>,
  op8: Operator<T7, R8>,
): Operator<T, R8>;
export function comp<T, T1, T2, T3, T4, T5, T6, T7, T8, R9>(
  name: string,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, T6>,
  op7: Operator<T6, T7>,
  op8: Operator<T7, T8>,
  op9: Operator<T8, R9>,
): Operator<T, R9>;
export function comp<T, T1, T2, T3, T4, T5, T6, T7, T8, T9, R10>(
  name: string,
  op1: Operator<T, T1>,
  op2: Operator<T1, T2>,
  op3: Operator<T2, T3>,
  op4: Operator<T3, T4>,
  op5: Operator<T4, T5>,
  op6: Operator<T5, T6>,
  op7: Operator<T6, T7>,
  op8: Operator<T7, T8>,
  op9: Operator<T8, T9>,
  op10: Operator<T9, R10>,
): Operator<T, R10>;
export function comp(name: string, ...operators: Operator<any, any>[]): Operator<any, any>;
export function comp(name: string, ...operators: Operator<any, any>[]): Operator<any, any> {
  const composed = (source: Source<any>) => {
    let s: Source<any> = source;
    for (const op of operators) {
      s = op(s);
    }
    return s;
  };
  // The name shows up in stack traces and debugger views of the pipeline.
  Object.defineProperty(composed, 'name', { value: name, configurable: true });
  return composed;
}
