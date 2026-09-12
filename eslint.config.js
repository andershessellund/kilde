import tseslint from 'typescript-eslint';

export default tseslint.config(
  { ignores: ['dist/'] },
  ...tseslint.configs.recommended,
  {
    rules: {
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_' },
      ],
      // Operator plumbing is generic over the value type; `any` at the
      // overload seams (stream/pipe/comp, Stream namespace) is deliberate.
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-empty-object-type': 'off',
      // Signals are callable objects: the implementation builds a function and
      // attaches state to it, which needs a `Function`-typed seam in signal.ts.
      '@typescript-eslint/no-unsafe-function-type': 'off',
      // Several bridges declare the connection with `let` and assign it after
      // building the sink that closes over it.
      'prefer-const': ['error', { ignoreReadBeforeAssign: true }],
    },
  },
);
