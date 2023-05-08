import toEqualBindings from './toEqualBindings';
import toEqualBindingsArray from './toEqualBindingsArray';
import toBeIsomorphicBindingsArray from './toEqualBindingsArray';
import toEqualBindingsStream from './toEqualBindingsStream';

export default [
  toEqualBindings,
  toEqualBindingsArray,
  toBeIsomorphicBindingsArray,
  toEqualBindingsStream,
// eslint-disable-next-line unicorn/prefer-object-from-entries
].reduce((acc, matcher) => ({ ...acc, ...matcher }), {});
