import toBeIsomorphicBindingsArray from './toBeIsomorphicBindingsArray';
import toEqualBindings from './toEqualBindings';
import toEqualBindingsArray from './toEqualBindingsArray';
import toEqualBindingsStream from './toEqualBindingsStream';

export default [
  toEqualBindings,
  toEqualBindingsArray,
  toBeIsomorphicBindingsArray,
  toEqualBindingsStream,
].reduce((acc, matcher) => ({ ...acc, ...matcher }), {});
