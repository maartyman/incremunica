import type { BindingsStream } from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import { arrayifyStream } from 'arrayify-stream';
import toEqualBindingsArray from './toEqualBindingsArray';

export default {
  async toEqualBindingsStream(received: BindingsStream, actual: Bindings[]) {
    return toEqualBindingsArray.toEqualBindingsArray(await arrayifyStream(received), actual);
  },
};
