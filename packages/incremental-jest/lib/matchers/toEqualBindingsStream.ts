import type { BindingsStream } from '@comunica/types';
import arrayifyStream from 'arrayify-stream';
import toEqualBindingsArray from './toEqualBindingsArray';
import { Bindings } from "@comunica/bindings-factory";

export default {
  async toEqualBindingsStream(received: BindingsStream, actual: Bindings[]) {
    return toEqualBindingsArray.toEqualBindingsArray(await arrayifyStream(received), actual);
  },
};
