import type {Bindings, BindingsStream} from '@comunica/incremental-types';
import {bindingsToString} from "@comunica/incremental-bindings-factory";
import {Quad} from "@comunica/incremental-types";

export const DevTools = {
  printBindings(bindings: Bindings) {
    let string = `bindings, ${bindings.diff} :`;
    bindings.forEach((value, key) => {
      string += `\n\t${key.value}: ${value.value}`;
    });
    // eslint-disable-next-line no-console
    console.log(string);
  },

  printBindingsStream(bindingsStream: BindingsStream) {
    return bindingsStream.map((bindings) => {
      this.printBindings(bindings);
      return bindings;
    });
  }
};
