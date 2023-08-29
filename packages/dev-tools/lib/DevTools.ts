import type {Bindings, BindingsStream} from '@incremunica/incremental-types';

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
