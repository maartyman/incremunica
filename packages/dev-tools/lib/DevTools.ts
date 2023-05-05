import type { Bindings } from '@comunica/incremental-bindings-factory';

export const DevTools = {
  printBindings(bindings: Bindings) {
    let string = `bindings, ${bindings.diff} :`;
    bindings.forEach((value, key) => {
      string += `\n\t${key.value}: ${value.value}`;
    });
    // eslint-disable-next-line no-console
    console.log(string);
  },
};
