import type { Bindings } from '@comunica/bindings-factory';
import toEqualBindings from './toEqualBindings';
import { bindingsArrayToString } from './toEqualBindingsArray';

export default {
  toBeIsomorphicBindingsArray(received: Bindings[], actual: Bindings[]) {
    if (received.length !== actual.length) {
      return {
        message: () => `\nExpected:\n${bindingsArrayToString(actual)}\nReceived:\n${bindingsArrayToString(received)}`,
        pass: false,
      };
    }

    const actualCopy = [];
    for (const [ _aI, aElement ] of actual.entries()) {
      actualCopy.push(aElement);
    }

    for (const [ _i, element ] of received.entries()) {
      for (const [ aI, aElement ] of actualCopy.entries()) {
        const sub = toEqualBindings.toEqualBindings(element, aElement);
        if (sub.pass) {
          actualCopy[aI] = actualCopy.at(-1)!;
          actualCopy.pop();
          break;
        }
      }
    }
    if (actualCopy.length > 0) {
      return {
        message: () => `\nExpected:\n${bindingsArrayToString(actual)}\nReceived:\n${bindingsArrayToString(received)}`,
        pass: false,
      };
    }

    return {
      message: () => `\nExpected:\n${bindingsArrayToString(actual)}\nReceived:\n${bindingsArrayToString(received)}`,
      pass: true,
    };
  },
};
