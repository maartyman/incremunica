import {Bindings, bindingsToString} from '@comunica/bindings-factory';
import toEqualBindings from './toEqualBindings';
import {ActionContextKeyIsAddition} from "@incremunica/actor-merge-bindings-context-is-addition";

function bindingsArrayToString(bindings: Bindings[]): string {
  if (bindings.length > 0){
    return `[${bindings.map(term => ("\n\t" + bindingsToString(term).replaceAll("\n", "\n\t") + ", isAddition: " + term.getContextEntry(new ActionContextKeyIsAddition()))).join('')}\n]`;
  }
  return `[ ]`
}

export default {
  toEqualBindingsArray(received: Bindings[], actual: Bindings[]) {
    if (received.length !== actual.length) {
      return {
        message: () => `\nExpected:\n${bindingsArrayToString(actual)}\nReceived:\n${bindingsArrayToString(received)}`,
        pass: false,
      };
    }

    for (const [ i, element ] of received.entries()) {
      const sub = toEqualBindings.toEqualBindings(element, actual[i]);
      if (!sub.pass) {
        return {
          message: () => `\nExpected:\n${bindingsArrayToString(actual)}\nReceived:\n${bindingsArrayToString(received)}\nIndex ${i} is different.`,
          pass: false,
        };
      }
    }

    return {
      message: () => `\nExpected:\n${bindingsArrayToString(actual)}\nReceived:\n${bindingsArrayToString(received)}`,
      pass: true,
    };
  },
  toBeIsomorphicBindingsArray(received: Bindings[], actual: Bindings[]) {
    if (received.length !== actual.length) {
      return {
        message: () => `\nExpected:\n${bindingsArrayToString(actual)}\nReceived:\n${bindingsArrayToString(received)}`,
        pass: false,
      };
    }

    let actualCopy = [];
    for (const [ aI, aElement ] of actual.entries()) {
      actualCopy.push(aElement);
    }

    for (const [ i, element ] of received.entries()) {
      for (const [ aI, aElement ] of actualCopy.entries()) {
        const sub = toEqualBindings.toEqualBindings(element, aElement);
        if (sub.pass) {
          actualCopy[aI] = actualCopy[actualCopy.length-1];
          actualCopy.pop();
          break;
        }
      }
    }
    if (actualCopy.length != 0) {
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
