import {Bindings, bindingsToString} from '@incremunica/incremental-bindings-factory';
import toEqualBindings from './toEqualBindings';

function bindingsArrayToString(bindings: Bindings[]): string {
  return `[ ${bindings.map(term => bindingsToString(term, true)).join(', ')} ]`;
}

export default {
  toEqualBindingsArray(received: Bindings[], actual: Bindings[]) {
    if (received.length !== actual.length) {
      return {
        message: () => `expected ${bindingsArrayToString(received)} to equal ${bindingsArrayToString(actual)}`,
        pass: false,
      };
    }

    for (const [ i, element ] of received.entries()) {
      const sub = toEqualBindings.toEqualBindings(element, actual[i]);
      if (!sub.pass) {
        return {
          message: () => `expected ${bindingsArrayToString(received)} to equal ${bindingsArrayToString(actual)}\nIndex ${i} is different.`,
          pass: false,
        };
      }
    }

    return {
      message: () => `expected ${bindingsArrayToString(received)} not to equal ${bindingsArrayToString(actual)}`,
      pass: true,
    };
  },
  toBeIsomorphicBindingsArray(received: Bindings[], actual: Bindings[]) {
    if (received.length !== actual.length) {
      return {
        message: () => `expected ${bindingsArrayToString(received)} to equal ${bindingsArrayToString(actual)}`,
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
        message: () => `expected ${bindingsArrayToString(received)} to equal ${bindingsArrayToString(actual)}.`,
        pass: false,
      };
    }

    return {
      message: () => `expected ${bindingsArrayToString(received)} not to equal ${bindingsArrayToString(actual)}`,
      pass: true,
    };
  },
};
