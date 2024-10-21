import type { Bindings } from '@comunica/utils-bindings-factory';
import { bindingsToString } from '@comunica/utils-bindings-factory';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import toEqualBindings from './toEqualBindings';

export function bindingsArrayToString(bindings: Bindings[]): string {
  if (bindings.length > 0) {
    return `[${bindings.map(term => (`\n\t${bindingsToString(term).replaceAll('\n', '\n\t')}, isAddition: ${term.getContextEntry<boolean>(new ActionContextKeyIsAddition())}`)).join('')}\n]`;
  }
  return `[ ]`;
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
};
