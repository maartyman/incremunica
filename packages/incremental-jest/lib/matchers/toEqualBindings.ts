import type { Bindings } from '@comunica/utils-bindings-factory';
import { bindingsToString } from '@comunica/utils-bindings-factory';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';

function fail(received: Bindings, actual: Bindings): any {
  return {
    message: () => `\nExpected:\n${bindingsToString(actual)}, isAddition: ${actual.getContextEntry<boolean>(new ActionContextKeyIsAddition())}\nReceived:\n${bindingsToString(received)}, isAddition: ${received.getContextEntry<boolean>(new ActionContextKeyIsAddition())}`,
    pass: false,
  };
}

function succeed(received: Bindings, actual: Bindings): any {
  return {
    message: () => `\nExpected:\n${bindingsToString(actual)}, isAddition: ${actual.getContextEntry<boolean>(new ActionContextKeyIsAddition())}\nReceived:\n${bindingsToString(received)}, isAddition: ${received.getContextEntry<boolean>(new ActionContextKeyIsAddition())}`,
    pass: true,
  };
}

export default {
  toEqualBindings(received: Bindings, actual: Bindings) {
    if (!received.equals(actual)) {
      return fail(received, actual);
    }
    if (received.getContextEntry(new ActionContextKeyIsAddition()) !==
      actual.getContextEntry(new ActionContextKeyIsAddition())) {
      return fail(received, actual);
    }

    return succeed(received, actual);
  },
};
