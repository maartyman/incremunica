import type { Bindings } from '@comunica/utils-bindings-factory';
import { bindingsToString } from '@comunica/utils-bindings-factory';
import { KeysBindings } from '@incremunica/context-entries';

function fail(received: Bindings, actual: Bindings): any {
  return {
    message: () => `\nExpected:\n${bindingsToString(actual)}, isAddition: ${actual.getContextEntry<boolean>(KeysBindings.isAddition)}\nReceived:\n${bindingsToString(received)}, isAddition: ${received.getContextEntry<boolean>(KeysBindings.isAddition)}`,
    pass: false,
  };
}

function succeed(received: Bindings, actual: Bindings): any {
  return {
    message: () => `\nExpected:\n${bindingsToString(actual)}, isAddition: ${actual.getContextEntry<boolean>(KeysBindings.isAddition)}\nReceived:\n${bindingsToString(received)}, isAddition: ${received.getContextEntry<boolean>(KeysBindings.isAddition)}`,
    pass: true,
  };
}

export default {
  toEqualBindings(received: Bindings, actual: Bindings) {
    if (!received.equals(actual)) {
      return fail(received, actual);
    }
    if (received.getContextEntry(KeysBindings.isAddition) !==
      actual.getContextEntry(KeysBindings.isAddition)) {
      return fail(received, actual);
    }

    return succeed(received, actual);
  },
};
