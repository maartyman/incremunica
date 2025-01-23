import type { Bindings } from '@comunica/utils-bindings-factory';
import { bindingsToString } from '@comunica/utils-bindings-factory';
import { KeysBindings } from '@incremunica/context-entries';

function fail(received: Bindings, actual: Bindings): any {
  return {
    message: () => `\nExpected:\n${bindingsToString(actual)}, isAddition: ${actual.getContextEntry<boolean>(KeysBindings.isAddition) ?? true}\nReceived:\n${bindingsToString(received)}, isAddition: ${received.getContextEntry<boolean>(KeysBindings.isAddition) ?? true}`,
    pass: false,
  };
}

function succeed(received: Bindings, actual: Bindings): any {
  return {
    message: () => `\nExpected:\n${bindingsToString(actual)}, isAddition: ${actual.getContextEntry<boolean>(KeysBindings.isAddition) ?? true}\nReceived:\n${bindingsToString(received)}, isAddition: ${received.getContextEntry<boolean>(KeysBindings.isAddition) ?? true}`,
    pass: true,
  };
}

export default {
  toEqualBindings(received: Bindings, actual: Bindings) {
    if (!received.equals(actual)) {
      return fail(received, actual);
    }
    if ((received.getContextEntry(KeysBindings.isAddition) ?? true) !==
      (actual.getContextEntry(KeysBindings.isAddition) ?? true)) {
      return fail(received, actual);
    }

    return succeed(received, actual);
  },
};
