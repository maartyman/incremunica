import type { Bindings } from '@comunica/bindings-factory';
import { bindingsToString } from '@comunica/bindings-factory';

function fail(received: Bindings, actual: Bindings): any {
  return {
    message: () => `expected ${bindingsToString(received, true)} and ${bindingsToString(actual, true)} to be equal`,
    pass: false,
  };
}

function succeed(received: Bindings, actual: Bindings): any {
  return {
    message: () => `expected ${bindingsToString(received, true)} and ${bindingsToString(actual, true)} not to be equal`,
    pass: true,
  };
}

export default {
  toEqualBindings(received: Bindings, actual: Bindings) {
    if (!received.equals(actual, true)) {
      return fail(received, actual);
    }

    return succeed(received, actual);
  },
};
