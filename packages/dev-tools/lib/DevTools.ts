import type { Bindings } from '@comunica/utils-bindings-factory';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { Bus } from '@comunica/core';
import type { BindingsStream } from '@comunica/types';
import {
  ActionContextKeyIsAddition,
  ActorMergeBindingsContextIsAddition,
} from '@incremunica/actor-merge-bindings-context-is-addition';
import { DataFactory } from 'rdf-data-factory';

export const DevTools = {
  bindingsToString(bindings: Bindings) {
    let string = `bindings, ${bindings.getContextEntry<boolean>(new ActionContextKeyIsAddition())} :`;
    for (const [ key, value ] of bindings) {
      string += `\n\t${key.value}: ${value.value}`;
    }
    return string;
  },

  printBindings(bindings: Bindings) {
    let string = `bindings, ${bindings.getContextEntry<boolean>(new ActionContextKeyIsAddition())} :`;
    for (const [ key, value ] of bindings) {
      string += `\n\t${key.value}: ${value.value}`;
    }
    // eslint-disable-next-line no-console
    console.log(string);
  },

  printBindingsStream(bindingsStream: BindingsStream) {
    return bindingsStream.map((bindings) => {
      if (bindings) {
        this.printBindings(<Bindings>bindings);
      } else {
        // eslint-disable-next-line no-console
        console.log(bindings);
      }
      return bindings;
    });
  },

  async createBindingsFactory(DF?: DataFactory): Promise<BindingsFactory> {
    return new BindingsFactory(
      DF ?? new DataFactory(),
      (await new ActorMergeBindingsContextIsAddition({
        bus: new Bus({ name: 'bus' }),
        name: 'actor',
      }).run(<any>{})).mergeHandlers,
    );
  },
};
