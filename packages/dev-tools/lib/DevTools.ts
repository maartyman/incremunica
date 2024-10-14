import {Bindings, BindingsFactory} from '@comunica/bindings-factory';
import type {BindingsStream} from '@comunica/types';
import {
  ActionContextKeyIsAddition,
  ActorMergeBindingsContextIsAddition
} from "@incremunica/actor-merge-bindings-context-is-addition";
import {Bus} from "@comunica/core";
import {DataFactory} from "rdf-data-factory";

export const DevTools = {
  bindingsToString(bindings: Bindings) {
    let string = `bindings, ${bindings.getContextEntry(new ActionContextKeyIsAddition())} :`;
    bindings.forEach((value, key) => {
      string += `\n\t${key.value}: ${value.value}`;
    });
    return string;
  },

  printBindings(bindings: Bindings) {
    let string = `bindings, ${bindings.getContextEntry(new ActionContextKeyIsAddition())} :`;
    bindings.forEach((value, key) => {
      string += `\n\t${key.value}: ${value.value}`;
    });
    // eslint-disable-next-line no-console
    console.log(string);
  },

  printBindingsStream(bindingsStream: BindingsStream) {
    return bindingsStream.map((bindings) => {
      if (bindings == undefined)
        // eslint-disable-next-line no-console
        console.log(undefined);
      else
        this.printBindings(<Bindings>bindings);
      return bindings;
    });
  },

  async createBindingsFactory(DF?: DataFactory): Promise<BindingsFactory> {
    return new BindingsFactory(
      DF? DF : new DataFactory(),
      (await new ActorMergeBindingsContextIsAddition({
        bus: new Bus({name: 'bus'}),
        name: 'actor'
      }).run(<any>{})).mergeHandlers
    );
  }
};
