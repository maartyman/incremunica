import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import type { MediatorMergeBindingsContext } from '@comunica/bus-merge-bindings-context';
import { KeysInitQuery } from '@comunica/context-entries';
import { ActionContext, Bus } from '@comunica/core';
import type { BindingsStream, IActionContext } from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import {
  ActionContextKeyIsAddition,
  ActorMergeBindingsContextIsAddition,
} from '@incremunica/actor-merge-bindings-context-is-addition';
import type { Quad } from '@incremunica/incremental-types';
import MurmurHash3 from 'imurmurhash';
import { DataFactory } from 'rdf-data-factory';
import type * as RDF from 'rdf-js';

const DF = new DataFactory();

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

  quad(isAddition: boolean, s: string, p: string, o: string, g?: string): Quad {
    const quad = <Quad>(DF.quad(DF.namedNode(s), DF.namedNode(p), DF.namedNode(o), g ? DF.namedNode(g) : undefined));
    quad.diff = isAddition;
    return quad;
  },

  createTestMediatorMergeBindingsContext(): MediatorMergeBindingsContext {
    return <any> {
      mediate: async() => {
        const mergeHandlers = (await new ActorMergeBindingsContextIsAddition({
          bus: new Bus({ name: 'bus' }),
          name: 'actor',
        }).run(<any>{})).mergeHandlers;
        return { mergeHandlers };
      },
    };
  },

  createTestHashBindings(b: RDF.Bindings): number {
    let hash = MurmurHash3();
    for (const variable of b.keys()) {
      hash = hash.hash(b.get(variable)?.value ?? 'UNDEF');
    }
    return hash.result();
  },

  createTestMediatorHashBindings(): MediatorHashBindings {
    return <any> {
      mediate: async() => {
        const hashFunction = (
          bindings: RDF.Bindings,
          variables: RDF.Variable[],
        ): number => DevTools.createTestHashBindings(
          bindings.filter((_value, key) => variables.some(variable => variable.equals(key))),
        );
        return { hashFunction };
      },
    };
  },

  async createTestBindingsFactory(DF?: DataFactory): Promise<BindingsFactory> {
    return new BindingsFactory(
      DF ?? new DataFactory(),
      (await new ActorMergeBindingsContextIsAddition({
        bus: new Bus({ name: 'bus' }),
        name: 'actor',
      }).run(<any>{})).mergeHandlers,
    );
  },

  createTestContextWithDataFactory(dataFactory?: DataFactory, context?: IActionContext): IActionContext {
    if (!context) {
      context = new ActionContext();
    }
    if (!dataFactory) {
      dataFactory = DF;
    }
    return context.set(KeysInitQuery.dataFactory, dataFactory);
  },
};
