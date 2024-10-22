import 'jest-rdf';
import '@incremunica/incremental-jest';
import { EventEmitter } from 'node:events';
import { ActionContext, Bus } from '@comunica/core';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import {
  ActionContextKeyIsAddition,
  ActorMergeBindingsContextIsAddition,
} from '@incremunica/actor-merge-bindings-context-is-addition';
import type { IActionGuard, MediatorGuard } from '@incremunica/bus-guard';
import { KeysGuard } from '@incremunica/context-entries';
import { DevTools } from '@incremunica/dev-tools';
import type { IGuardEvents } from '@incremunica/incremental-types';
import arrayifyStream from 'arrayify-stream';
import { DataFactory } from 'rdf-data-factory';
import { Factory } from 'sparqlalgebrajs';
import { ActorQuerySourceIdentifyHypermediaStreamNone } from '../lib';

const DF = new DataFactory();
const AF = new Factory();
const quad = require('rdf-quad');
const streamifyArray = require('streamify-array');

function captureEvents(item: EventEmitter, ...events: string[]) {
  const counts = (<any>item)._eventCounts = Object.create(null);
  for (const event of events) {
    counts[event] = 0;
    item.on(event, () => {
      counts[event]++;
    });
  }
  return item;
}

describe('ActorRdfResolveHypermediaStreamNone', () => {
  let bus: any;
  let BF: BindingsFactory;

  beforeEach(async() => {
    bus = new Bus({ name: 'bus' });
    BF = await DevTools.createBindingsFactory(DF);
  });

  describe('An ActorRdfResolveHypermediaStreamNone instance', () => {
    let actor: ActorQuerySourceIdentifyHypermediaStreamNone;
    let mediatorGuard: MediatorGuard;
    let mediatorMergeBindingsContext: any;
    let guardEvents: EventEmitter;
    let mediatorFn: jest.Func;

    beforeEach(() => {
      guardEvents = new EventEmitter();
      captureEvents(guardEvents, 'modified', 'up-to-date');
      mediatorFn = jest.fn();
      mediatorGuard = <any> {
        mediate: (action: IActionGuard) => {
          mediatorFn(action);
          return { guardEvents };
        },
      };
      mediatorMergeBindingsContext = <any> {
        mediate: async(action: any) => {
          return (await new ActorMergeBindingsContextIsAddition({
            bus: new Bus({ name: 'bus' }),
            name: 'actor',
          }).run(<any>{})).mergeHandlers;
        },
      };
      actor = new ActorQuerySourceIdentifyHypermediaStreamNone({
        name: 'actor',
        bus,
        mediatorGuard,
        mediatorMergeBindingsContext,
      });
    });

    it('should test', async() => {
      const action = <any>{};
      await expect(actor.test(action)).resolves.toMatchObject({ filterFactor: 0 });
    });

    it('should run and make a streaming store', async() => {
      const deletedQuad = quad('s1', 'p1', 'o1');
      deletedQuad.diff = false;
      const action = <any> {
        context: {
          get: () => {
            return '';
          },
        },
        url: 'http://test.com',
        quads: streamifyArray([
          quad('s1', 'p1', 'o1'),
          quad('s2', 'p2', 'o2'),
          deletedQuad,
        ]),
      };
      const result = (await actor.run(action));
      const stream = result.source.queryBindings(
        AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o')),
        new ActionContext(),
      );
      let number = 2;
      stream.on('data', () => {
        number--;
        if (number === 0) {
          stream.close();
        }
      });
      await expect(arrayifyStream(stream)).resolves.toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s1') ],
          [ DF.variable('p'), DF.namedNode('p1') ],
          [ DF.variable('o'), DF.namedNode('o1') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s2') ],
          [ DF.variable('p'), DF.namedNode('p2') ],
          [ DF.variable('o'), DF.namedNode('o2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s1') ],
          [ DF.variable('p'), DF.namedNode('p1') ],
          [ DF.variable('o'), DF.namedNode('o1') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), false),
      ]);
    });

    it('should run and add a guard', async() => {
      const action = <any> {
        context: {
          get: () => {
            return '';
          },
        },
        url: 'http://test.com',
        quads: streamifyArray([]),
      };
      await actor.run(action);
      expect(mediatorFn).toHaveBeenCalledTimes(1);
    });

    it('should add the guard events to the source', async() => {
      const action = <any> {
        context: {
          get: () => {
            return '';
          },
        },
        url: 'http://test.com',
        quads: streamifyArray([]),
      };
      const result = await actor.run(action);
      const events = <IGuardEvents>(<any> result.source).context.get(KeysGuard.events);
      expect(events).toEqual(guardEvents);
      guardEvents.emit('modified');
      expect((<any>guardEvents)._eventCounts.modified).toBe(1);
    });

    it('should add the guard events to the source even if the source has no context', async() => {
      mediatorFn = jest.fn((action: IActionGuard) => {
        action.streamingSource.context = undefined;
      });
      const action = <any> {
        context: {
          get: () => {
            return '';
          },
        },
        url: 'http://test.com',
        quads: streamifyArray([]),
      };
      const result = await actor.run(action);
      expect(mediatorFn).toHaveBeenCalledTimes(1);
      expect((<any> result.source).context.get(KeysGuard.events)).toEqual(guardEvents);
    });
  });
});
