import {ActionContext, Bus} from '@comunica/core';
import { ActorQuerySourceIdentifyHypermediaStreamNone } from '../lib';
import {IActionGuard, MediatorGuard} from "@incremunica/bus-guard";
import {DataFactory} from "rdf-data-factory";
import arrayifyStream from "arrayify-stream";
import 'jest-rdf'
import '@incremunica/incremental-jest'
import {EventEmitter} from "events";
import {KeysGuard} from "@incremunica/context-entries";
import { Factory } from 'sparqlalgebrajs';
import {IGuardEvents} from "@incremunica/incremental-types";
import {
  ActionContextKeyIsAddition,
  ActorMergeBindingsContextIsAddition
} from "@incremunica/actor-merge-bindings-context-is-addition";
import {BindingsFactory} from "@comunica/bindings-factory";
import {DevTools} from "@incremunica/dev-tools";

const DF = new DataFactory();
const AF = new Factory();
const quad = require('rdf-quad');
const streamifyArray = require('streamify-array');

function captureEvents(item: EventEmitter, ...events: string[]) {
  const counts = (<any>item)._eventCounts = Object.create(null);
  for (const event of events) {
    counts[event] = 0;
    item.on(event, () => { counts[event]++; });
  }
  return item;
}

describe('ActorRdfResolveHypermediaStreamNone', () => {
  let bus: any;
  let BF: BindingsFactory;

  beforeEach(async () => {
    bus = new Bus({name: 'bus'});
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
          return { guardEvents }
        }
      };
      mediatorMergeBindingsContext = <any> {
        mediate: async (action: any) => {
          return (await new ActorMergeBindingsContextIsAddition({
            bus: new Bus({name: 'bus'}),
            name: 'actor'
          }).run(<any>{})).mergeHandlers;
        }
      }
      actor = new ActorQuerySourceIdentifyHypermediaStreamNone({ name: 'actor', bus, mediatorGuard, mediatorMergeBindingsContext});
    });

    it('should test', async () => {
      let action = <any>{};
      expect(await actor.test(action)).toMatchObject({filterFactor: 0});
    });

    it('should run and make a streaming store', async () => {
      let deletedQuad = quad("s1","p1","o1");
      deletedQuad.diff = false
      let action = <any> {
        context: {
          get: () => {
            return ""
          }
        },
        url: "http://test.com",
        quads: streamifyArray([
          quad("s1","p1","o1"),
          quad("s2","p2","o2"),
          deletedQuad
        ])
      };
      let result = (await actor.run(action))
      let stream = result.source.queryBindings(
        AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o')),
        new ActionContext()
      );
      let number = 2
      stream.on("data", () => {
        number--;
        if (number == 0) {
          stream.close();
        }
      })
      expect(await arrayifyStream(stream)).toBeIsomorphicBindingsArray([
        BF.bindings([
          [DF.variable('s'), DF.namedNode('s1')],
          [DF.variable('p'), DF.namedNode('p1')],
          [DF.variable('o'), DF.namedNode('o1')],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [DF.variable('s'), DF.namedNode('s2')],
          [DF.variable('p'), DF.namedNode('p2')],
          [DF.variable('o'), DF.namedNode('o2')],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [DF.variable('s'), DF.namedNode('s1')],
          [DF.variable('p'), DF.namedNode('p1')],
          [DF.variable('o'), DF.namedNode('o1')],
        ]).setContextEntry(new ActionContextKeyIsAddition(), false),
      ]);
    });

    it('should run and add a guard', async () => {
      let action = <any> {
        context: {
          get: () => {
            return ""
          }
        },
        url: "http://test.com",
        quads: streamifyArray([])
      };
      await actor.run(action);
      expect(mediatorFn).toHaveBeenCalledTimes(1);
    });

    it('should add the guard events to the source', async () => {
      let action = <any> {
        context: {
          get: () => {
            return ""
          }
        },
        url: "http://test.com",
        quads: streamifyArray([])
      };
      let result = await actor.run(action);
      let events = <IGuardEvents>(<any> result.source).context.get(KeysGuard.events);
      expect(events).toEqual(guardEvents);
      guardEvents.emit("modified");
      expect((<any>guardEvents)._eventCounts.modified).toEqual(1);
    });

    it('should add the guard events to the source even if the source has no context', async () => {
      mediatorFn = jest.fn((action: IActionGuard) => {
        action.streamingSource.context = undefined;
      });
      let action = <any> {
        context: {
          get: () => {
            return ""
          }
        },
        url: "http://test.com",
        quads: streamifyArray([])
      };
      let result = await actor.run(action);
      expect(mediatorFn).toHaveBeenCalledTimes(1);
      expect((<any> result.source).context.get(KeysGuard.events)).toEqual(guardEvents);
    });
  });
});
