import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaStreamNone } from '../lib/ActorRdfResolveHypermediaStreamNone';
import {IActionGuard, MediatorGuard} from "@incremunica/bus-guard";
import {DataFactory} from "rdf-data-factory";
import arrayifyStream from "arrayify-stream";
import 'jest-rdf'
import {EventEmitter} from "events";
import {KeysGuard} from "@incremunica/context-entries";
import {IGuardEvents} from "@incremunica/incremental-types";

const DF = new DataFactory();
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

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaStreamNone instance', () => {
    let actor: ActorRdfResolveHypermediaStreamNone;
    let mediatorGuard: MediatorGuard;
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
      actor = new ActorRdfResolveHypermediaStreamNone({ name: 'actor', bus, mediatorGuard});
    });

    it('should test', async () => {
      let action = <any>{};
      expect(await actor.test(action)).toMatchObject({filterFactor: 0});
    });

    it('should run and make a streaming store', async () => {
      let action = <any> {
        context: {
          get: () => {
            return ""
          }
        },
        url: "http://test.com",
        quads: streamifyArray([
          quad("s1","p1","o1"),
          quad("s2","p2","o2")
        ])
      };
      let stream = (await actor.run(action)).source.match(
        DF.variable('s'),
        DF.variable('p'),
        DF.variable('o'),
        DF.variable('g'),
      )
      let number = 2
      stream.on("data", () => {
        number--;
        if (number == 0) {
          stream.close();
        }
      })
      expect(await arrayifyStream(stream)).toBeRdfIsomorphic([
        quad("s1","p1","o1"),
        quad("s2","p2","o2")
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
