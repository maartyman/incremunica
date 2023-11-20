import {Bus} from '@comunica/core';
import {IActionDereferenceRdf, MediatorDereferenceRdf} from "@comunica/bus-dereference-rdf";
import {IActionGuard} from "@incremunica/bus-guard";
import {Transform} from "readable-stream";
import 'jest-rdf';
import { Store, DataFactory} from "n3";
import EventEmitter = require("events");
import {ActorGuardNaive} from "../lib";
import {IActionResourceWatch, IActorResourceWatchOutput, MediatorResourceWatch} from "@incremunica/bus-resource-watch";

const quad = require('rdf-quad');
const streamifyArray = require('streamify-array');

describe('ActorGuardNaive', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorGuardNaive instance', () => {
    let actor: ActorGuardNaive;
    let mediatorResourceWatch: MediatorResourceWatch;
    let mediatorDereferenceRdf: MediatorDereferenceRdf;
    let action: IActionGuard;
    let quadArrayStore: any[];
    let quadArray: any[];
    let streamingStoreEventEmitter: EventEmitter;
    let changeNotificationEventEmitter: EventEmitter;
    let removeQuadFn = jest.fn();
    let addQuadFn = jest.fn();
    let stopFn = jest.fn();
    let onFn: () => void;
    let hasEnded: {value: boolean};

    beforeEach(() => {
      quadArray = [];
      streamingStoreEventEmitter = new EventEmitter();
      changeNotificationEventEmitter = new EventEmitter();
      quadArrayStore = [];
      removeQuadFn = jest.fn();
      addQuadFn = jest.fn();
      stopFn = jest.fn();
      hasEnded = {value: false};

      mediatorDereferenceRdf = <any>{
        mediate: async (action: IActionDereferenceRdf) => {
          return {
            data: streamifyArray(quadArray)
          };
        }
      }

      mediatorResourceWatch = <any>{
        mediate: async (action: IActionResourceWatch): Promise<IActorResourceWatchOutput> => {
          return {
            events: changeNotificationEventEmitter,
            stopFunction: stopFn
          };
        }
      }

      actor = new ActorGuardNaive({
        name: 'actor',
        bus,
        mediatorResourceWatch,
        mediatorDereferenceRdf
      });

      action = {
        context: <any>{},
        url: "www.test.com",
        metadata: {},
        streamingSource: <any>{
          store: {
            on: (str: string, fn: () => void) => {
              onFn = fn;
            },
            hasEnded: () => {
              return hasEnded.value;
            },
            import: (stream: Transform) => {
              streamingStoreEventEmitter.emit("data", stream);
              return stream;
            },
            copyOfStore: () => {
              return new Store(quadArrayStore);
            },
            getStore: () => {
              return new Store(quadArrayStore);
            },
            removeQuad: (quad: any) => removeQuadFn(quad),
            addQuad: (quad: any) => addQuadFn(quad),
          }
        }
      }
    });

    it('should test', () => {
      return expect(actor.test(action)).resolves.toBeTruthy();
    });

    it('should stop resource watcher if store stops', async () => {
      await actor.run(action);

      onFn();

      expect(stopFn).toHaveBeenCalled();
    });


    it('should stop resource when the store has stopped really early', async () => {
      hasEnded.value = true;

      await actor.run(action);

      expect(stopFn).toHaveBeenCalled();
    });

    it('should attach a positive changes stream', async () => {
      quadArrayStore = [
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2')
      ];

      await actor.run(action);

      quadArray = [
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3')
      ];

      changeNotificationEventEmitter.emit("update");

      await new Promise<void>((resolve) => setImmediate(resolve));

      expect(addQuadFn).toHaveBeenCalledTimes(1);
      expect(addQuadFn).toHaveBeenCalledWith(
        DataFactory.quad(
          DataFactory.namedNode('s3'),
          DataFactory.namedNode('p3'),
          DataFactory.namedNode('o3')
        )
      );
    });

    it('should attach a negative changes stream', async () => {
      quadArrayStore = [
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3')
      ];

      await actor.run(action);

      quadArray = [
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2')
      ];

      changeNotificationEventEmitter.emit("update");

      await new Promise<void>((resolve) => setImmediate(resolve));

      expect(removeQuadFn).toHaveBeenCalledTimes(1);
      expect(removeQuadFn).toHaveBeenCalledWith(
        DataFactory.quad(
          DataFactory.namedNode('s3'),
          DataFactory.namedNode('p3'),
          DataFactory.namedNode('o3')
        )
      );
    });

    it('should handle delete events', async () => {
      quadArrayStore = [
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3')
      ];

      await actor.run(action);

      changeNotificationEventEmitter.emit("delete");

      await new Promise<void>((resolve) => setImmediate(resolve));

      expect(removeQuadFn).toHaveBeenCalledTimes(3);
      expect(removeQuadFn).toHaveBeenNthCalledWith(1,
        DataFactory.quad(
          DataFactory.namedNode('s1'),
          DataFactory.namedNode('p1'),
          DataFactory.namedNode('o1')
        )
      );
      expect(removeQuadFn).toHaveBeenNthCalledWith(2,
        DataFactory.quad(
          DataFactory.namedNode('s2'),
          DataFactory.namedNode('p2'),
          DataFactory.namedNode('o2')
        )
      );
      expect(removeQuadFn).toHaveBeenNthCalledWith(3,
        DataFactory.quad(
          DataFactory.namedNode('s3'),
          DataFactory.namedNode('p3'),
          DataFactory.namedNode('o3')
        )
      );
    });
  });
});


