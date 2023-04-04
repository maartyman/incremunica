import { Actor, Bus, IActorTest, Mediator} from '@comunica/core';
import { ActorGuardPollingDiff } from '../lib/ActorGuardPollingDiff';
import {IActionHttp, IActorHttpOutput } from "@comunica/bus-http";
import {IActionDereferenceRdf, IActorDereferenceRdfOutput} from "@comunica/bus-dereference-rdf";
import {ActorGuard, IActionGuard} from "@comunica/bus-guard";
import {Transform} from "readable-stream";
import arrayifyStream from "arrayify-stream";
import 'jest-rdf';
import {Store} from "n3";
import EventEmitter = require("events");
import {PollingDiffGuard} from "../lib/PollingDiffGuard";

const quad = require('rdf-quad');
const streamifyArray = require('streamify-array');

describe('ActorGuardPolling', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorGuardPolling instance', () => {
    let actor: ActorGuardPollingDiff;
    let mediatorHttp: Mediator<
      Actor<IActionHttp, IActorTest, IActorHttpOutput>,
      IActionHttp, IActorTest, IActorHttpOutput>;
    let mediatorDereferenceRdf: Mediator<
      Actor<IActionDereferenceRdf, IActorTest, IActorDereferenceRdfOutput>,
      IActionDereferenceRdf, IActorTest, IActorDereferenceRdfOutput>;
    let action: IActionGuard;
    let quadArrayStore: any[];
    let quadArray: any[];
    let headersObject: {
      age: string | undefined,
      'cache-control': string | undefined,
      etag: number,
    };
    let eventEmitter: EventEmitter;

    beforeEach(() => {
      headersObject = {
        age: undefined,
        'cache-control': undefined,
        etag: 0,
      };
      quadArray = [];
      eventEmitter = new EventEmitter();
      quadArrayStore = [];

      mediatorHttp = <any>{
        mediate: async (action: IActionHttp) => {
          return {
            headers: {
              age: headersObject.age,
              'cache-control': headersObject["cache-control"],
              etag: headersObject.etag,
              get: (key: string) => {
                if (key == "etag") {
                  return headersObject.etag;
                }
                return undefined;
              }
            }
          }
        },
      };

      mediatorDereferenceRdf = <any>{
        mediate: async (action: IActionDereferenceRdf) => {
          return {
            data: streamifyArray(quadArray),
            headers: {
              age: headersObject.age,
              'cache-control': headersObject["cache-control"],
              etag: headersObject.etag,
              forEach: (func: (val: any, key: any) => void) => {
                func(headersObject.age, 'age');
                func(headersObject["cache-control"], 'cache-control');
                func(headersObject.etag, 'etag');
              }
            }
          };
        }
      }

      actor = new ActorGuardPollingDiff({
        beforeActors: [],
        mediatorHttp: mediatorHttp,
        pollingFrequency: 1,
        name: 'actor', bus, mediatorDereferenceRdf
      });

      action = {
        context: <any>{},
        url: "www.test.com",
        metadata: {
          etag: 0,
          "cache-control": undefined,
          age: undefined
        },
        streamingSource: <any>{
          store: {
            on: (str: string, fn: () => void) => {

            },
            hasEnded: () => {
              return false
            },
            import: (stream: Transform) => {
              eventEmitter.emit("data", stream);
              return stream;
            },
            copyOfStore: () => {
              return new Store(quadArrayStore);
            }
          }
        }
      }

    });

    afterEach(() => {
      ActorGuard.deleteGuard(action.url);
    })

    it('should test', () => {
      return expect(actor.test(<any>{})).resolves.toBeTruthy();
    });

    it('should attach a positive changes stream', async () => {
      //set data of file by setting etag and store
      headersObject.etag = 0;
      quadArrayStore = [
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2')
      ];

      await actor.run(action);

      let promise = new Promise<void>((resolve) => {
        eventEmitter.once("data", async (stream) => {
          let tempArray = await arrayifyStream(stream);
          expect(tempArray).toBeRdfIsomorphic([
            quad('s3', 'p3', 'o3')
          ]);
          expect(tempArray[0].diff).toBeTruthy();
          resolve();
        });
      });

      //set data of file by setting quadArray and etag
      headersObject.etag = 1;
      quadArray = [
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3')
      ];

      await promise;
    });

    it('should attach a negative changes stream', async () => {
      //set data of file by setting etag and store
      headersObject.etag = 0;
      quadArrayStore = [
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3')
      ];

      await actor.run(action);

      let promise = new Promise<void>((resolve) => {
        eventEmitter.once("data", async (stream) => {
          let tempArray = await arrayifyStream(stream);
          expect(tempArray).toBeRdfIsomorphic([
            quad('s3', 'p3', 'o3')
          ]);
          expect(tempArray[0].diff).toBeFalsy();
          resolve();
        });
      });

      //set data of file by setting quadArray and etag
      headersObject.etag = 1;
      quadArray = [
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2')
      ];

      await promise;
    });

    it('should overwrite the an old guard', async () => {
      await actor.run(action);

      expect(
        (<ActorGuardPollingDiff><any>ActorGuard.getGuard(action.url)).pollingFrequency
      ).toEqual(1);

      action.metadata = {
        etag: 0,
        "cache-control": "max-age=2",
        age: "0"
      }

      await actor.run(action);

      expect(
        (<ActorGuardPollingDiff><any>ActorGuard.getGuard(action.url)).pollingFrequency
      ).toEqual(2);
    });

    it('should use cache control', async () => {
      //set data of file by setting etag and store
      action.metadata = {
        etag: 0,
        "cache-control": "max-age=123",
        age: "0"
      }

      await actor.run(action);

      expect(
        (<ActorGuardPollingDiff><any>ActorGuard.getGuard(action.url)).pollingFrequency
      ).toEqual(123);
    });

    it('should use age', async () => {
      //set data of file by setting etag and store
      action.metadata = {
        etag: 0,
        "cache-control": "max-age=2",
        age: "1"
      }

      await actor.run(action);

      let promise = new Promise<void>((resolve) => {
        eventEmitter.once("data", async (stream) => {
          resolve();
        });
      });

      //set data of file by setting quadArray and etag
      headersObject.etag = 1;
      quadArray = [];

      await promise;

      expect(
        (<ActorGuardPollingDiff><any>ActorGuard.getGuard(action.url)).pollingFrequency
      ).toEqual(2);
    });

  });

  describe('An ActorGuardPolling instance when the store has no listeners', () => {
    let actor: ActorGuardPollingDiff;
    let mediatorHttp: Mediator<
      Actor<IActionHttp, IActorTest, IActorHttpOutput>,
      IActionHttp, IActorTest, IActorHttpOutput>;
    let mediatorDereferenceRdf: Mediator<
      Actor<IActionDereferenceRdf, IActorTest, IActorDereferenceRdfOutput>,
      IActionDereferenceRdf, IActorTest, IActorDereferenceRdfOutput>;
    let action: IActionGuard;
    let quadArrayStore: any[];
    let quadArray: any[];
    let headersObject: {
      age: string | undefined,
      'cache-control': string | undefined,
      etag: number,
    };
    let eventEmitter: EventEmitter;
    let func = () => {}

    beforeEach(() => {
      headersObject = {
        age: undefined,
        'cache-control': undefined,
        etag: 0,
      };
      quadArray = [];
      eventEmitter = new EventEmitter();
      quadArrayStore = [];

      mediatorHttp = <any>{
        mediate: async (action: IActionHttp) => {
          return {
            headers: {
              age: headersObject.age,
              'cache-control': headersObject["cache-control"],
              etag: headersObject.etag,
              get: (key: string) => {
                if (key == "etag") {
                  return headersObject.etag;
                }
                return undefined;
              }
            }
          }
        },
      };

      mediatorDereferenceRdf = <any>{
        mediate: async (action: IActionDereferenceRdf) => {
          return {
            data: streamifyArray(quadArray),
            headers: {
              age: headersObject.age,
              'cache-control': headersObject["cache-control"],
              etag: headersObject.etag,
              forEach: (func: (val: any, key: any) => void) => {
                func(headersObject.age, 'age');
                func(headersObject["cache-control"], 'cache-control');
                func(headersObject.etag, 'etag');
              }
            }
          };
        }
      }

      actor = new ActorGuardPollingDiff({
        beforeActors: [],
        mediatorHttp: mediatorHttp,
        pollingFrequency: 1,
        name: 'actor', bus, mediatorDereferenceRdf
      });

      action = {
        context: <any>{},
        url: "www.test.com",
        metadata: {
          etag: 0,
          "cache-control": undefined,
          age: undefined
        },
        streamingSource: <any>{
          store: {
            on: (str: string, fn: () => void) => {
              eventEmitter.on("end", fn);
            },
            hasEnded: () => {
              return false;
            },
            import: (stream: Transform) => {
              eventEmitter.emit("data", stream);
              return stream;
            },
            copyOfStore: () => {
              return new Store(quadArrayStore);
            }
          }
        }
      }

    });

    it('should stop if on end is called', async () => {
      //set data of file by setting etag and store
      action.metadata = {
        etag: 0,
        "cache-control": undefined,
        age: undefined
      }

      await actor.run(action);

      let promise = new Promise<void>((resolve) => {
        eventEmitter.once("data", async (stream) => {
          resolve();
        });
      });

      //set data of file by setting quadArray and etag
      headersObject.etag = 1;
      quadArray = [];

      await promise;

      //after first get request delete guard
      eventEmitter.emit("end");

      expect(
        (<PollingDiffGuard><any>ActorGuard.getGuard(action.url))
      ).toBeUndefined();
    });
  });
});


