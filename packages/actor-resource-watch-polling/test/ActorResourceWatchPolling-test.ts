import { Actor, Bus, IActorTest, Mediator} from '@comunica/core';
import { ActorResourceWatchPolling } from '../lib/ActorResourceWatchPolling';
import {IActionHttp, IActorHttpOutput } from "@comunica/bus-http";
import 'jest-rdf';
import {IActionResourceWatch} from "@incremunica/bus-resource-watch";

describe('ActorGuardPolling', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorGuardPolling instance', () => {
    let actor: ActorResourceWatchPolling;
    let mediatorHttp: Mediator<
      Actor<IActionHttp, IActorTest, IActorHttpOutput>,
      IActionHttp, IActorTest, IActorHttpOutput>;

    let action: IActionResourceWatch;
    let headersObject: {
      age: string | undefined,
      'cache-control': string | undefined,
      etag: number,
    };

    beforeEach(() => {
      headersObject = {
        age: undefined,
        'cache-control': undefined,
        etag: 0,
      };

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

      actor = new ActorResourceWatchPolling({
        beforeActors: [],
        mediatorHttp: mediatorHttp,
        defaultPollingFrequency: 1,
        name: 'actor', bus
      });

      action = {
        context: <any>{},
        url: "www.test.com",
        metadata: {
          etag: 0,
          "cache-control": undefined,
          age: undefined
        },
      }
    });

    it('should test', () => {
      return expect(actor.test(<any>{})).resolves.toBeTruthy();
    });

    it('should get an update if the etag changes', async () => {
      headersObject.etag = 0;

      let result = await actor.run(action);

      headersObject.etag = 1;

      await new Promise<void>(resolve => result.events.on("update", () => {
        resolve();
      }));

      expect(true).toBeTruthy();

      result.stopFunction();
    });

    it('should use cache control', async () => {
      //set data of file by setting etag and store
      action.metadata = {
        etag: 0,
        "cache-control": "max-age=5",
        age: "0"
      }

      let result = await actor.run(action);

      headersObject.etag = 1;

      let time = process.hrtime();
      await new Promise<void>(resolve => result.events.on("update", () => {
        resolve();
      }));
      expect(process.hrtime(time)[0]).toBeGreaterThanOrEqual(3);

      result.stopFunction();
    });

    it('should use age', async () => {
      //set data of file by setting etag and store
      action.metadata = {
        etag: 0,
        "cache-control": "max-age=30",
        age: "25"
      }

      let result = await actor.run(action);

      headersObject.etag = 1;

      let time = process.hrtime();
      await new Promise<void>(resolve => result.events.on("update", () => {
        resolve();
      }));
      expect(process.hrtime(time)[0]).toBeGreaterThanOrEqual(3);

      result.stopFunction();
    });
  });
});
