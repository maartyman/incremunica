import type { IActionHttp, IActorHttpOutput } from '@comunica/bus-http';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { Bus } from '@comunica/core';
import type { IActionResourceWatch } from '@incremunica/bus-resource-watch';
import { ActorResourceWatchPolling } from '../lib/ActorResourceWatchPolling';
import 'jest-rdf';

describe('ActorGuardPolling', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorGuardPolling instance', () => {
    let actor: ActorResourceWatchPolling;
    let mediatorHttp: Mediator<
      Actor<IActionHttp, IActorTest, IActorHttpOutput>,
      IActionHttp,
IActorTest,
IActorHttpOutput
>;
    let priority: number;

    let action: IActionResourceWatch;
    let headersObject: {
      age: string | undefined;
      'cache-control': string | undefined;
      etag: number;
    };

    beforeEach(() => {
      priority = 0;
      headersObject = {
        age: undefined,
        'cache-control': undefined,
        etag: 0,
      };

      mediatorHttp = <any>{
        mediate: async(action: IActionHttp) => {
          return {
            headers: {
              age: headersObject.age,
              'cache-control': headersObject['cache-control'],
              etag: headersObject.etag,
              get: (key: string) => {
                if (key === 'etag') {
                  return headersObject.etag;
                }
                return undefined;
              },
            },
          };
        },
      };

      actor = new ActorResourceWatchPolling({
        beforeActors: [],
        mediatorHttp,
        defaultPollingFrequency: 1,
        priority,
        name: 'actor',
        bus,
      });

      action = {
        context: <any>{},
        url: 'www.test.com',
        metadata: {
          etag: 0,
          'cache-control': undefined,
          age: undefined,
        },
      };
    });

    it('should test', async() => {
      await expect(actor.test(action)).resolves.toEqual({ priority });
    });

    it('should get an update if the etag changes', async() => {
      headersObject.etag = 0;

      const result = await actor.run(action);

      headersObject.etag = 1;

      await new Promise<void>(resolve => result.events.on('update', () => {
        resolve();
      }));

      expect(true).toBeTruthy();

      result.stopFunction();
    });

    it('should use cache control', async() => {
      // Set data of file by setting etag and store
      action.metadata = {
        etag: 0,
        'cache-control': 'max-age=5',
        age: '0',
      };

      const result = await actor.run(action);

      headersObject.etag = 1;

      const time = process.hrtime();
      await new Promise<void>(resolve => result.events.on('update', () => {
        resolve();
      }));
      expect(process.hrtime(time)[0]).toBeGreaterThanOrEqual(3);

      result.stopFunction();
    });

    it('should use age', async() => {
      // Set data of file by setting etag and store
      action.metadata = {
        etag: 0,
        'cache-control': 'max-age=30',
        age: '25',
      };

      const result = await actor.run(action);

      headersObject.etag = 1;

      const time = process.hrtime();
      await new Promise<void>(resolve => result.events.on('update', () => {
        resolve();
      }));
      expect(process.hrtime(time)[0]).toBeGreaterThanOrEqual(3);

      result.stopFunction();
    });
  });
});
