import type { IActionHttp, IActorHttpOutput, MediatorHttp } from '@comunica/bus-http';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionResourceWatch } from '@incremunica/bus-resource-watch';
import { KeysResourceWatch } from '@incremunica/context-entries';
import { ActorResourceWatchPolling } from '../lib';
import 'jest-rdf';
import '@comunica/utils-jest';

expect.extend({
  toBeBetween(received, argumentOne, argumentTwo) {
    if (argumentOne > argumentTwo) {
      // Switch values
      [ argumentOne, argumentTwo ] = [ argumentTwo, argumentOne ];
    }

    const pass = (received >= argumentOne && received <= argumentTwo);

    if (pass) {
      return {
        message: () => (`expected ${received} not to be between ${argumentOne} and ${argumentTwo}`),
        pass: true,
      };
    }
    return {
      message: () => (`expected ${received} to be between ${argumentOne} and ${argumentTwo}`),
      pass: false,
    };
  },
});

describe('ActorGuardPolling', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorGuardPolling instance', () => {
    let actor: ActorResourceWatchPolling;
    let mediatorHttp: MediatorHttp;
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
        mediate: jest.fn(async(_action: IActionHttp) => {
          return {
            headers: {
              age: headersObject.age,
              'cache-control': headersObject['cache-control'],
              etag: headersObject.etag,
              get: (key: string) => {
                if (key === 'etag') {
                  return headersObject.etag;
                }
                if (key === 'age') {
                  return headersObject.age;
                }
                return undefined;
              },
              has: (key: string) => {
                if (key === 'etag') {
                  return Boolean(headersObject.etag);
                }
                if (key === 'age') {
                  return Boolean(headersObject.age);
                }
                return undefined;
              },
            },
          };
        }),
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
        context: new ActionContext(),
        url: 'www.test.com',
        metadata: {
          etag: 0,
          'cache-control': undefined,
          age: undefined,
        },
      };

      jest.spyOn(globalThis, 'setTimeout');
      jest.spyOn(globalThis, 'clearTimeout');
      jest.spyOn(globalThis, 'setInterval');
      jest.spyOn(globalThis, 'clearInterval');
    });

    afterEach(() => {
      jest.clearAllMocks();
    });

    it('should test', async() => {
      await expect(actor.test(action)).resolves.toPassTest({
        priority: 0,
      });
    });

    it('should emit "delete" if HTTP mediator errors', async() => {
      mediatorHttp.mediate = async(): Promise<IActorHttpOutput> => {
        throw new Error('Test error in HTTP mediator');
      };
      headersObject.etag = 0;

      const result = await actor.run(action);
      result.start();
      expect(setInterval).toHaveBeenCalledTimes(0);
      expect(setTimeout).toHaveBeenCalledTimes(1);
      expect(setTimeout).toHaveBeenNthCalledWith(1, expect.any(Function), 1000);
      headersObject.etag = 1;
      await expect(new Promise<void>(resolve => result.events.once('delete', () => {
        resolve();
      }))).resolves.toBeUndefined();

      expect(setInterval).toHaveBeenCalledTimes(1);
      expect(setInterval).toHaveBeenNthCalledWith(1, expect.any(Function), 1000);

      result.stop();
      expect(clearTimeout).toHaveBeenCalledTimes(1);
      expect(clearInterval).toHaveBeenCalledTimes(1);
    });

    it('should get an update if the etag changes', async() => {
      headersObject.etag = 0;

      const result = await actor.run(action);
      result.start();
      expect(setInterval).toHaveBeenCalledTimes(0);
      expect(setTimeout).toHaveBeenCalledTimes(1);
      expect(setTimeout).toHaveBeenNthCalledWith(1, expect.any(Function), 1000);

      headersObject.etag = 1;

      await expect(new Promise<void>(resolve => result.events.once('update', () => {
        resolve();
      }))).resolves.toBeUndefined();

      result.stop();
      expect(setInterval).toHaveBeenCalledTimes(1);
      expect(setInterval).toHaveBeenNthCalledWith(1, expect.any(Function), 1000);
      expect(clearTimeout).toHaveBeenCalledTimes(1);
      expect(clearInterval).toHaveBeenCalledTimes(1);
    });

    it('should be able to start and stop multiple times', async() => {
      action.context = action.context.set(KeysResourceWatch.pollingFrequency, 0.5);
      headersObject.etag = 0;

      const result = await actor.run(action);

      for (let i = 1; i < 6; i++) {
        result.start();
        expect(setInterval).toHaveBeenCalledTimes(i - 1);
        expect(setTimeout).toHaveBeenCalledTimes(i);
        expect(setTimeout).toHaveBeenNthCalledWith(i, expect.any(Function), 500);

        headersObject.etag = i;

        await expect(new Promise<void>(resolve => result.events.once('update', () => {
          resolve();
        }))).resolves.toBeUndefined();

        result.stop();
        expect(setInterval).toHaveBeenCalledTimes(i);
        expect(clearTimeout).toHaveBeenCalledTimes(i);
        expect(clearInterval).toHaveBeenCalledTimes(i);
      }
    });

    it('should use cache control', async() => {
      // Set data of file by setting etag and store
      action.metadata = {
        etag: 0,
        'cache-control': 'max-age=5',
        age: '0',
      };
      headersObject = {
        etag: 0,
        'cache-control': 'max-age=5',
        age: '0',
      };

      const result = await actor.run(action);
      result.start();
      expect(setInterval).toHaveBeenCalledTimes(0);
      expect(setTimeout).toHaveBeenCalledTimes(1);
      // @ts-expect-error
      expect(setTimeout).toHaveBeenNthCalledWith(1, expect.any(Function), expect.toBeBetween(4900, 5000));

      headersObject.etag = 1;

      const time = process.hrtime();
      await expect(new Promise<void>(resolve => result.events.once('update', () => {
        resolve();
      }))).resolves.toBeUndefined();
      expect(process.hrtime(time)[0]).toBeGreaterThanOrEqual(3);

      result.stop();
      expect(setInterval).toHaveBeenCalledTimes(1);
      expect(setTimeout).toHaveBeenCalledTimes(1);
      expect(setInterval).toHaveBeenNthCalledWith(1, expect.any(Function), 5000);
      // @ts-expect-error
      expect(setTimeout).toHaveBeenNthCalledWith(1, expect.any(Function), expect.toBeBetween(4900, 5000));
      expect(clearTimeout).toHaveBeenCalledTimes(1);
      expect(clearInterval).toHaveBeenCalledTimes(1);
    });

    it('should use polling frequency in context', async() => {
      action.context = new ActionContext().set(KeysResourceWatch.pollingFrequency, 2);

      headersObject.etag = 0;

      const result = await actor.run(action);
      result.start();
      expect(setInterval).toHaveBeenCalledTimes(0);
      expect(setTimeout).toHaveBeenCalledTimes(1);
      expect(setTimeout).toHaveBeenNthCalledWith(1, expect.any(Function), 2000);

      headersObject.etag = 1;

      const time = process.hrtime();
      await expect(new Promise<void>(resolve => result.events.once('update', () => {
        resolve();
      }))).resolves.toBeUndefined();
      expect(process.hrtime(time)[0]).toBeGreaterThanOrEqual(1);

      result.stop();
      expect(setInterval).toHaveBeenCalledTimes(1);
      expect(setTimeout).toHaveBeenCalledTimes(1);
      expect(setInterval).toHaveBeenNthCalledWith(1, expect.any(Function), 2000);
      expect(setTimeout).toHaveBeenNthCalledWith(1, expect.any(Function), 2000);
      expect(clearTimeout).toHaveBeenCalledTimes(1);
      expect(clearInterval).toHaveBeenCalledTimes(1);
    });

    it('should clear timeout if immediately stopped', async() => {
      // Set data of file by setting etag and store
      action.metadata = {
        etag: 0,
        'cache-control': 'max-age=30',
        age: '25',
      };
      headersObject = {
        etag: 0,
        'cache-control': 'max-age=30',
        age: '25',
      };

      const result = await actor.run(action);
      result.start();
      expect(setInterval).toHaveBeenCalledTimes(0);
      expect(setTimeout).toHaveBeenCalledTimes(1);
      // @ts-expect-error
      expect(setTimeout).toHaveBeenNthCalledWith(1, expect.any(Function), expect.toBeBetween(4900, 5000));
      result.stop();
      expect(clearTimeout).toHaveBeenCalledTimes(1);
      expect(clearInterval).toHaveBeenCalledTimes(0);
    });

    it('should use age', async() => {
      // Set data of file by setting etag and store
      action.metadata = {
        etag: 0,
        'cache-control': 'max-age=30',
        age: '25',
      };
      headersObject = {
        etag: 0,
        'cache-control': 'max-age=30',
        age: '25',
      };

      const result = await actor.run(action);
      result.start();

      headersObject.etag = 1;

      const time = process.hrtime();
      await expect(new Promise<void>(resolve => result.events.once('update', () => {
        resolve();
      }))).resolves.toBeUndefined();
      expect(process.hrtime(time)[0]).toBeGreaterThanOrEqual(3);
      expect(setInterval).toHaveBeenCalledTimes(1);
      expect(setTimeout).toHaveBeenCalledTimes(1);
      expect(setInterval).toHaveBeenNthCalledWith(1, expect.any(Function), 30000);
      // @ts-expect-error
      expect(setTimeout).toHaveBeenNthCalledWith(1, expect.any(Function), expect.toBeBetween(4900, 5000));

      result.stop();
      expect(clearTimeout).toHaveBeenCalledTimes(1);
      expect(clearInterval).toHaveBeenCalledTimes(1);
    });

    it('should use age with late start', async() => {
      // Set data of file by setting etag and store
      action.metadata = {
        etag: 0,
        'cache-control': 'max-age=2',
        age: '1',
      };
      headersObject = {
        age: '1',
        'cache-control': 'max-age=2',
        etag: 0,
      };

      const result = await actor.run(action);
      await new Promise<void>(resolve => setTimeout(resolve, 1050));
      result.start();
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(1);
      expect(setInterval).toHaveBeenCalledTimes(0);
      expect(setTimeout).toHaveBeenCalledTimes(2);
      // @ts-expect-error
      expect(setTimeout).toHaveBeenNthCalledWith(2, expect.any(Function), expect.toBeBetween(1_900, 2_000));

      const time = process.hrtime();
      await new Promise<void>(resolve => setTimeout(resolve, 500));

      headersObject.etag = 1;

      await expect(new Promise<void>(resolve => result.events.once('update', () => {
        resolve();
      }))).resolves.toBeUndefined();
      expect(process.hrtime(time)[0]).toBeGreaterThanOrEqual(1);
      expect(mediatorHttp.mediate).toHaveBeenCalledTimes(2);

      expect(setInterval).toHaveBeenCalledTimes(1);
      expect(setInterval).toHaveBeenNthCalledWith(1, expect.any(Function), 2000);

      result.stop();
      expect(clearTimeout).toHaveBeenCalledTimes(1);
      expect(clearInterval).toHaveBeenCalledTimes(1);
    });

    it('should handle start being called multiple times', async() => {
      headersObject.etag = 0;

      const result = await actor.run(action);
      result.start();
      result.start();
      result.start();
      expect(setInterval).toHaveBeenCalledTimes(0);
      expect(setTimeout).toHaveBeenCalledTimes(1);
      expect(setTimeout).toHaveBeenNthCalledWith(1, expect.any(Function), 1000);

      headersObject.etag = 1;

      await expect(new Promise<void>(resolve => result.events.once('update', () => {
        resolve();
      }))).resolves.toBeUndefined();

      result.stop();
      result.stop();
      expect(setInterval).toHaveBeenCalledTimes(1);
      expect(setInterval).toHaveBeenNthCalledWith(1, expect.any(Function), 1000);
      expect(clearTimeout).toHaveBeenCalledTimes(1);
      expect(clearInterval).toHaveBeenCalledTimes(1);

      result.start();
      result.start();
      expect(setInterval).toHaveBeenCalledTimes(1);
      expect(setTimeout).toHaveBeenCalledTimes(2);
      expect(setTimeout).toHaveBeenNthCalledWith(2, expect.any(Function), 1000);

      result.stop();
      expect(clearTimeout).toHaveBeenCalledTimes(2);
      expect(clearInterval).toHaveBeenCalledTimes(2);
    });
  });
});
