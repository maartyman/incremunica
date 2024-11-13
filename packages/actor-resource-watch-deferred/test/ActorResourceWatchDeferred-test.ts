import { EventEmitter } from 'events';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionResourceWatch } from '@incremunica/bus-resource-watch';
import { KeysResourceWatch } from '@incremunica/context-entries';
import { ActorResourceWatchDeferred } from '../lib';
import 'jest-rdf';
import '@comunica/utils-jest';

describe('ActorGuardDeferred', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorGuardDeferred instance', () => {
    let actor: ActorResourceWatchDeferred;
    let priority: number;
    let action: IActionResourceWatch;
    let deferredEventEmitter: EventEmitter;

    beforeEach(() => {
      deferredEventEmitter = new EventEmitter();
      const context = new ActionContext().set(KeysResourceWatch.deferredEvaluationEventEmitter, deferredEventEmitter);
      priority = 0;

      actor = new ActorResourceWatchDeferred({
        priority,
        name: 'actor',
        bus,
      });

      action = {
        context,
        url: 'www.test.com',
        metadata: {
          etag: 0,
          'cache-control': undefined,
          age: undefined,
        },
      };
    });

    it('should test', async() => {
      await expect(actor.test(action)).resolves.toPassTest({
        priority: 0,
      });
    });

    it('should not test', async() => {
      action.context = new ActionContext();
      await expect(actor.test(action)).resolves.toFailTest('Context does not have \'deferredEvaluationEventEmitter\'');
    });

    it('should run', async() => {
      const result = await actor.run(action);
      expect(result.events).toBeInstanceOf(EventEmitter);
      expect(result.start).toBeInstanceOf(Function);
      expect(result.stop).toBeInstanceOf(Function);
    });

    it('should start and stop', async() => {
      const result = await actor.run(action);
      result.start();
      expect(deferredEventEmitter.listenerCount('update')).toBe(1);
      result.stop();
      expect(deferredEventEmitter.listenerCount('update')).toBe(0);
    });

    it('should start and stop multiple times', async() => {
      const result = await actor.run(action);
      result.start();
      result.start();
      expect(deferredEventEmitter.listenerCount('update')).toBe(1);
      result.stop();
      result.stop();
      expect(deferredEventEmitter.listenerCount('update')).toBe(0);
      result.start();
      result.start();
      expect(deferredEventEmitter.listenerCount('update')).toBe(1);
      result.stop();
      result.stop();
      expect(deferredEventEmitter.listenerCount('update')).toBe(0);
    });

    it('should not emit update events if not started', async() => {
      const result = await actor.run(action);
      const listener = jest.fn();
      result.events.on('update', listener);
      deferredEventEmitter.emit('update');
      expect(listener).toHaveBeenCalledTimes(0);
    });

    it('should emit update events if started', async() => {
      const result = await actor.run(action);
      const listener = jest.fn();
      result.events.on('update', listener);
      result.start();
      deferredEventEmitter.emit('update');
      expect(listener).toHaveBeenCalledTimes(1);
    });

    it('should not emit update events if stopped', async() => {
      const result = await actor.run(action);
      const listener = jest.fn();
      result.events.on('update', listener);
      result.start();
      result.stop();
      deferredEventEmitter.emit('update');
      expect(listener).toHaveBeenCalledTimes(0);
    });
  });
});
