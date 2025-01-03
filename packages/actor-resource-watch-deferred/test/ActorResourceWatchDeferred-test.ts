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
    let mediatorHttp: any;

    beforeEach(() => {
      deferredEventEmitter = new EventEmitter();
      const context = new ActionContext().set(KeysResourceWatch.deferredEvaluationEventEmitter, deferredEventEmitter);
      priority = 0;
      mediatorHttp = <any> { mediate: jest.fn(() => Promise.resolve({ ok: true, headers: { get: () => '0' } })) };

      actor = new ActorResourceWatchDeferred({
        priority,
        name: 'actor',
        bus,
        mediatorHttp: mediatorHttp,
      });

      action = {
        context,
        url: 'www.test.com',
        metadata: {
          etag: '0',
          'cache-control': undefined,
          age: undefined,
        },
      };
    });

    describe('test', () => {
      it('should test', async () => {
        await expect(actor.test(action)).resolves.toPassTest({
          priority: 0,
        });
      });

      it('should not test if context doesn\'t have deferredEvaluationEventEmitter', async() => {
        action.context = new ActionContext();
        await expect(actor.test(action)).resolves
          .toFailTest('Context does not have \'deferredEvaluationEventEmitter\'');
      });

      it('should not test if source doesn\'t support HEAD requests', async () => {
        mediatorHttp.mediate = jest.fn(() => Promise.resolve({ ok: false }));
        await expect(actor.test(action)).resolves.toFailTest('Source does not support HEAD requests');
      });

      it('should not test if source doesn\'t support etags', async () => {
        mediatorHttp.mediate = jest.fn(() => Promise.resolve({ ok: true, headers: { get: () => null } }));
        await expect(actor.test(action)).resolves.toFailTest('Source does not support etag headers');
      });
    });

    describe('run', () => {
      it('should run', async () => {
        const result = await actor.run(action);
        expect(result.events).toBeInstanceOf(EventEmitter);
        expect(result.start).toBeInstanceOf(Function);
        expect(result.stop).toBeInstanceOf(Function);
      });

      it('should start and stop', async () => {
        const result = await actor.run(action);
        result.start();
        expect(deferredEventEmitter.listenerCount('update')).toBe(1);
        result.stop();
        expect(deferredEventEmitter.listenerCount('update')).toBe(0);
      });

      it('should start and stop multiple times', async () => {
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

      it('should not emit update events if not started', async () => {
        const result = await actor.run(action);
        const listener = jest.fn();
        result.events.on('update', listener);
        deferredEventEmitter.emit('update');
        expect(listener).toHaveBeenCalledTimes(0);
      });

      it('should not emit update events if started and no changes', async () => {
        const result = await actor.run(action);
        const listener = jest.fn();
        result.events.on('update', listener);
        result.start();
        deferredEventEmitter.emit('update');
        expect(mediatorHttp.mediate).toHaveBeenCalledTimes(1);
        //make sure the promise the mediator promise is resolved
        await new Promise(resolve => setImmediate(resolve));
        expect(listener).toHaveBeenCalledTimes(0);
      });

      it('should emit update events if started and changes', async () => {
        const result = await actor.run(action);
        const listener = jest.fn();
        result.events.on('update', listener);
        mediatorHttp.mediate = jest.fn(() => Promise.resolve({ ok: true, headers: { get: () => '1' } }));
        result.start();
        deferredEventEmitter.emit('update');
        expect(mediatorHttp.mediate).toHaveBeenCalledTimes(1);
        //make sure the promise the mediator promise is resolved
        await new Promise(resolve => setImmediate(resolve));
        expect(listener).toHaveBeenCalledTimes(1);
      });

      it('should not emit update events if stopped', async () => {
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
});
