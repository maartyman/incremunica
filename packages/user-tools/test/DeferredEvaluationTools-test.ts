import 'jest';
import { DeferredEvaluation } from '../lib';

describe('DeferredEvaluationTools', () => {
  describe('DeferredEvaluation', () => {
    it('should trigger an update event on ', async() => {
      const deferredEvaluation = new DeferredEvaluation();
      const eventEmitter = deferredEvaluation.events;
      const updateSpy = jest.fn();
      eventEmitter.on('update', updateSpy);
      deferredEvaluation.triggerUpdate();
      expect(updateSpy).toHaveBeenCalledTimes(1);
    });
  });
});
