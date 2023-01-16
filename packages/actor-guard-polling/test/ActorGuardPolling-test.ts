import { Bus } from '@comunica/core';
import { ActorGuardPolling } from '../lib/ActorGuardPolling';

describe('ActorGuardPolling', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorGuardPolling instance', () => {
    let actor: ActorGuardPolling;

    beforeEach(() => {
      actor = new ActorGuardPolling({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
