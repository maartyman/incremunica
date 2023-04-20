import { Bus } from '@comunica/core';
import { ActorRdfJoinInnerIncrementalMultiBindJoin } from '../lib/ActorRdfJoinInnerIncrementalMultiBind';

describe('ActorRdfJoinInnerIncrementalMultiBindJoin', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfJoinInnerIncrementalMultiBindJoin instance', () => {
    let actor: ActorRdfJoinInnerIncrementalMultiBindJoin;

    beforeEach(() => {
      actor = new ActorRdfJoinInnerIncrementalMultiBindJoin({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
