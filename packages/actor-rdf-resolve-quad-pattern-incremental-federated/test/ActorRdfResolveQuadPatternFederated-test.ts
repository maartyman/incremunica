import { Bus } from '@comunica/core';
import { ActorRdfResolveQuadPatternIncrementalFederated } from '../lib/ActorRdfResolveQuadPatternIncrementalFederated';

describe('ActorRdfResolveQuadPatternFederated', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveQuadPatternIncrementalFederated instance', () => {
    let actor: ActorRdfResolveQuadPatternIncrementalFederated;

    beforeEach(() => {
      actor = new ActorRdfResolveQuadPatternIncrementalFederated({ name: 'actor', bus });
    });

    it('should test', () => {
      return expect(actor.test({ todo: true })).resolves.toEqual({ todo: true }); // TODO
    });

    it('should run', () => {
      return expect(actor.run({ todo: true })).resolves.toMatchObject({ todo: true }); // TODO
    });
  });
});
