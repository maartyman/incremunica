import { Bus } from '@comunica/core';
import { ActorRdfMetadataExtractGuardData } from '../lib/ActorRdfMetadataExtractGuardData';

describe('ActorRdfMetadataExtractGuardData', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfMetadataExtractGuardData instance', () => {
    let actor: ActorRdfMetadataExtractGuardData;

    beforeEach(() => {
      actor = new ActorRdfMetadataExtractGuardData({ name: 'actor', bus });
    });

    it('should test', () => {
      return;
    });
    //TODO make tests
  });
});
