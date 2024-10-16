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

    it('should test', async() => {
      await expect(actor.test(<any>{})).resolves.toBeTruthy();
    });

    it('should run and extract data from headers', async() => {
      await expect(actor.run({
        metadata: <any>{},
        context: <any>{},
        requestTime: 0,
        url: 'http://test.com',
        headers: <any> {
          get: (key: string) => {
            if (key === 'etag') {
              return '123';
            }
            if (key === 'cache-control') {
              return 'max-age:20';
            }
            if (key === 'age') {
              return '5';
            }
            return null;
          },
        },
      })).resolves.toMatchObject({
        metadata: {
          etag: '123',
          'cache-control': 'max-age:20',
          age: '5',
        },
      });
    });

    it('should run with not all headers available', async() => {
      await expect(actor.run({
        metadata: <any>{},
        context: <any>{},
        requestTime: 0,
        url: 'http://test.com',
        headers: <any>{
          get: (key: string) => {
            if (key === 'etag') {
              return '123';
            }
            return null;
          },
        },
      })).resolves.toMatchObject({
        metadata: {
          etag: '123',
        },
      });
    });
  });
});
