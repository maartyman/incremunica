import type {
  IActionRdfMetadataExtract,
  IActorRdfMetadataExtractOutput,
  IActorRdfMetadataExtractArgs,
} from '@comunica/bus-rdf-metadata-extract';
import { ActorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';

/**
 * A comunica Guard Data RDF Metadata Extract Actor.
 */
export class ActorRdfMetadataExtractGuardData extends ActorRdfMetadataExtract {
  public constructor(args: IActorRdfMetadataExtractArgs) {
    super(args);
  }

  public async test(_action: IActionRdfMetadataExtract): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionRdfMetadataExtract): Promise<IActorRdfMetadataExtractOutput> {
    const metadata: IActorRdfMetadataExtractOutput['metadata'] = {};
    if (action.headers?.get('etag')) {
      metadata.etag = action.headers.get('etag');
    }
    if (action.headers?.get('cache-control')) {
      metadata['cache-control'] = action.headers.get('cache-control');
    }
    if (action.headers?.get('age')) {
      metadata.age = action.headers.get('age');
    }
    return { metadata };
  }
}
