import type { IActionRdfJoin, IActorRdfJoinArgs, IActorRdfJoinOutputInner } from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { MetadataBindings } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import { IncrementalMinusHash } from './IncrementalMinusHash';

/**
 * An Incremunica Minus Hash RDF Join Actor.
 */
export class ActorRdfJoinIncrementalMinusHash extends ActorRdfJoin {
  public constructor(args: IActorRdfJoinArgs) {
    super(args, {
      logicalType: 'minus',
      physicalName: 'hash',
      limitEntries: 2,
      canHandleUndefs: true,
    });
  }

  public async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const buffer = action.entries[1].output;
    const output = action.entries[0].output;

    const metadatas = await ActorRdfJoin.getMetadatas(action.entries);
    const commonVariables: RDF.Variable[] = ActorRdfJoin.overlappingVariables(metadatas);
    if (commonVariables.length > 0) {
      const bindingsStream = new IncrementalMinusHash(
        output.bindingsStream,
        buffer.bindingsStream,
        commonVariables,
      );
      return {
        result: {
          type: 'bindings',
          bindingsStream,
          metadata: output.metadata,
        },
      };
    }
    return {
      result: output,
    };
  }

  protected async getJoinCoefficients(
    action: IActionRdfJoin,
    metadatas: MetadataBindings[],
  ): Promise<IMediatorTypeJoinCoefficients> {
    return {
      iterations: 0,
      persistedItems: 0,
      blockingItems: 0,
      requestTime: 0,
    };
  }
}
