import type { IActionRdfJoin,
  IActorRdfJoinOutputInner,
  IActorRdfJoinArgs } from '@comunica/bus-rdf-join';
import {
  ActorRdfJoin,
} from '@comunica/bus-rdf-join';
import type { BindingsStream } from '@comunica/incremental-types';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { MetadataBindings } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import { IncrementalMinusHash } from './IncrementalMinusHash';

/**
 * A comunica Minus Differential Hash RDF Join Actor.
 */
export class ActorRdfJoinMinusIncrementalHash extends ActorRdfJoin {
  public constructor(args: IActorRdfJoinArgs) {
    super(args, {
      logicalType: 'minus',
      physicalName: 'incremental-hash',
      limitEntries: 2,
    });
  }

  public async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const buffer = action.entries[1].output;
    const output = action.entries[0].output;

    const metadatas = await ActorRdfJoin.getMetadatas(action.entries);
    const commonVariables: RDF.Variable[] = ActorRdfJoin.overlappingVariables(metadatas);
    if (commonVariables.length > 0) {
      const bindingsStream = new IncrementalMinusHash(
        <BindingsStream><any>output.bindingsStream,
        <BindingsStream><any>buffer.bindingsStream,
        commonVariables,
        ActorRdfJoin.hash,
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
    const requestInitialTimes = ActorRdfJoin.getRequestInitialTimes(metadatas);
    const requestItemTimes = ActorRdfJoin.getRequestItemTimes(metadatas);
    return {
      iterations: metadatas[0].cardinality.value + metadatas[1].cardinality.value,
      persistedItems: metadatas[0].cardinality.value,
      blockingItems: metadatas[0].cardinality.value,
      requestTime: requestInitialTimes[0] + metadatas[0].cardinality.value * requestItemTimes[0] +
        requestInitialTimes[1] + metadatas[1].cardinality.value * requestItemTimes[1],
    };
  }
}
