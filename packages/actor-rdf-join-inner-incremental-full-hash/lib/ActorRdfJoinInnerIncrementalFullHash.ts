import type {
  IActionRdfJoin,
  IActorRdfJoinArgs,
  IActorRdfJoinOutputInner,
} from '@comunica/bus-rdf-join';
import {
  ActorRdfJoin,
} from '@comunica/bus-rdf-join';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { MetadataBindings } from '@comunica/types';
import type { BindingsStream } from '@incremunica/incremental-types';
import { IncrementalFullHashJoin } from './IncrementalFullHashJoin';

/**
 * A comunica Inner Incremental Full Hash RDF Join Actor.
 */
export class ActorRdfJoinInnerIncrementalFullHash extends ActorRdfJoin {
  public constructor(args: IActorRdfJoinArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'full-hash',
      limitEntries: 2,
      canHandleUndefs: false,
    });
  }

  protected async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const metadatas = await ActorRdfJoin.getMetadatas(action.entries);
    const variables = ActorRdfJoin.overlappingVariables(metadatas);
    const join = new IncrementalFullHashJoin(
      <BindingsStream><any>action.entries[0].output.bindingsStream,
      <BindingsStream><any>action.entries[1].output.bindingsStream,
      entry => ActorRdfJoinInnerIncrementalFullHash.hash(entry, variables),
      <any> ActorRdfJoin.joinBindings,
    );
    return {
      result: {
        type: 'bindings',
        bindingsStream: join,
        metadata: async() => await this.constructResultMetadata(
          action.entries,
          metadatas,
          action.context,
        ),
      },
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
