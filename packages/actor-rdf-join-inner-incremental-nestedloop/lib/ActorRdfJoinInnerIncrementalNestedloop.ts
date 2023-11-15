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
import { IncrementalNestedLoopJoin } from './IncrementalNestedLoopJoin';

/**
 * A comunica Inner Incremental Nestedloop RDF Join Actor.
 */
export class ActorRdfJoinInnerIncrementalNestedloop extends ActorRdfJoin {
  public constructor(args: IActorRdfJoinArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'nested-loop',
      limitEntries: 2,
      canHandleUndefs: true,
    });
  }

  protected async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const join = new IncrementalNestedLoopJoin(
      <BindingsStream><any>action.entries[0].output.bindingsStream,
      <BindingsStream><any>action.entries[1].output.bindingsStream,
      <any> ActorRdfJoin.joinBindings,
    );
    return {
      result: {
        type: 'bindings',
        bindingsStream: join,
        metadata: async() => await this.constructResultMetadata(
          action.entries,
          await ActorRdfJoin.getMetadatas(action.entries),
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
