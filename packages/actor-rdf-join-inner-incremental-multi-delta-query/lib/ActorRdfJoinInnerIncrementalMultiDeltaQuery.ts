import type { MediatorQueryOperation } from '@comunica/bus-query-operation';
import type {
  IActionRdfJoin,
  IActorRdfJoinArgs,
  IActorRdfJoinOutputInner,
} from '@comunica/bus-rdf-join';
import {
  ActorRdfJoin,
} from '@comunica/bus-rdf-join';
import { ActionContextKey } from '@comunica/core';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { MetadataBindings } from '@comunica/types';
import { DeltaQueryIterator } from './DeltaQueryIterator';

/**
 * A comunica Inner Incremental Nestedloop RDF Join Actor.
 */
export class ActorRdfJoinInnerIncrementalMultiDeltaQuery extends ActorRdfJoin {
  public readonly selectivityModifier: number;
  public readonly mediatorQueryOperation: MediatorQueryOperation;
  public static readonly keyFromDeltaQuery = new ActionContextKey('keyFromDeltaQuery');
  public constructor(args: IActorRdfJoinMultiDeltaQueryArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'multi-delta-query',
      canHandleUndefs: true,
    });
  }

  public async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const deltaQuery = new DeltaQueryIterator(
      action.entries,
      action.context,
      this.mediatorQueryOperation,
    );

    return {
      result: {
        type: 'bindings',
        bindingsStream: deltaQuery,
        metadata: async() => await this.constructResultMetadata(
          action.entries,
          await ActorRdfJoin.getMetadatas(action.entries),
          action.context,
        ),
      },
    };
  }

  public async getJoinCoefficients(
    action: IActionRdfJoin,
    metadatas: MetadataBindings[],
  ): Promise<IMediatorTypeJoinCoefficients> {
    // Throw when the previous join was a delta query join or when it is a static query
    if (action.context
      .get(ActorRdfJoinInnerIncrementalMultiDeltaQuery.keyFromDeltaQuery)) {
      throw new Error('Can\'t do two delta query joins after each other');
    }
    return {
      iterations: 0,
      persistedItems: 0,
      blockingItems: 0,
      requestTime: 0,
    };
  }
}

export interface IActorRdfJoinMultiDeltaQueryArgs extends IActorRdfJoinArgs {
  /**
   * Multiplier for selectivity values
   * @range {double}
   * @default {0.0001}
   */
  selectivityModifier: number;
  /**
  /**
   * The query operation mediator
   */
  mediatorQueryOperation: MediatorQueryOperation;
}
