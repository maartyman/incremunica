import type {
  IActionRdfJoin,
  IActorRdfJoinArgs,
  IActorRdfJoinOutputInner,
  IActorRdfJoinTestSideData,
} from '@comunica/bus-rdf-join';
import {
  ActorRdfJoin,
} from '@comunica/bus-rdf-join';
import type { TestResult } from '@comunica/core';
import { passTestWithSideData } from '@comunica/core';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { BindingsStream } from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import type { AsyncIterator } from 'asynciterator';
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
    const bindingsStream = <BindingsStream><unknown> new IncrementalNestedLoopJoin(
      <AsyncIterator<Bindings>><unknown>action.entries[0].output.bindingsStream,
      <AsyncIterator<Bindings>><unknown>action.entries[1].output.bindingsStream,
      <(...bindings: Bindings[]) => Bindings | null>ActorRdfJoin.joinBindings,
    );
    return {
      result: {
        type: 'bindings',
        bindingsStream,
        metadata: async() => await this.constructResultMetadata(
          action.entries,
          await ActorRdfJoin.getMetadatas(action.entries),
          action.context,
        ),
      },
    };
  }

  public async getJoinCoefficients(
    _action: IActionRdfJoin,
    sideData: IActorRdfJoinTestSideData,
  ): Promise<TestResult<IMediatorTypeJoinCoefficients, IActorRdfJoinTestSideData>> {
    return passTestWithSideData({
      iterations: 0,
      persistedItems: 0,
      blockingItems: 0,
      requestTime: 0,
    }, sideData);
  }
}
