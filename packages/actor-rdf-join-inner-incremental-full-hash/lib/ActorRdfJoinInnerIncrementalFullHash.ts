import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
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
import { IncrementalFullHashJoin } from './IncrementalFullHashJoin';

/**
 * A comunica Inner Incremental Full Hash RDF Join Actor.
 */
export class ActorRdfJoinInnerIncrementalFullHash extends ActorRdfJoin {
  public readonly mediatorHashBindings: MediatorHashBindings;

  public constructor(args: IActorRdfJoinInnerIncrementalFullHashArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'full-hash',
      limitEntries: 2,
      canHandleUndefs: false,
    });
  }

  protected async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const metadatas = await ActorRdfJoin.getMetadatas(action.entries);
    const commonVariables = ActorRdfJoin.overlappingVariables(metadatas).map(v => v.variable);
    const { hashFunction } = await this.mediatorHashBindings.mediate({ context: action.context });
    const bindingsStream = <BindingsStream><any> new IncrementalFullHashJoin(
      <AsyncIterator<Bindings>><unknown>action.entries[0].output.bindingsStream,
      <AsyncIterator<Bindings>><unknown>action.entries[1].output.bindingsStream,
      <(...bindings: Bindings[]) => Bindings | null>ActorRdfJoin.joinBindings,
      entry => hashFunction(entry, commonVariables),
      entry => hashFunction(entry, metadatas[0].variables.map(v => v.variable)),
      entry => hashFunction(entry, metadatas[1].variables.map(v => v.variable)),
    );
    return {
      result: {
        type: 'bindings',
        bindingsStream,
        metadata: async() => await this.constructResultMetadata(
          action.entries,
          metadatas,
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
export interface IActorRdfJoinInnerIncrementalFullHashArgs extends IActorRdfJoinArgs {
  mediatorHashBindings: MediatorHashBindings;
}
