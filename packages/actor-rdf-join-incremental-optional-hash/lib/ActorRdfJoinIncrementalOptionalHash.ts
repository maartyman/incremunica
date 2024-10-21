import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import type { Bindings } from '@comunica/utils-bindings-factory';
import type {
  IActionRdfJoin,
  IActorRdfJoinArgs,
  IActorRdfJoinOutputInner,
  IActorRdfJoinTestSideData
} from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { MetadataBindings, BindingsStream } from '@comunica/types';
import type { AsyncIterator } from 'asynciterator';
import { IncrementalOptionalHash } from './IncrementalOptionalHash';
import {passTestWithSideData, TestResult} from "@comunica/core";

/**
 * An Incremunica Optional Hash RDF Join Actor.
 */
export class ActorRdfJoinIncrementalOptionalHash extends ActorRdfJoin {
  public readonly mediatorHashBindings: MediatorHashBindings;

  public constructor(args: IActorRdfJoinIncrementalOptionalHashArgs) {
    super(args, {
      logicalType: 'optional',
      physicalName: 'hash',
      limitEntries: 2,
      canHandleUndefs: false,
    });
  }

  protected async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const metadatas = await ActorRdfJoin.getMetadatas(action.entries);
    const commonVariables = ActorRdfJoin.overlappingVariables(metadatas).map(v => v.variable);
    const { hashFunction } = await this.mediatorHashBindings.mediate({ context: action.context });
    const bindingsStream = <BindingsStream><unknown> new IncrementalOptionalHash(
      <AsyncIterator<Bindings>><unknown>action.entries[0].output.bindingsStream,
      <AsyncIterator<Bindings>><unknown>action.entries[1].output.bindingsStream,
      <(...bindings: Bindings[]) => Bindings | null>ActorRdfJoin.joinBindings,
      entry => hashFunction(entry, commonVariables),
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

  protected async getJoinCoefficients(
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

export interface IActorRdfJoinIncrementalOptionalHashArgs extends IActorRdfJoinArgs {
  mediatorHashBindings: MediatorHashBindings;
}
