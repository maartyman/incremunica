import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import type {
  IActionRdfJoin,
  IActorRdfJoinArgs,
  IActorRdfJoinOutputInner,
  IActorRdfJoinTestSideData,
} from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { TestResult } from '@comunica/core';
import { passTestWithSideData } from '@comunica/core';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { BindingsStream } from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import type { AsyncIterator } from 'asynciterator';
import { MinusHash } from './MinusHash';

/**
 * An Incremunica Minus Hash RDF Join Actor.
 */
export class ActorRdfJoinMinusHash extends ActorRdfJoin {
  public readonly mediatorHashBindings: MediatorHashBindings;

  public constructor(args: IActorRdfJoinMinusHashArgs) {
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
    const commonVariables = ActorRdfJoin.overlappingVariables(metadatas).map(v => v.variable);

    // Destroy the buffer stream since it is not needed when
    // there are no common variables.
    if (commonVariables.length === 0) {
      buffer.bindingsStream.destroy();
      return { result: output };
    }
    const { hashFunction } = await this.mediatorHashBindings.mediate({ context: action.context });
    const bindingsStream = <BindingsStream><unknown> new MinusHash(
        <AsyncIterator<Bindings>><unknown>output.bindingsStream,
        <AsyncIterator<Bindings>><unknown>buffer.bindingsStream,
        entry => hashFunction(entry, commonVariables),
    );
    return {
      result: {
        type: 'bindings',
        bindingsStream,
        metadata: output.metadata,
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

export interface IActorRdfJoinMinusHashArgs extends IActorRdfJoinArgs {
  mediatorHashBindings: MediatorHashBindings;
}
