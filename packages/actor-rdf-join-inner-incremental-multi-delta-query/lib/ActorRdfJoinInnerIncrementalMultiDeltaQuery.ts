import type {
  IActionRdfJoin,
  IActorRdfJoinArgs,
  IActorRdfJoinOutputInner, IActorRdfJoinTestSideData,
} from '@comunica/bus-rdf-join';
import {
  ActorRdfJoin,
} from '@comunica/bus-rdf-join';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type {BindingsStream, ComunicaDataFactory} from '@comunica/types';
import { KeysDeltaQueryJoin } from '@incremunica/context-entries';
import { DeltaQueryIterator } from './DeltaQueryIterator';
import {passTestWithSideData, TestResult} from "@comunica/core";
import {KeysInitQuery} from "@comunica/context-entries";
import {MediatorMergeBindingsContext} from "@comunica/bus-merge-bindings-context";
import {BindingsFactory} from "@comunica/utils-bindings-factory";
import {MediatorQueryOperation} from "@comunica/bus-query-operation";
import {Factory} from "sparqlalgebrajs";

/**
 * A comunica Inner Incremental Nestedloop RDF Join Actor.
 */
export class ActorRdfJoinInnerIncrementalMultiDeltaQuery extends ActorRdfJoin {
  public readonly selectivityModifier: number;
  public readonly mediatorQueryOperation: MediatorQueryOperation;
  public readonly mediatorMergeBindingsContext: MediatorMergeBindingsContext;

  public constructor(args: IActorRdfJoinMultiDeltaQueryArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'multi-delta-query',
      canHandleUndefs: true,
    });
  }

  public async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const dataFactory: ComunicaDataFactory = action.context.getSafe(KeysInitQuery.dataFactory);
    const algebraFactory = new Factory(dataFactory);
    const bindingsFactory = await BindingsFactory.create(
      this.mediatorMergeBindingsContext,
      action.context,
      dataFactory,
    );
    const bindingsStream = <BindingsStream><unknown> new DeltaQueryIterator(
      action.entries,
      this.mediatorQueryOperation,
      dataFactory,
      algebraFactory,
      bindingsFactory
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
    action: IActionRdfJoin,
    sideData: IActorRdfJoinTestSideData,
  ): Promise<TestResult<IMediatorTypeJoinCoefficients, IActorRdfJoinTestSideData>> {
    // Throw when the previous join was a delta query join or when it is a static query
    if (action.context
      .get(KeysDeltaQueryJoin.fromDeltaQuery)) {
      throw new Error('Can\'t do two delta query joins after each other');
    }
    return passTestWithSideData({
      iterations: 0,
      persistedItems: 0,
      blockingItems: 0,
      requestTime: 0,
    }, sideData);
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
   * The query operation mediator
   */
  mediatorQueryOperation: MediatorQueryOperation;
  /**
   * The merge bindings context mediator
   */
  mediatorMergeBindingsContext: MediatorMergeBindingsContext;
}
