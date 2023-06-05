import { ActorQueryOperation } from '@comunica/bus-query-operation';
import type {
  IActionRdfJoin,
  IActorRdfJoinArgs,
  IActorRdfJoinOutputInner,
  MediatorRdfJoin,
} from '@comunica/bus-rdf-join';
import {
  ActorRdfJoin,
} from '@comunica/bus-rdf-join';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { BindingsStream, MetadataBindings } from '@comunica/types';
import { UnionIterator } from 'asynciterator';
import { Algebra, Factory } from 'sparqlalgebrajs';

/**
 * A comunica Optional Differential Bind RDF Join Actor.
 */
export class ActorRdfJoinOptionalIncrementalUnion extends ActorRdfJoin {
  public readonly selectivityModifier: number;
  public readonly mediatorRdfJoin: MediatorRdfJoin;

  public static readonly FACTORY = new Factory();

  public constructor(args: IActorRdfJoinOptionalIncrementalUnion) {
    super(args, {
      logicalType: 'optional',
      physicalName: 'incremental-union',
      limitEntries: 2,
      canHandleUndefs: false,
    });
  }

  protected async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const metadatas = await ActorRdfJoin.getMetadatas(action.entries);
    const variables = ActorRdfJoin.overlappingVariables(metadatas);

    const joinAction: IActionRdfJoin = {
      type: 'inner',
      context: action.context,
      entries: action.entries.map(entry => {
        entry.output.bindingsStream = entry.output.bindingsStream.clone();
        return entry;
      }),
    };
    const join: BindingsStream = (await this.mediatorRdfJoin.mediate(
      joinAction,
    )).bindingsStream;

    const minusAction: IActionRdfJoin = {
      type: 'minus',
      context: action.context,
      entries: action.entries.map(entry => {
        entry.output.bindingsStream = entry.output.bindingsStream.clone();
        return entry;
      }),
    };
    let minus: BindingsStream;
    if (variables.length > 0) {
      minus = (await this.mediatorRdfJoin.mediate(
        minusAction,
      )).bindingsStream;
    } else {
      minus = action.entries[0].output.bindingsStream.clone();
    }

    const union = new UnionIterator([ join, minus ], { autoStart: false });

    return {
      result: {
        type: 'bindings',
        bindingsStream: <BindingsStream><any>union,
        metadata: async() => await this.constructResultMetadata(action.entries, metadatas, action.context),
      },
    };
  }

  public async getJoinCoefficients(
    action: IActionRdfJoin,
    metadatas: MetadataBindings[],
  ): Promise<IMediatorTypeJoinCoefficients> {
    const requestInitialTimes = ActorRdfJoin.getRequestInitialTimes(metadatas);
    const requestItemTimes = ActorRdfJoin.getRequestItemTimes(metadatas);

    // Reject binding on some operation types
    if (action.entries[1].operation.type === Algebra.types.EXTEND ||
      action.entries[1].operation.type === Algebra.types.GROUP) {
      throw new Error(`Actor ${this.name} can not bind on Extend and Group operations`);
    }

    // Determine selectivity of join
    const selectivity = (await this.mediatorJoinSelectivity.mediate({
      entries: action.entries,
      context: action.context,
    })).selectivity * this.selectivityModifier;

    return {
      iterations: metadatas[0].cardinality.value * metadatas[1].cardinality.value * selectivity,
      persistedItems: 0,
      blockingItems: 0,
      requestTime: requestInitialTimes[0] +
        metadatas[0].cardinality.value * selectivity * (
          requestItemTimes[0] +
          requestInitialTimes[1] +
          metadatas[1].cardinality.value * requestItemTimes[1]
        ),
    };
  }
}

export interface IActorRdfJoinOptionalIncrementalUnion extends IActorRdfJoinArgs {
  /**
   * Multiplier for selectivity values
   * @range {double}
   * @default {0.0001}
   */
  selectivityModifier: number;
  /**
   * The Join mediator
   */
  mediatorRdfJoin: MediatorRdfJoin;
}
