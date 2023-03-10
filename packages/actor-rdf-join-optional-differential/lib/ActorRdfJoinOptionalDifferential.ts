import {
  ActorRdfJoin,
  IActionRdfJoin,
  IActorRdfJoinOutputInner
} from '@comunica/bus-rdf-join';
import {BindOrder} from "@comunica/actor-rdf-join-inner-multi-bind";
import {MediatorQueryOperation} from "@comunica/bus-query-operation";
import {IActorRdfJoinOptionalBindArgs} from "@comunica/actor-rdf-join-optional-bind";
import {BindingsStream, MetadataBindings} from "@comunica/types";
import {
  DifferentialMinusHashIterator
} from "@comunica/actor-rdf-join-minus-differential-hash/lib/DifferentialMinusHashIterator";
import {
  DifferentialSymmetricHashJoin
} from "@comunica/actor-rdf-join-inner-differential-symmetrichash/lib/DifferentialSymmetricHashJoin";
import {UnionIterator} from "asynciterator";
import {IMediatorTypeJoinCoefficients} from "@comunica/mediatortype-join-coefficients";
import {Algebra} from "sparqlalgebrajs";

/**
 * A comunica Optional Differential Bind RDF Join Actor.
 */
export class ActorRdfJoinOptionalDifferential extends ActorRdfJoin {
  public readonly bindOrder: BindOrder;
  public readonly selectivityModifier: number;
  public readonly mediatorQueryOperation: MediatorQueryOperation;

  public constructor(args: IActorRdfJoinOptionalBindArgs) {
    super(args, {
      logicalType: 'optional',
      physicalName: 'differential-bind',
      limitEntries: 2,
      canHandleUndefs: false,
    });
  }

  protected async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const metadatas = await ActorRdfJoin.getMetadatas(action.entries);
    const variables = ActorRdfJoin.overlappingVariables(metadatas);

    const join: BindingsStream = new DifferentialSymmetricHashJoin(
      action.entries[0].output.bindingsStream.clone(),
      action.entries[1].output.bindingsStream.clone(),
      entry => ActorRdfJoinOptionalDifferential.hash(entry, variables),
      <any> ActorRdfJoin.joinBindings,
    );

    let minus: BindingsStream;
    if (variables.length > 0) {
      minus = new DifferentialMinusHashIterator(
        action.entries[0].output.bindingsStream.clone(),
        action.entries[1].output.bindingsStream.clone(),
        variables,
        ActorRdfJoin.hash
      );
    }
    else {
      minus = action.entries[0].output.bindingsStream.clone();
    }

    const union = new UnionIterator([join, minus], {autoStart: false});

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
