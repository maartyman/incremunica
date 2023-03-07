import {
  ActorRdfJoin,
  IActionRdfJoin,
  IActorRdfJoinArgs,
  IActorRdfJoinOutputInner
} from '@comunica/bus-rdf-join';
import {DifferentialSymmetricHashJoin} from "./DifferentialSymmetricHashJoin";
import {BindingsStream, MetadataBindings} from "@comunica/types";
import {IMediatorTypeJoinCoefficients} from "@comunica/mediatortype-join-coefficients";

/**
 * A comunica Inner Differential Symmetrichash RDF Join Actor.
 */
export class ActorRdfJoinInnerDifferentialSymmetrichash extends ActorRdfJoin {
  public constructor(args: IActorRdfJoinArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'differential-symmetric-hash',
      limitEntries: 2,
    });
  }

  public async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    const metadatas = await ActorRdfJoin.getMetadatas(action.entries);
    const variables = ActorRdfJoin.overlappingVariables(metadatas);
    const join = new DifferentialSymmetricHashJoin(
      action.entries[0].output.bindingsStream,
      action.entries[1].output.bindingsStream,
      entry => ActorRdfJoinInnerDifferentialSymmetrichash.hash(entry, variables),
      <any> ActorRdfJoin.joinBindings,
    );
    return {
      result: {
        type: 'bindings',
        bindingsStream: <BindingsStream><any>join,
        metadata: async() => await this.constructResultMetadata(action.entries, metadatas, action.context),
      },
    };
  }

  protected async getJoinCoefficients(
    action: IActionRdfJoin,
    metadatas: MetadataBindings[],
  ): Promise<IMediatorTypeJoinCoefficients> {
    const requestInitialTimes = ActorRdfJoin.getRequestInitialTimes(metadatas);
    const requestItemTimes = ActorRdfJoin.getRequestItemTimes(metadatas);
    return {
      iterations: metadatas[0].cardinality.value + metadatas[1].cardinality.value,
      persistedItems: metadatas[0].cardinality.value + metadatas[1].cardinality.value,
      blockingItems: 0,
      requestTime: requestInitialTimes[0] + metadatas[0].cardinality.value * requestItemTimes[0] +
        requestInitialTimes[1] + metadatas[1].cardinality.value * requestItemTimes[1],
    };
  }
}
