import type { MediatorQueryOperation } from '@comunica/bus-query-operation';
import { ActorQueryOperation, materializeOperation } from '@comunica/bus-query-operation';
import type { IActionRdfJoin, IActorRdfJoinArgs, IActorRdfJoinOutputInner } from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { MediatorRdfJoinEntriesSort } from '@comunica/bus-rdf-join-entries-sort';
import { getContextSources } from '@comunica/bus-rdf-resolve-quad-pattern';
import { KeysQueryOperation } from '@comunica/context-entries';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type {
  BindingsStream,
  DataSources,
  IActionContext,
  IJoinEntryWithMetadata,
  IQueryOperationResultBindings,
  MetadataBindings,
} from '@comunica/types';
import { KeysStreamingSource } from '@incremunica/context-entries';
import { HashBindings } from '@incremunica/hash-bindings';
import type { Bindings } from '@incremunica/incremental-bindings-factory';
import type { AsyncIterator } from 'asynciterator';
import { TransformIterator, UnionIterator } from 'asynciterator';
import type { Algebra } from 'sparqlalgebrajs';
import { Factory } from 'sparqlalgebrajs';

/**
 * A comunica Multi-way Bind RDF Join Actor.
 */
export class ActorRdfJoinInnerIncrementalComputationalMultiBind extends ActorRdfJoin {
  public readonly selectivityModifier: number;
  public readonly mediatorJoinEntriesSort: MediatorRdfJoinEntriesSort;
  public readonly mediatorQueryOperation: MediatorQueryOperation;

  public static readonly FACTORY = new Factory();

  public constructor(args: IActorRdfJoinInnerIncrementalComputationalMultiBindArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'bind',
      canHandleUndefs: true,
    });
  }

  public static haltSources(sources: DataSources): void {
    for (const source of sources) {
      if (typeof source !== 'string' && 'resume' in source && 'halt' in source) {
        (<any>source).halt();
      }
    }
  }

  public static resumeSources(sources: DataSources): void {
    for (const source of sources) {
      if (typeof source !== 'string' && 'resume' in source && 'halt' in source) {
        (<any>source).resume();
      }
    }
  }

  /**
   * Create a new bindings stream that takes every binding of the base stream
   * and binds it to the remaining patterns, evaluates those patterns, and emits all their bindings.
   *
   * @param baseStream The base stream.
   * @param operations The operations to bind with each binding of the base stream.
   * @param operationBinder A callback to retrieve the bindings stream of bound operations.
   * @param optional If the original bindings should be emitted when the resulting bindings stream is empty.
   * @param sources The sources of the query.
   * @return {BindingsStream}
   */
  public static async createBindStream(
    baseStream: BindingsStream,
    operations: Algebra.Operation[],
    operationBinder: (boundOperations: Algebra.Operation[], operationBindings: Bindings)
    => Promise<[BindingsStream, () => void]>,
    optional: boolean,
    sources: DataSources,
  ): Promise<BindingsStream> {
    const transformMap = new Map<
    string,
    {
      elements: {
        iterator: AsyncIterator<Bindings>;
        stopFunction: (() => void);
      }[];
      subOperations: Algebra.Operation[];
    }>();

    const hashBindings = new HashBindings();

    // Create bindings function
    const binder = async(bindings: Bindings, done: () => void, push: (i: BindingsStream) => void): Promise<void> => {
      const hash = hashBindings.hash(bindings);
      let hashData = transformMap.get(hash);
      if (bindings.diff) {
        if (hashData === undefined) {
          hashData = {
            elements: [],
            subOperations: operations.map(operation => materializeOperation(
              operation,
              bindings,
              { bindFilter: false },
            )),
          };
          transformMap.set(hash, hashData);
        }

        const bindingsMerger = (subBindings: Bindings): Bindings | null => {
          const subBindingsOrUndefined = subBindings.merge(bindings);
          return subBindingsOrUndefined === undefined ? null : subBindingsOrUndefined;
        };

        const [ bindingStream, stopFunction ] = await operationBinder(hashData.subOperations, bindings);
        hashData.elements.push({
          stopFunction,
          iterator: bindingStream.map(bindingsMerger),
        });

        push(hashData.elements[hashData.elements.length - 1].iterator);
        done();
        return;
      }
      if (hashData !== undefined && hashData.elements.length > 0) {
        const activeElement = hashData.elements[hashData.elements.length - 1];
        hashData.elements.pop();

        ActorRdfJoinInnerIncrementalComputationalMultiBind.haltSources(sources);

        let activeIteratorStopped = false;
        let newIteratorStopped = false;

        activeElement.iterator.on('end', () => {
          activeIteratorStopped = true;
          if (newIteratorStopped) {
            ActorRdfJoinInnerIncrementalComputationalMultiBind.resumeSources(sources);
          }
        });
        activeElement.stopFunction();

        const bindingsMerger = (subBindings: Bindings): Bindings | undefined => subBindings.merge(bindings);

        const [ newIterator, newStopFunction ] = await operationBinder(hashData.subOperations, bindings);
        newStopFunction();
        newIterator.on('end', () => {
          newIteratorStopped = true;
          if (activeIteratorStopped) {
            ActorRdfJoinInnerIncrementalComputationalMultiBind.resumeSources(sources);
          }
        });

        if (hashData.elements.length === 0) {
          transformMap.delete(hash);
        }

        push(new TransformIterator(
          () => newIterator.map(bindingsMerger),
          { maxBufferSize: 128, autoStart: false },
        ));
        done();
        return;
      }
      done();
    };

    return new UnionIterator(baseStream.transform({
      transform: binder,
      optional,
    }), { autoStart: false });
  }

  /**
   * Order the given join entries using the join-entries-sort bus.
   * @param {IJoinEntryWithMetadata[]} entries An array of join entries.
   * @param context The action context.
   * @return {IJoinEntryWithMetadata[]} The sorted join entries.
   */
  public async sortJoinEntries(
    entries: IJoinEntryWithMetadata[],
    context: IActionContext,
  ): Promise<IJoinEntryWithMetadata[]> {
    // If there is a stream that can contain undefs, we don't modify the join order.
    const canContainUndefs = entries.some(entry => entry.metadata.canContainUndefs);
    if (canContainUndefs) {
      return entries;
    }

    // Calculate number of occurrences of each variable
    const variableOccurrences: Record<string, number> = {};
    for (const entry of entries) {
      for (const variable of entry.metadata.variables) {
        let counter = variableOccurrences[variable.value];
        if (!counter) {
          counter = 0;
        }
        variableOccurrences[variable.value] = ++counter;
      }
    }

    // Determine variables that occur in at least two join entries
    const multiOccurrenceVariables: string[] = [];
    for (const [ variable, count ] of Object.entries(variableOccurrences)) {
      if (count >= 2) {
        multiOccurrenceVariables.push(variable);
      }
    }

    // Reject if no entries have common variables
    // if (multiOccurrenceVariables.length === 0) {
    // throw new Error(`Bind join can only join entries with at least one common variable`);
    // }

    // Determine entries without common variables
    // These will be placed in the back of the sorted array
    const entriesWithoutCommonVariables: IJoinEntryWithMetadata[] = [];
    for (const entry of entries) {
      let hasCommon = false;
      for (const variable of entry.metadata.variables) {
        if (multiOccurrenceVariables.includes(variable.value)) {
          hasCommon = true;
          break;
        }
      }
      if (!hasCommon) {
        entriesWithoutCommonVariables.push(entry);
      }
    }

    return (await this.mediatorJoinEntriesSort.mediate({ entries, context })).entries
      .sort((entryLeft, entryRight) => {
        // Sort to make sure that entries without common variables come last in the array.
        // For all other entries, the original order is kept.
        const leftWithoutCommonVariables = entriesWithoutCommonVariables.includes(entryLeft);
        const rightWithoutCommonVariables = entriesWithoutCommonVariables.includes(entryRight);
        if (leftWithoutCommonVariables === rightWithoutCommonVariables) {
          return 0;
        }
        return leftWithoutCommonVariables ?
          1 :
          -1;
      });
  }

  public async getOutput(action: IActionRdfJoin): Promise<IActorRdfJoinOutputInner> {
    // Order the entries so we can pick the first one (usually the one with the lowest cardinality)
    const entriesUnsorted = await ActorRdfJoin.getEntriesWithMetadatas(action.entries);
    const entries = await this.sortJoinEntries(entriesUnsorted, action.context);

    for (const [ i, element ] of entries.entries()) {
      if (i !== 0) {
        element.output.bindingsStream.close();
      }
    }

    // Take the stream with the lowest cardinality
    const smallestStream: IQueryOperationResultBindings = entries[0].output;
    const remainingEntries = [ ...entries ];
    remainingEntries.splice(0, 1);

    const sources = getContextSources(action.context);

    // Bind the remaining patterns for each binding in the stream
    const subContext = action.context
      .set(KeysQueryOperation.joinLeftMetadata, entries[0].metadata)
      .set(KeysQueryOperation.joinRightMetadatas, remainingEntries.map(entry => entry.metadata));
    const bindingsStream: BindingsStream = await ActorRdfJoinInnerIncrementalComputationalMultiBind.createBindStream(
      smallestStream.bindingsStream,
      remainingEntries.map(entry => entry.operation),
      async(operations: Algebra.Operation[], operationBindings: Bindings) => {
        // Send the materialized patterns to the mediator for recursive join evaluation.
        const matchOptions: ({ stopMatch: () => void })[] = [];
        const currentSubContext = subContext
          .set(KeysQueryOperation.joinBindings, operationBindings)
          .set(KeysStreamingSource.matchOptions, matchOptions);
        const operation = operations.length === 1 ?
          operations[0] :
          ActorRdfJoinInnerIncrementalComputationalMultiBind.FACTORY.createJoin(operations);
        const output = ActorQueryOperation.getSafeBindings(await this.mediatorQueryOperation.mediate(
          { operation, context: currentSubContext },
        ));
        const stopFunction = (): void => {
          for (const MatchOption of matchOptions) {
            MatchOption.stopMatch();
          }
        };

        return [ output.bindingsStream, stopFunction ];
      },
      false,
      sources === undefined ? [] : sources,
    );

    return {
      result: {
        type: 'bindings',
        bindingsStream,
        metadata: () => this.constructResultMetadata(entries, entries.map(entry => entry.metadata), action.context),
      },
      physicalPlanMetadata: {
        bindIndex: entriesUnsorted.indexOf(entries[0]),
      },
    };
  }

  public async getJoinCoefficients(
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

export interface IActorRdfJoinInnerIncrementalComputationalMultiBindArgs extends IActorRdfJoinArgs {
  /**
   * Multiplier for selectivity values
   * @range {double}
   * @default {0.0001}
   */
  selectivityModifier: number;
  /**
   * The join entries sort mediator
   */
  mediatorJoinEntriesSort: MediatorRdfJoinEntriesSort;
  /**
   * The query operation mediator
   */
  mediatorQueryOperation: MediatorQueryOperation;
}
