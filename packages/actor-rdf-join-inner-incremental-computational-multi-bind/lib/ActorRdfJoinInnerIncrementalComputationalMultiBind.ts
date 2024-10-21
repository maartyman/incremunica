import type { Bindings } from '@comunica/utils-bindings-factory';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type { MediatorQueryOperation } from '@comunica/bus-query-operation';
import type {
  IActionRdfJoin,
  IActorRdfJoinArgs,
  IActorRdfJoinOutputInner,
  IActorRdfJoinTestSideData
} from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { MediatorRdfJoinEntriesSort } from '@comunica/bus-rdf-join-entries-sort';
import {KeysInitQuery, KeysQueryOperation} from '@comunica/context-entries';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type {
  BindingsStream,
  IQuerySource,
  IActionContext,
  IJoinEntryWithMetadata,
  IQueryOperationResultBindings, ComunicaDataFactory,
} from '@comunica/types';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import { KeysStreamingSource } from '@incremunica/context-entries';
import { HashBindings } from '@incremunica/hash-bindings';
import { TransformIterator, UnionIterator } from 'asynciterator';
import type { AsyncIterator } from 'asynciterator';
import type { Algebra } from 'sparqlalgebrajs';
import { Factory } from 'sparqlalgebrajs';
import {passTestWithSideData, TestResult} from "@comunica/core";
import {getSafeBindings, materializeOperation, getOperationSource} from '@comunica/utils-query-operation';
import {MediatorMergeBindingsContext} from "@comunica/bus-merge-bindings-context";

/**
 * A comunica Multi-way Bind RDF Join Actor.
 */
export class ActorRdfJoinInnerIncrementalComputationalMultiBind extends ActorRdfJoin {
  public readonly selectivityModifier: number;
  public readonly mediatorJoinEntriesSort: MediatorRdfJoinEntriesSort;
  public readonly mediatorQueryOperation: MediatorQueryOperation;
  public readonly mediatorMergeBindingsContext: MediatorMergeBindingsContext;

  public constructor(args: IActorRdfJoinInnerIncrementalComputationalMultiBindArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'bind',
      canHandleUndefs: true,
    });
  }

  public static haltSources(sources: IQuerySource[]): void {
    for (const source of sources) {
      if (typeof source !== 'string' && 'resume' in source && 'halt' in source) {
        (<any>source).halt();
      }
    }
  }

  public static resumeSources(sources: IQuerySource[]): void {
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
   * @param algebraFactory The algebra factory.
   * @param bindingsFactory The bindingsFactory created with bindings context merger.
   * @return {AsyncIterator<Bindings>}
   */
  public static async createBindStream(
    baseStream: AsyncIterator<Bindings>,
    operations: Algebra.Operation[],
    operationBinder: (boundOperations: Algebra.Operation[], operationBindings: Bindings)
    => Promise<[AsyncIterator<Bindings>, () => void]>,
    optional: boolean,
    sources: any,
    algebraFactory: Factory,
    bindingsFactory: BindingsFactory,
  ): Promise<AsyncIterator<Bindings>> {
    const transformMap = new Map<
    string,
    {
      elements: {
        iterator: AsyncIterator<Bindings>;
        stopFunction: (() => void);
      }[];
      subOperations: Algebra.Operation[];
    }
>();

    const hashBindings = new HashBindings();

    // Create bindings function
    const binder = async(
      bindings: Bindings,
      done: () => void,
      push: (i: AsyncIterator<Bindings>) => void,
    ): Promise<void> => {
      const hash = hashBindings.hash(bindings);
      let hashData = transformMap.get(hash);
      if (bindings.getContextEntry(new ActionContextKeyIsAddition())) {
        if (hashData === undefined) {
          hashData = {
            elements: [],
            subOperations: operations.map(operation => materializeOperation(
              operation,
              bindings,
              algebraFactory,
              bindingsFactory,
              { bindFilter: false },
            )),
          };
          transformMap.set(hash, hashData);
        }

        const bindingsMerger = (subBindings: Bindings): Bindings | null => {
          const subBindingsOrUndefined = subBindings.merge(bindings);
          return subBindingsOrUndefined ?? null;
        };

        const [ bindingStream, stopFunction ] = await operationBinder(hashData.subOperations, bindings);
        hashData.elements.push({
          stopFunction,
          iterator: bindingStream.map(bindingsMerger),
        });

        push(hashData.elements.at(-1)!.iterator);
        done();
        return;
      }
      if (hashData !== undefined && hashData.elements.length > 0) {
        const activeElement = hashData.elements.at(-1)!;
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
      for (const metadataVariable of entry.metadata.variables) {
        let counter = variableOccurrences[metadataVariable.variable.value];
        if (!counter) {
          counter = 0;
        }
        variableOccurrences[metadataVariable.variable.value] = ++counter;
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
    if (multiOccurrenceVariables.length === 0) {
      throw new Error(`Bind join can only join entries with at least one common variable`);
    }

    // Determine entries without common variables
    // These will be placed in the back of the sorted array
    const entriesWithoutCommonVariables: IJoinEntryWithMetadata[] = [];
    for (const entry of entries) {
      let hasCommon = false;
      for (const metadataVariable of entry.metadata.variables) {
        if (multiOccurrenceVariables.includes(metadataVariable.variable.value)) {
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

    const dataFactory: ComunicaDataFactory = action.context.getSafe(KeysInitQuery.dataFactory);
    const algebraFactory = new Factory(dataFactory);
    const bindingsFactory = await BindingsFactory.create(
      this.mediatorMergeBindingsContext,
      action.context,
      dataFactory,
    );

    for (const [ i, element ] of entries.entries()) {
      if (i !== 0) {
        element.output.bindingsStream.close();
      }
    }

    // Take the stream with the lowest cardinality
    const smallestStream: IQueryOperationResultBindings = entries[0].output;
    const remainingEntries = [ ...entries ];
    remainingEntries.splice(0, 1);

    const sources = getOperationSource(remainingEntries[0].operation);

    // Bind the remaining patterns for each binding in the stream
    const subContext = action.context
      .set(KeysQueryOperation.joinLeftMetadata, entries[0].metadata)
      .set(KeysQueryOperation.joinRightMetadatas, remainingEntries.map(entry => entry.metadata));
    const bindingsStream = <BindingsStream><unknown> await ActorRdfJoinInnerIncrementalComputationalMultiBind
      .createBindStream(
        <AsyncIterator<Bindings>><unknown>smallestStream.bindingsStream,
        remainingEntries.map(entry => entry.operation),
        async(operations: Algebra.Operation[], operationBindings: Bindings) => {
          // Send the materialized patterns to the mediator for recursive join evaluation.
          const matchOptions: ({ stopMatch: () => void })[] = [];
          const currentSubContext = subContext
            .set(KeysQueryOperation.joinBindings, operationBindings)
            .set(KeysStreamingSource.matchOptions, matchOptions);
          const operation = operations.length === 1 ?
            operations[0] :
            algebraFactory.createJoin(operations);
          const output = getSafeBindings(await this.mediatorQueryOperation.mediate(
            { operation, context: currentSubContext },
          ));
          const stopFunction = (): void => {
            for (const MatchOption of matchOptions) {
              MatchOption.stopMatch();
            }
          };

          return [ <AsyncIterator<Bindings>><unknown>output.bindingsStream, stopFunction ];
        },
        false,
        sources ?? [],
        algebraFactory,
        bindingsFactory
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
