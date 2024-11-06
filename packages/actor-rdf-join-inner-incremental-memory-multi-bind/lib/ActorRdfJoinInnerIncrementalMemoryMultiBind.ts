import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import type { MediatorMergeBindingsContext } from '@comunica/bus-merge-bindings-context';
import type { MediatorQueryOperation } from '@comunica/bus-query-operation';
import type {
  IActionRdfJoin,
  IActorRdfJoinOutputInner,
  IActorRdfJoinArgs,
  IActorRdfJoinTestSideData,
} from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { MediatorRdfJoinEntriesSort } from '@comunica/bus-rdf-join-entries-sort';
import { KeysInitQuery, KeysQueryOperation } from '@comunica/context-entries';
import type { TestResult } from '@comunica/core';
import { failTest, passTest, passTestWithSideData } from '@comunica/core';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type {
  BindingsStream,
  IQueryOperationResultBindings,
  IActionContext,
  IJoinEntryWithMetadata,
  ComunicaDataFactory,
} from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { materializeOperation, getSafeBindings } from '@comunica/utils-query-operation';
import { KeysBindings } from '@incremunica/context-entries';
import {
  ArrayIterator,
  EmptyIterator,
  UnionIterator,
} from 'asynciterator';
import type { AsyncIterator } from 'asynciterator';
import type { Algebra } from 'sparqlalgebrajs';
import { Factory } from 'sparqlalgebrajs';

/**
 * A comunica Multi-way Bind RDF Join Actor.
 */
export class ActorRdfJoinInnerIncrementalMemoryMultiBind extends ActorRdfJoin {
  public readonly selectivityModifier: number;
  public readonly mediatorJoinEntriesSort: MediatorRdfJoinEntriesSort;
  public readonly mediatorQueryOperation: MediatorQueryOperation;
  public readonly mediatorMergeBindingsContext: MediatorMergeBindingsContext;
  public readonly mediatorHashBindings: MediatorHashBindings;

  public constructor(args: IActorRdfJoinInnerIncrementalMemoryMultiBindArgs) {
    super(args, {
      logicalType: 'inner',
      physicalName: 'bind',
      canHandleUndefs: true,
    });
  }

  /**
   * Create a new bindings stream that takes every binding of the base stream
   * and binds it to the remaining patterns, evaluates those patterns, and emits all their bindings.
   *
   * @param baseStream The base stream.
   * @param operations The operations to bind with each binding of the base stream.
   * @param operationBinder A callback to retrieve the bindings stream of bound operations.
   * @param optional If the original bindings should be emitted when the resulting bindings stream is empty.
   * @param algebraFactory The algebra factory.
   * @param bindingsFactory The bindingsFactory created with bindings context merger.
   * @param hashBindings A function that hashes bindings.
   * @return {AsyncIterator<Bindings>}
   */
  public static async createBindStream(
    baseStream: AsyncIterator<Bindings>,
    operations: Algebra.Operation[],
    operationBinder: (boundOperations: Algebra.Operation[], operationBindings: Bindings)
    => Promise<AsyncIterator<Bindings>>,
    optional: boolean,
    algebraFactory: Factory,
    bindingsFactory: BindingsFactory,
    hashBindings: (bindings: Bindings) => number,
  ): Promise<AsyncIterator<Bindings>> {
    const transformMap = new Map<
      number,
      {
        iterator: AsyncIterator<Bindings>;
        memory: Map<
          number,
          {
            bindings: Bindings;
            count: number;
          }
        >;
        count: number;
      }
    >();
    // Create bindings function
    const binder = (bindings: Bindings, done: () => void, push: (i: AsyncIterator<Bindings>) => void): void => {
      const hash = hashBindings(bindings);
      if (bindings) {
        const hashData = transformMap.get(hash);
        if (hashData === undefined) {
          const data = {
            iterator: new EmptyIterator<Bindings>(),
            memory: new Map<number, { bindings: Bindings; count: number }>(),
            count: 1,
          };
          transformMap.set(hash, data);

          // We don't bind the filter because filters are always handled last,
          // and we need to avoid binding filters of sub-queries, which are to be handled first. (see spec test bind10)
          const subOperations = operations
            .map(operation => materializeOperation(
              operation,
              bindings,
              algebraFactory,
              bindingsFactory,
              { bindFilter: false },
            ));

          const transformFunc = (subBindings: Bindings, subDone: () => void, subPush: (i: Bindings) => void): void => {
            const newBindings = subBindings.merge(bindings);
            if (newBindings === undefined) {
              subDone();
              return;
            }
            const bindingsHash = hashBindings(newBindings);
            const bindingsData = data.memory.get(bindingsHash);
            if (newBindings.getContextEntry(KeysBindings.isAddition)) {
              if (bindingsData === undefined) {
                data.memory.set(bindingsHash, { bindings: newBindings, count: 1 });
              } else {
                bindingsData.count++;
              }
              for (let i = 0; i < data.count; i++) {
                subPush(newBindings);
              }
            } else if (bindingsData !== undefined) {
              if (bindingsData.count > 1) {
                bindingsData.count--;
              } else {
                data.memory.delete(bindingsHash);
              }
              for (let i = 0; i < data.count; i++) {
                subPush(newBindings);
              }
            }
            subDone();
          };

          // eslint-disable-next-line ts/no-floating-promises
          operationBinder(subOperations, bindings).then((bindingStream) => {
            const transformIterator = bindingStream.transform({
              transform: transformFunc,
            });

            // Maybe by the time operationBinder has finished the current bindings already has been deleted
            // => count should be 0 then
            // if (data.count == 0) {
            //  transformIterator.destroy();
            //  done();
            //  return;
            // }

            data.iterator = transformIterator;

            push(transformIterator);
            done();
          });
        } else {
          hashData.count++;
          push(new ArrayIterator(hashData.memory.values()).transform({
            transform(item, arrayDone, arrayPush) {
              for (let i = 0; i < item.count; i++) {
                arrayPush(item.bindings);
              }
              arrayDone();
            },
          }));
          done();
        }
      } else {
        const hashData = transformMap.get(hash);
        if (hashData !== undefined) {
          if (hashData.count < 2) {
            hashData.iterator.close();
            hashData.count = 0;
            transformMap.delete(hash);
          } else {
            hashData.count--;
          }
          if (hashData.memory.size > 0) {
            push(new ArrayIterator(hashData.memory.values()).transform({
              transform(item, arrayDone, arrayPush) {
                let transformBindings = bindingsFactory.fromBindings(item.bindings);
                transformBindings = transformBindings.setContextEntry(KeysBindings.isAddition, false);
                for (let i = 0; i < item.count; i++) {
                  arrayPush(transformBindings);
                }
                arrayDone();
              },
            }));
          }
        }
        done();
      }
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
  ): Promise<TestResult<IJoinEntryWithMetadata[]>> {
    // If there is a stream that can contain undefs, we don't modify the join order.
    const hasUndefVars = entries.some(entry => entry.metadata.variables.some(variable => variable.canBeUndef));
    if (hasUndefVars) {
      return passTest(entries);
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
      return failTest(`Bind join can only join entries with at least one common variable`);
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

    return passTest((await this.mediatorJoinEntriesSort.mediate({ entries, context })).entries
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
      }));
  }

  public async getOutput(
    action: IActionRdfJoin,
    sideData: IActorRdfJoinIncrementalMemoryMultiBindTestSideData,
  ): Promise<IActorRdfJoinOutputInner> {
    const dataFactory: ComunicaDataFactory = action.context.getSafe(KeysInitQuery.dataFactory);
    const algebraFactory = new Factory(dataFactory);
    const bindingsFactory = await BindingsFactory.create(
      this.mediatorMergeBindingsContext,
      action.context,
      dataFactory,
    );

    const { hashFunction } = await this.mediatorHashBindings.mediate({ context: action.context });

    for (const [ i, element ] of sideData.entriesSorted.entries()) {
      if (i !== 0) {
        element.output.bindingsStream.close();
      }
    }

    // Take the stream with the lowest cardinality
    const smallestStream: IQueryOperationResultBindings = sideData.entriesSorted[0].output;
    const remainingEntries = [ ...sideData.entriesSorted ];
    remainingEntries.splice(0, 1);

    // Bind the remaining patterns for each binding in the stream
    const subContext = action.context
      .set(KeysQueryOperation.joinLeftMetadata, sideData.entriesSorted[0].metadata)
      .set(KeysQueryOperation.joinRightMetadatas, remainingEntries.map(entry => entry.metadata));
    const bindingsStream = <BindingsStream><unknown> await ActorRdfJoinInnerIncrementalMemoryMultiBind.createBindStream(
      <AsyncIterator<Bindings>><unknown>smallestStream.bindingsStream,
      remainingEntries.map(entry => entry.operation),
      async(operations: Algebra.Operation[], operationBindings: Bindings) => {
        // Send the materialized patterns to the mediator for recursive join evaluation.
        const operation = operations.length === 1 ?
          operations[0] :
          algebraFactory.createJoin(operations);
        const output = getSafeBindings(await this.mediatorQueryOperation.mediate(
          { operation, context: subContext?.set(KeysQueryOperation.joinBindings, operationBindings) },
        ));
        return <AsyncIterator<Bindings>><unknown>output.bindingsStream;
      },
      false,
      algebraFactory,
      bindingsFactory,
      entry => hashFunction(entry, sideData.entriesSorted[0].metadata.variables.map(v => v.variable)),
    );

    return {
      result: {
        type: 'bindings',
        bindingsStream,
        metadata: () => this.constructResultMetadata(
          sideData.entriesSorted,
          sideData.entriesSorted.map(entry => entry.metadata),
          action.context,
        ),
      },
      physicalPlanMetadata: {
        bindIndex: sideData.entriesUnsorted.indexOf(sideData.entriesSorted[0]),
      },
    };
  }

  public async getJoinCoefficients(
    action: IActionRdfJoin,
    sideData: IActorRdfJoinTestSideData,
  ): Promise<TestResult<IMediatorTypeJoinCoefficients, IActorRdfJoinIncrementalMemoryMultiBindTestSideData>> {
    const { metadatas } = sideData;
    // Order the entries so we can pick the first one (usually the one with the lowest cardinality)
    const entriesUnsorted = action.entries
      .map((entry, i) => ({ ...entry, metadata: metadatas[i] }));
    const entriesTest = await this.sortJoinEntries(entriesUnsorted, action.context);
    if (entriesTest.isFailed()) {
      return entriesTest;
    }
    const entriesSorted = entriesTest.get();
    return passTestWithSideData({
      iterations: 0,
      persistedItems: 0,
      blockingItems: 0,
      requestTime: 0,
    }, { ...sideData, entriesUnsorted, entriesSorted });
  }
}

export interface IActorRdfJoinInnerIncrementalMemoryMultiBindArgs extends IActorRdfJoinArgs {
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
  /**
   * The merge bindings context mediator
   */
  mediatorMergeBindingsContext: MediatorMergeBindingsContext;
  /**
   * The hash bindings mediator
   */
  mediatorHashBindings: MediatorHashBindings;
}

export interface IActorRdfJoinIncrementalMemoryMultiBindTestSideData extends IActorRdfJoinTestSideData {
  entriesUnsorted: IJoinEntryWithMetadata[];
  entriesSorted: IJoinEntryWithMetadata[];
}
