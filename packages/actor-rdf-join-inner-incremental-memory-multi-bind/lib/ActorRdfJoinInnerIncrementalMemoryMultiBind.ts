import type { Bindings } from '@comunica/bindings-factory';
import { BindingsFactory } from '@comunica/bindings-factory';
import type { MediatorQueryOperation } from '@comunica/bus-query-operation';
import { ActorQueryOperation, materializeOperation } from '@comunica/bus-query-operation';
import type {
  IActionRdfJoin,
  IActorRdfJoinOutputInner,
  IActorRdfJoinArgs,
} from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { MediatorRdfJoinEntriesSort } from '@comunica/bus-rdf-join-entries-sort';
import { KeysQueryOperation } from '@comunica/context-entries';
import type { IMediatorTypeJoinCoefficients } from '@comunica/mediatortype-join-coefficients';
import type { BindingsStream, IQueryOperationResultBindings,
  MetadataBindings, IActionContext, IJoinEntryWithMetadata } from '@comunica/types';
import type { AsyncIterator } from 'asynciterator';
import {
  ArrayIterator,
  EmptyIterator,
  UnionIterator,
} from 'asynciterator';
import { Factory, Algebra } from 'sparqlalgebrajs';

/**
 * A comunica Multi-way Bind RDF Join Actor.
 */
export class ActorRdfJoinInnerIncrementalMemoryMultiBind extends ActorRdfJoin {
  public readonly selectivityModifier: number;
  public readonly mediatorJoinEntriesSort: MediatorRdfJoinEntriesSort;
  public readonly mediatorQueryOperation: MediatorQueryOperation;

  public static readonly FACTORY = new Factory();

  public static bindingHash(bindings: Bindings): string {
    let hash = '';
    for (const binding of bindings) {
      hash += `${binding[0].value}:${binding[1].value}#`;
    }
    return hash;
  }

  public constructor(args: IActorRdfJoinMultiBindArgs) {
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
   * @return {BindingsStream}
   */
  public static async createBindStream(
    baseStream: BindingsStream,
    operations: Algebra.Operation[],
    operationBinder: (boundOperations: Algebra.Operation[], operationBindings: Bindings)
    => Promise<BindingsStream>,
    optional: boolean,
  ): Promise<BindingsStream> {
    const transformMap = new Map<
    string,
    {
      iterator: AsyncIterator<Bindings | undefined>;
      memory: Map<
      string,
      {
        bindings: Bindings;
        count: number;
      }>;
      count: number;
    }>();

    // Create bindings function
    const binder = (bindings: Bindings, done: () => void, push: (i: BindingsStream) => void): void => {
      const hash = ActorRdfJoinInnerIncrementalMemoryMultiBind.bindingHash(bindings);
      if (bindings.diff) {
        const hashData = transformMap.get(hash);
        if (hashData === undefined) {
          const data = {
            iterator: new EmptyIterator<Bindings>(),
            memory: new Map<string, { bindings: Bindings; count: number }>(),
            count: 1,
          };
          transformMap.set(hash, data);

          // We don't bind the filter because filters are always handled last,
          // and we need to avoid binding filters of sub-queries, which are to be handled first. (see spec test bind10)
          const subOperations = operations
            .map(operation => materializeOperation(operation, bindings, { bindFilter: false }));

          const transformFunc = (subBindings: Bindings, subDone: () => void, subPush: (i: Bindings) => void): void => {
            const newBindings = subBindings.merge(bindings);
            if (newBindings === undefined) {
              subDone();
              return;
            }
            const bindingHash = ActorRdfJoinInnerIncrementalMemoryMultiBind.bindingHash(newBindings);
            const bindingsData = data.memory.get(bindingHash);
            if (newBindings.diff) {
              if (bindingsData === undefined) {
                data.memory.set(bindingHash, { bindings: newBindings, count: 1 });
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
                data.memory.delete(bindingHash);
              }
              for (let i = 0; i < data.count; i++) {
                subPush(newBindings);
              }
            }
            subDone();
          };

          // eslint-disable-next-line @typescript-eslint/no-floating-promises
          operationBinder(subOperations, bindings).then(bindingStream => {
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

            push(<BindingsStream>transformIterator);
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
            hashData.iterator.destroy();
            hashData.count = 0;
            transformMap.delete(hash);
          } else {
            hashData.count--;
          }
          const bindingsFactory = new BindingsFactory();
          if (hashData.memory.size > 0) {
            push(new ArrayIterator(hashData.memory.values()).transform({
              transform(item, arrayDone, arrayPush) {
                const transformBindings = bindingsFactory.fromBindings(item.bindings);
                transformBindings.diff = false;
                for (let i = 0; i < item.count; i++) {
                  arrayPush(transformBindings);
                }
                arrayDone();
              },
            }));
          }
          done();
        }
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
    if (multiOccurrenceVariables.length === 0) {
      throw new Error(`Bind join can only join entries with at least one common variable`);
    }

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

    // Bind the remaining patterns for each binding in the stream
    const subContext = action.context
      .set(KeysQueryOperation.joinLeftMetadata, entries[0].metadata)
      .set(KeysQueryOperation.joinRightMetadatas, remainingEntries.map(entry => entry.metadata));
    const bindingsStream: BindingsStream = await ActorRdfJoinInnerIncrementalMemoryMultiBind.createBindStream(
      smallestStream.bindingsStream,
      remainingEntries.map(entry => entry.operation),
      async(operations: Algebra.Operation[], operationBindings: Bindings) => {
        // Send the materialized patterns to the mediator for recursive join evaluation.
        const operation = operations.length === 1 ?
          operations[0] :
          ActorRdfJoinInnerIncrementalMemoryMultiBind.FACTORY.createJoin(operations);
        const output = ActorQueryOperation.getSafeBindings(await this.mediatorQueryOperation.mediate(
          { operation, context: subContext?.set(KeysQueryOperation.joinBindings, operationBindings) },
        ));
        return output.bindingsStream;
      },
      false,
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
    // Order the entries so we can pick the first one (usually the one with the lowest cardinality)
    const entries = await this.sortJoinEntries(action.entries
      .map((entry, i) => ({ ...entry, metadata: metadatas[i] })), action.context);
    metadatas = entries.map(entry => entry.metadata);

    const requestInitialTimes = ActorRdfJoin.getRequestInitialTimes(metadatas);
    const requestItemTimes = ActorRdfJoin.getRequestItemTimes(metadatas);

    // Determine first stream and remaining ones
    const remainingEntries = [ ...entries ];
    const remainingRequestInitialTimes = [ ...requestInitialTimes ];
    const remainingRequestItemTimes = [ ...requestItemTimes ];
    remainingEntries.splice(0, 1);
    remainingRequestInitialTimes.splice(0, 1);
    remainingRequestItemTimes.splice(0, 1);

    // Reject binding on some operation types
    if (remainingEntries
      .some(entry => entry.operation.type === Algebra.types.EXTEND || entry.operation.type === Algebra.types.GROUP)) {
      throw new Error(`Actor ${this.name} can not bind on Extend and Group operations`);
    }

    // Determine selectivities of smallest entry with all other entries
    const selectivities = await Promise.all(remainingEntries
      .map(async entry => (await this.mediatorJoinSelectivity.mediate({
        entries: [ entries[0], entry ],
        context: action.context,
      })).selectivity * this.selectivityModifier));

    // Determine coefficients for remaining entries
    const cardinalityRemaining = remainingEntries
      .map((entry, i) => entry.metadata.cardinality.value * selectivities[i])
      .reduce((sum, element) => sum + element, 0);
    const receiveInitialCostRemaining = remainingRequestInitialTimes
      .reduce((sum, element, i) => sum + (element * selectivities[i]), 0);
    const receiveItemCostRemaining = remainingRequestItemTimes
      .reduce((sum, element, i) => sum + (element * selectivities[i]), 0);

    return {
      iterations: metadatas[0].cardinality.value * cardinalityRemaining,
      persistedItems: 0,
      blockingItems: 0,
      requestTime: requestInitialTimes[0] +
        metadatas[0].cardinality.value * (
          requestItemTimes[0] +
          receiveInitialCostRemaining +
          cardinalityRemaining * receiveItemCostRemaining
        ),
    };
  }
}

export interface IActorRdfJoinMultiBindArgs extends IActorRdfJoinArgs {
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
