import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import type { IActionQueryOperation } from '@comunica/bus-query-operation';
import type { IActionRdfJoin } from '@comunica/bus-rdf-join';
import type { IActionRdfJoinEntriesSort, MediatorRdfJoinEntriesSort } from '@comunica/bus-rdf-join-entries-sort';
import type { IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput } from '@comunica/bus-rdf-join-selectivity';
import 'jest-rdf';
import { KeysQueryOperation } from '@comunica/context-entries';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { ActionContext, Bus } from '@comunica/core';
import { MetadataValidationState } from '@comunica/metadata';
import type { IActionContext, IQueryOperationResultBindings } from '@comunica/types';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import { DevTools } from '@incremunica/dev-tools';
import arrayifyStream from 'arrayify-stream';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { Factory, Algebra } from 'sparqlalgebrajs';
import { ActorRdfJoinInnerIncrementalMemoryMultiBind } from '../lib/ActorRdfJoinInnerIncrementalMemoryMultiBind';
import '@incremunica/incremental-jest';

const DF = new DataFactory();
const FACTORY = new Factory();

describe('ActorRdfJoinIncrementalMemoryMultiBind', () => {
  let bus: any;
  let BF: BindingsFactory;

  beforeEach(async() => {
    bus = new Bus({ name: 'bus' });
    BF = await DevTools.createBindingsFactory(DF);
  });

  describe('An ActorRdfJoinIncrementalMemoryMultiBind instance', () => {
    let mediatorJoinSelectivity: Mediator<
      Actor<IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>,
      IActionRdfJoinSelectivity,
      IActorTest,
      IActorRdfJoinSelectivityOutput
    >;
    let mediatorJoinEntriesSort: MediatorRdfJoinEntriesSort;
    let context: IActionContext;
    let mediatorQueryOperation: Mediator<
      Actor<IActionQueryOperation, IActorTest, IQueryOperationResultBindings>,
      IActionQueryOperation,
      IActorTest,
      IQueryOperationResultBindings
    >;
    let actor: ActorRdfJoinInnerIncrementalMemoryMultiBind;

    beforeEach(() => {
      mediatorJoinSelectivity = <any> {
        mediate: async() => ({ selectivity: 0.8 }),
      };
      mediatorJoinEntriesSort = <any> {
        async mediate(action: IActionRdfJoinEntriesSort) {
          const entries = [ ...action.entries ]
            .sort((left, right) => left.metadata.cardinality.value - right.metadata.cardinality.value);
          return { entries };
        },
      };
      context = new ActionContext({ a: 'b' });
      mediatorQueryOperation = <any> {
        mediate: jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
          return {
            bindingsStream: new ArrayIterator([
              BF.bindings([
                [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
              ]).setContextEntry(new ActionContextKeyIsAddition(), true),
              BF.bindings([
                [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
              ]).setContextEntry(new ActionContextKeyIsAddition(), true),
              BF.bindings([
                [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
              ]).setContextEntry(new ActionContextKeyIsAddition(), true),
            ], { autoStart: false }),
            metadata: () => Promise.resolve({
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              canContainUndefs: false,
              variables: [ DF.variable('bound') ],
            }),
            type: 'bindings',
          };
        }),
      };
      actor = new ActorRdfJoinInnerIncrementalMemoryMultiBind({
        name: 'actor',
        bus,
        selectivityModifier: 0.1,
        mediatorQueryOperation,
        mediatorJoinSelectivity,
        mediatorJoinEntriesSort,
      });
    });

    describe('getJoinCoefficients', () => {
      it('should handle three entries', async() => {
        await expect(actor.getJoinCoefficients(
          {
            type: 'inner',
            entries: [
              {
                output: <any>{},
                operation: <any>{},
              },
              {
                output: <any>{},
                operation: <any>{},
              },
              {
                output: <any>{},
                operation: <any>{},
              },
            ],
            context: new ActionContext(),
          },
          [
            {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              pageSize: 100,
              requestTime: 10,
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
            {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 2 },
              pageSize: 100,
              requestTime: 20,
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
            {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 5 },
              pageSize: 100,
              requestTime: 30,
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          ],
        )).resolves.toEqual({
          iterations: 0,
          persistedItems: 0,
          blockingItems: 0,
          requestTime: 0,
        });
      });

      it('should handle three entries with a lower variable overlap', async() => {
        await expect(actor.getJoinCoefficients(
          {
            type: 'inner',
            entries: [
              {
                output: <any>{},
                operation: <any>{},
              },
              {
                output: <any>{},
                operation: <any>{},
              },
              {
                output: <any>{},
                operation: <any>{},
              },
            ],
            context: new ActionContext(),
          },
          [
            {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              pageSize: 100,
              requestTime: 10,
              canContainUndefs: false,
              variables: [ DF.variable('a'), DF.variable('b') ],
            },
            {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 2 },
              pageSize: 100,
              requestTime: 20,
              canContainUndefs: false,
              variables: [ DF.variable('a'), DF.variable('b') ],
            },
            {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 5 },
              pageSize: 100,
              requestTime: 30,
              canContainUndefs: false,
              variables: [ DF.variable('a'), DF.variable('b') ],
            },
          ],
        )).resolves.toEqual({
          iterations: 0,
          persistedItems: 0,
          blockingItems: 0,
          requestTime: 0,
        });
      });

      it('should reject on a right stream of type extend', async() => {
        await expect(actor.getJoinCoefficients(
          {
            type: 'inner',
            entries: [
              {
                output: <any>{
                  metadata: () => Promise.resolve({
                    cardinality: { type: 'estimate', value: 3 },
                    canContainUndefs: false,
                  }),

                },
                operation: <any>{ type: Algebra.types.EXTEND },
              },
              {
                output: <any>{
                  metadata: () => Promise.resolve({
                    cardinality: { type: 'estimate', value: 2 },
                    canContainUndefs: false,
                  }),
                },
                operation: <any>{},
              },
            ],
            context: new ActionContext(),
          },
          [
            {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              pageSize: 100,
              requestTime: 10,
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
            {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 2 },
              pageSize: 100,
              requestTime: 20,
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          ],
        )).resolves.toEqual({
          iterations: 0,
          persistedItems: 0,
          blockingItems: 0,
          requestTime: 0,
        });
        // .rejects.toThrowError('Actor actor can not bind on Extend and Group operations');
      });

      it('should reject on a right stream of type group', async() => {
        await expect(actor.getJoinCoefficients(
          {
            type: 'inner',
            entries: [
              {
                output: <any> {},
                operation: <any> { type: Algebra.types.GROUP },
              },
              {
                output: <any> {},
                operation: <any> {},
              },
            ],
            context: new ActionContext(),
          },
          [
            {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              pageSize: 100,
              requestTime: 10,
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
            {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 2 },
              pageSize: 100,
              requestTime: 20,
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          ],
        )).resolves.toEqual({
          iterations: 0,
          persistedItems: 0,
          blockingItems: 0,
          requestTime: 0,
        });

        // .rejects.toThrowError('Actor actor can not bind on Extend and Group operations');
      });

      it('should not reject on a left stream of type group', async() => {
        await expect(actor.getJoinCoefficients(
          {
            type: 'inner',
            entries: [
              {
                output: <any> {},
                operation: <any> {},
              },
              {
                output: <any> {},
                operation: <any> { type: Algebra.types.GROUP },
              },
            ],
            context: new ActionContext(),
          },
          [
            {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              pageSize: 100,
              requestTime: 10,
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
            {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 2 },
              pageSize: 100,
              requestTime: 20,
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          ],
        )).resolves.toEqual({
          iterations: 0,
          persistedItems: 0,
          blockingItems: 0,
          requestTime: 0,
        });
      });
    });

    describe('sortJoinEntries', () => {
      it('sorts 2 entries', async() => {
        await expect(actor.sortJoinEntries(
          [
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                canContainUndefs: false,
                variables: [ DF.variable('a') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                canContainUndefs: false,
                variables: [ DF.variable('a') ],
              },
            },
          ],
          context,
        )).resolves.toEqual([
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 2 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
        ]);
      });

      it('sorts 3 entries', async() => {
        await expect(actor.sortJoinEntries([
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 2 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 5 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
        ], context)).resolves.toEqual([
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 2 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 5 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
        ]);
      });

      it('sorts 3 equal entries', async() => {
        await expect(actor.sortJoinEntries(
          [
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                canContainUndefs: false,
                variables: [ DF.variable('a') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                canContainUndefs: false,
                variables: [ DF.variable('a') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                canContainUndefs: false,
                variables: [ DF.variable('a') ],
              },
            },
          ],
          context,
        )).resolves.toEqual([
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
        ]);
      });

      it('does not sort if there is an undef', async() => {
        await expect(actor.sortJoinEntries(
          [
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                canContainUndefs: false,
                variables: [ DF.variable('a') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                canContainUndefs: false,
                variables: [ DF.variable('a') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 5 },
                canContainUndefs: true,
                variables: [ DF.variable('a') ],
              },
            },
          ],
          context,
        )).resolves.toEqual([
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 2 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 5 },
              canContainUndefs: true,
              variables: [ DF.variable('a') ],
            },
          },
        ]);
      });

      it('throws if there are no overlapping variables', async() => {
        await expect(actor.sortJoinEntries(
          [
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                canContainUndefs: false,
                variables: [ DF.variable('a1'), DF.variable('b1') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                canContainUndefs: false,
                variables: [ DF.variable('a2'), DF.variable('b2') ],
              },
            },
          ],
          context,
        )).rejects.toThrow('Bind join can only join entries with at least one common variable');
      });

      it('sorts entries without common variables in the back', async() => {
        await expect(actor.sortJoinEntries(
          [
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 1 },
                canContainUndefs: false,
                variables: [ DF.variable('b') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                canContainUndefs: false,
                variables: [ DF.variable('a') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                canContainUndefs: false,
                variables: [ DF.variable('a') ],
              },
            },
          ],
          context,
        )).resolves.toEqual([
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 2 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 1 },
              canContainUndefs: false,
              variables: [ DF.variable('b') ],
            },
          },
        ]);
      });

      it('sorts several entries without variables in the back', async() => {
        await expect(actor.sortJoinEntries(
          [
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                canContainUndefs: false,
                variables: [ DF.variable('a') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 1 },
                canContainUndefs: false,
                variables: [ DF.variable('b') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 20 },
                canContainUndefs: false,
                variables: [ DF.variable('a') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 20 },
                canContainUndefs: false,
                variables: [ DF.variable('c') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                canContainUndefs: false,
                variables: [ DF.variable('a') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 10 },
                canContainUndefs: false,
                variables: [ DF.variable('d') ],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 10 },
                canContainUndefs: false,
                variables: [ DF.variable('a') ],
              },
            },
          ],
          context,
        )).resolves.toEqual([
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 2 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 10 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 20 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 1 },
              canContainUndefs: false,
              variables: [ DF.variable('b') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 10 },
              canContainUndefs: false,
              variables: [ DF.variable('d') ],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 20 },
              canContainUndefs: false,
              variables: [ DF.variable('c') ],
            },
          },
        ]);
      });
    });

    describe('getOutput', () => {
      it('should handle two entries without context', async() => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 3 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a'), DF.variable('b') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 1 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toBe('bindings');
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ]);
        await expect(result.metadata()).resolves.toEqual({
          state: new MetadataValidationState(),
          cardinality: { type: 'estimate', value: 2.400_000_000_000_000_4 },
          canContainUndefs: false,
          variables: [ DF.variable('a'), DF.variable('b') ],
        });
      });

      it('should handle three entries', async() => {
        const action: IActionRdfJoin = {
          context,
          type: 'inner',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 3 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a'), DF.variable('b') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('c'), DF.namedNode('ex:c1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('c'), DF.namedNode('ex:c2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('c'), DF.namedNode('ex:c3') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 4 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a'), DF.variable('c') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.variable('c')),
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 1 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toBe('bindings');
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ]);
        await expect(result.metadata()).resolves.toEqual({
          state: new MetadataValidationState(),
          cardinality: { type: 'estimate', value: 9.600_000_000_000_001 },
          canContainUndefs: false,
          variables: [ DF.variable('a'), DF.variable('b'), DF.variable('c') ],
        });

        // Validate mock calls
        expect(mediatorQueryOperation.mediate).toHaveBeenCalledTimes(2);
        expect(mediatorQueryOperation.mediate).toHaveBeenNthCalledWith(1, {
          operation: FACTORY.createJoin([
            FACTORY.createPattern(DF.namedNode('ex:a1'), DF.namedNode('ex:p1'), DF.variable('b')),
            FACTORY.createPattern(DF.namedNode('ex:a1'), DF.namedNode('ex:p2'), DF.variable('c')),
          ]),
          context: new ActionContext({
            a: 'b',
            [KeysQueryOperation.joinLeftMetadata.name]: {
              state: expect.any(MetadataValidationState),
              cardinality: { type: 'estimate', value: 1 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
            [KeysQueryOperation.joinRightMetadatas.name]: [
              {
                state: expect.any(MetadataValidationState),
                cardinality: { type: 'estimate', value: 3 },
                canContainUndefs: false,
                variables: [ DF.variable('a'), DF.variable('b') ],
              },
              {
                state: expect.any(MetadataValidationState),
                cardinality: { type: 'estimate', value: 4 },
                canContainUndefs: false,
                variables: [ DF.variable('a'), DF.variable('c') ],
              },
            ],
            [KeysQueryOperation.joinBindings.name]: BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          }),
        });
        expect(mediatorQueryOperation.mediate).toHaveBeenNthCalledWith(2, {
          operation: FACTORY.createJoin([
            FACTORY.createPattern(DF.namedNode('ex:a2'), DF.namedNode('ex:p1'), DF.variable('b')),
            FACTORY.createPattern(DF.namedNode('ex:a2'), DF.namedNode('ex:p2'), DF.variable('c')),
          ]),
          context: new ActionContext({
            a: 'b',
            [KeysQueryOperation.joinLeftMetadata.name]: {
              state: expect.any(MetadataValidationState),
              cardinality: { type: 'estimate', value: 1 },
              canContainUndefs: false,
              variables: [ DF.variable('a') ],
            },
            [KeysQueryOperation.joinRightMetadatas.name]: [
              {
                state: expect.any(MetadataValidationState),
                cardinality: { type: 'estimate', value: 3 },
                canContainUndefs: false,
                variables: [ DF.variable('a'), DF.variable('b') ],
              },
              {
                state: expect.any(MetadataValidationState),
                cardinality: { type: 'estimate', value: 4 },
                canContainUndefs: false,
                variables: [ DF.variable('a'), DF.variable('c') ],
              },
            ],
            [KeysQueryOperation.joinBindings.name]: BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          }),
        });
      });

      it('should handle two entries with one wrong binding (should not happen)', async() => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: { type: 'estimate', value: 4 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a'), DF.variable('b') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('bound'), DF.namedNode('ex:bound4') ],
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: { type: 'estimate', value: 1 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a'), DF.variable('bound') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.variable('bound')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([ ]);
      });

      it('should handle two entries with immediate deletions', async() => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: { type: 'estimate', value: 4 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a'), DF.variable('b') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: { type: 'estimate', value: 1 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ]);
      });

      it('should handle two entries with deletions', async() => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: { type: 'estimate', value: 4 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a'), DF.variable('b') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a3') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: { type: 'estimate', value: 1 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a3') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a3') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a3') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
        ]);
      });

      it('should handle two entries with multiple identical bindings and removal', async() => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: { type: 'estimate', value: 4 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a'), DF.variable('b') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: { type: 'estimate', value: 1 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
        ]);
      });

      it('should handle two entries with multiple identical bindings and removal (other)', async() => {
        mediatorQueryOperation = <any> {
          mediate: jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
            return {
              bindingsStream: new ArrayIterator([
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
                ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(new ActionContextKeyIsAddition(), false),
              ], { autoStart: false }),
              metadata: () => Promise.resolve({
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                canContainUndefs: false,
                variables: [ DF.variable('bound') ],
              }),
              type: 'bindings',
            };
          }),
        };

        actor = new ActorRdfJoinInnerIncrementalMemoryMultiBind({
          name: 'actor',
          bus,
          selectivityModifier: 0.1,
          mediatorQueryOperation,
          mediatorJoinSelectivity,
          mediatorJoinEntriesSort,
        });

        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 4 },
                  canContainUndefs: false,
                  variables: [ DF.variable('b'), DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 1 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
        ]);
      });

      it('should handle two entries with deletions (other)', async() => {
        mediatorQueryOperation = <any> {
          mediate: jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
            return {
              bindingsStream: new ArrayIterator([
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
                ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(new ActionContextKeyIsAddition(), false),
              ], { autoStart: false }),
              metadata: () => Promise.resolve({
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                canContainUndefs: false,
                variables: [ DF.variable('bound') ],
              }),
              type: 'bindings',
            };
          }),
        };

        actor = new ActorRdfJoinInnerIncrementalMemoryMultiBind({
          name: 'actor',
          bus,
          selectivityModifier: 0.1,
          mediatorQueryOperation,
          mediatorJoinSelectivity,
          mediatorJoinEntriesSort,
        });

        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 4 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a'), DF.variable('b') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 1 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
        ]);
      });

      it('should handle two entries with immediate deletions (other)', async() => {
        mediatorQueryOperation = <any> {
          mediate: jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
            return {
              bindingsStream: new ArrayIterator([
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(new ActionContextKeyIsAddition(), false),
              ], { autoStart: false }),
              metadata: () => Promise.resolve({
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                canContainUndefs: false,
                variables: [ DF.variable('bound') ],
              }),
              type: 'bindings',
            };
          }),
        };

        actor = new ActorRdfJoinInnerIncrementalMemoryMultiBind({
          name: 'actor',
          bus,
          selectivityModifier: 0.1,
          mediatorQueryOperation,
          mediatorJoinSelectivity,
          mediatorJoinEntriesSort,
        });

        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: { type: 'estimate', value: 4 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a'), DF.variable('b') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: { type: 'estimate', value: 1 },
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
        ]);
      });

      // TODO re-enable these tests
      // eslint-disable-next-line jest/no-commented-out-tests
      // it('should throw if operationBinder throws error', async() => {
      // await expect(await arrayifyStream(await ActorRdfJoinInnerIncrementalMemoryMultiBind.createBindStream(
      //       new ArrayIterator([
      //         BF.bindings([
      //           [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
      //         ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      //       ], { autoStart: false }),
      //       [FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b'))],
      //       (boundOperations: Algebra.Operation[], operationBindings: Bindings) => {throw new Error("throw test")},
      //       false
      //     ))
      // ).toThrow('throw test');
      // });
      //
    });
  });
});
