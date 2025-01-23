import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import type { MediatorMergeBindingsContext } from '@comunica/bus-merge-bindings-context';
import type { IActionQueryOperation } from '@comunica/bus-query-operation';
import type { IActionRdfJoin } from '@comunica/bus-rdf-join';
import type { IActionRdfJoinEntriesSort, MediatorRdfJoinEntriesSort } from '@comunica/bus-rdf-join-entries-sort';
import type { IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput } from '@comunica/bus-rdf-join-selectivity';
import 'jest-rdf';
import { KeysInitQuery, KeysQueryOperation } from '@comunica/context-entries';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { failTest, ActionContext, Bus } from '@comunica/core';
import type { IActionContext, IQueryOperationResultBindings } from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { MetadataValidationState } from '@comunica/utils-metadata';
import { KeysBindings } from '@incremunica/context-entries';
import {
  createTestBindingsFactory,
  createTestContextWithDataFactory,
  createTestMediatorMergeBindingsContext,
  createTestMediatorHashBindings,
} from '@incremunica/dev-tools';
import { arrayifyStream } from 'arrayify-stream';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { Factory } from 'sparqlalgebrajs';
import { ActorRdfJoinInnerMemoryBind } from '../lib';
import '@comunica/utils-jest';
import '@incremunica/jest';

const DF = new DataFactory();
const FACTORY = new Factory(DF);

describe('ActorRdfJoinMemoryBind', () => {
  let bus: any;
  let BF: BindingsFactory;

  beforeEach(async() => {
    bus = new Bus({ name: 'bus' });
    BF = await createTestBindingsFactory(DF);
  });

  describe('An ActorRdfJoinMemoryBind instance', () => {
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
    let mediatorMergeBindingsContext: MediatorMergeBindingsContext;
    let mediatorHashBindings: MediatorHashBindings;
    let actor: ActorRdfJoinInnerMemoryBind;

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
      context = createTestContextWithDataFactory(DF);
      mediatorQueryOperation = <any> {
        mediate: jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
          return {
            bindingsStream: new ArrayIterator([
              BF.bindings([
                [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
              ]).setContextEntry(KeysBindings.isAddition, true),
              BF.bindings([
                [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
              ]).setContextEntry(KeysBindings.isAddition, true),
              BF.bindings([
                [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
              ]).setContextEntry(KeysBindings.isAddition, true),
            ], { autoStart: false }),
            metadata: () => Promise.resolve({
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              variables: [{
                variable: DF.variable('bound'),
                canBeUndef: false,
              }],
            }),
            type: 'bindings',
          };
        }),
      };
      mediatorMergeBindingsContext = createTestMediatorMergeBindingsContext();
      mediatorHashBindings = createTestMediatorHashBindings();
      actor = new ActorRdfJoinInnerMemoryBind({
        name: 'actor',
        bus,
        selectivityModifier: 0.1,
        mediatorQueryOperation,
        mediatorJoinSelectivity,
        mediatorJoinEntriesSort,
        mediatorMergeBindingsContext,
        mediatorHashBindings,
      });
    });

    describe('getJoinCoefficients', () => {
      it('should fail if sort fails', async() => {
        jest.spyOn(actor, 'sortJoinEntries').mockResolvedValue(failTest('test message'));
        const joinCoefficients = await actor.getJoinCoefficients({
          type: 'inner',
          entries: [
            {
              output: <any>{},
              operation: <any>{},
            },
          ],
          context: new ActionContext(),
        }, {
          metadatas: [
            {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              pageSize: 100,
              requestTime: 10,
              variables: [{
                canBeUndef: false,
                variable: DF.variable('a'),
              }],
            },
          ],
        });
        expect(actor.sortJoinEntries).toHaveBeenCalledTimes(1);
        expect(joinCoefficients).toFailTest('test message');
      });

      it('should handle three entries', async() => {
        const joinCoeficients = await actor.getJoinCoefficients(
          {
            type: 'inner',
            entries: [
              {
                output: <any>{},
                operation: FACTORY.createNop(),
              },
              {
                output: <any>{},
                operation: FACTORY.createNop(),
              },
              {
                output: <any>{},
                operation: FACTORY.createNop(),
              },
            ],
            context: new ActionContext(),
          },
          {
            metadatas: [
              {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                pageSize: 100,
                requestTime: 10,
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
              {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                pageSize: 100,
                requestTime: 20,
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
              {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 500 },
                pageSize: 100,
                requestTime: 30,
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            ],
          },
        );
        expect(joinCoeficients.isPassed()).toBeTruthy();
        expect(joinCoeficients.get()).toEqual({
          iterations: 80.48000000000002,
          persistedItems: 0,
          blockingItems: 0,
          requestTime: 32.592000000000006,
        });
        expect(joinCoeficients.getSideData().entriesUnsorted.map(entry => entry.metadata.cardinality.value))
          .toEqual([ 3, 2, 500 ]);
        expect(joinCoeficients.getSideData().entriesSorted.map(entry => entry.metadata.cardinality.value))
          .toEqual([ 2, 3, 500 ]);
      });

      it('should handle three entries with a lower variable overlap', async() => {
        const joinCoeficients = await actor.getJoinCoefficients(
          {
            type: 'inner',
            entries: [
              {
                output: <any>{},
                operation: FACTORY.createNop(),
              },
              {
                output: <any>{},
                operation: FACTORY.createNop(),
              },
              {
                output: <any>{},
                operation: FACTORY.createNop(),
              },
            ],
            context: new ActionContext(),
          },
          {
            metadatas: [
              {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                pageSize: 100,
                requestTime: 10,
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }, {
                  canBeUndef: false,
                  variable: DF.variable('b'),
                }],
              },
              {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                pageSize: 100,
                requestTime: 20,
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }, {
                  canBeUndef: false,
                  variable: DF.variable('b'),
                }],
              },
              {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 500 },
                pageSize: 100,
                requestTime: 30,
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }, {
                  canBeUndef: false,
                  variable: DF.variable('b'),
                }],
              },
            ],
          },
        );
        expect(joinCoeficients.isPassed()).toBeTruthy();
        expect(joinCoeficients.get()).toEqual({
          iterations: 80.48000000000002,
          persistedItems: 0,
          blockingItems: 0,
          requestTime: 32.592000000000006,
        });
        expect(joinCoeficients.getSideData().entriesUnsorted.map(entry => entry.metadata.cardinality.value))
          .toEqual([ 3, 2, 500 ]);
        expect(joinCoeficients.getSideData().entriesSorted.map(entry => entry.metadata.cardinality.value))
          .toEqual([ 2, 3, 500 ]);
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
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
          ],
          context,
        )).resolves.toEqual({
          value: [
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
          ],
        });
      });

      it('sorts 3 entries', async() => {
        await expect(actor.sortJoinEntries([
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 3 },
              variables: [{
                canBeUndef: false,
                variable: DF.variable('a'),
              }],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 2 },
              variables: [{
                canBeUndef: false,
                variable: DF.variable('a'),
              }],
            },
          },
          {
            output: <any> {},
            operation: <any> {},
            metadata: {
              state: new MetadataValidationState(),
              cardinality: { type: 'estimate', value: 5 },
              variables: [{
                canBeUndef: false,
                variable: DF.variable('a'),
              }],
            },
          },
        ], context)).resolves.toEqual({
          value: [
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 5 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
          ],
        });
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
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
          ],
          context,
        )).resolves.toEqual({
          value: [
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
          ],
        });
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
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 5 },
                variables: [{
                  canBeUndef: true,
                  variable: DF.variable('a'),
                }],
              },
            },
          ],
          context,
        )).resolves.toEqual({
          value: [
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 5 },
                variables: [{
                  canBeUndef: true,
                  variable: DF.variable('a'),
                }],
              },
            },
          ],
        });
      });

      it('throws if there are no overlapping variables', async() => {
        await expect(actor.sortJoinEntries(
          [
            {
              output: <any>{},
              operation: <any>{},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a1'),
                }, {
                  canBeUndef: false,
                  variable: DF.variable('b1'),
                }],
              },
            },
            {
              output: <any>{},
              operation: <any>{},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a2'),
                }, {
                  canBeUndef: false,
                  variable: DF.variable('b2'),
                }],
              },
            },
          ],
          context,
        )).resolves.toFailTest('Bind join can only join entries with at least one common variable');
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
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('b'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
          ],
          context,
        )).resolves.toEqual({
          value: [
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 1 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('b'),
                }],
              },
            },
          ],
        });
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
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 1 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('b'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 20 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 20 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('c'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 10 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('d'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 10 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
          ],
          context,
        )).resolves.toEqual({
          value: [
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 10 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 20 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 1 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('b'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 10 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('d'),
                }],
              },
            },
            {
              output: <any> {},
              operation: <any> {},
              metadata: {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 20 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('c'),
                }],
              },
            },
          ],
        });
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 500 },
                  variables: [
                    {
                      variable: DF.variable('a'),
                      canBeUndef: false,
                    },
                    {
                      variable: DF.variable('b'),
                      canBeUndef: false,
                    },
                  ],
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 1 },
                  variables: [{
                    variable: DF.variable('a'),
                    canBeUndef: false,
                  }],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action, <any>(await actor.test(action)).getSideData());

        // Validate output
        expect(result.type).toBe('bindings');
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
        ]);
        await expect(result.metadata()).resolves.toEqual({
          state: new MetadataValidationState(),
          cardinality: { type: 'estimate', value: 400 },
          variables: [{
            canBeUndef: false,
            variable: DF.variable('a'),
          }, {
            canBeUndef: false,
            variable: DF.variable('b'),
          }],
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 3 },
                  variables: [
                    {
                      variable: DF.variable('a'),
                      canBeUndef: false,
                    },
                    {
                      variable: DF.variable('b'),
                      canBeUndef: false,
                    },
                  ],
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('c'), DF.namedNode('ex:c2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('c'), DF.namedNode('ex:c3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 500 },
                  variables: [
                    {
                      variable: DF.variable('a'),
                      canBeUndef: false,
                    },
                    {
                      variable: DF.variable('c'),
                      canBeUndef: false,
                    },
                  ],
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 1 },
                  variables: [{
                    variable: DF.variable('a'),
                    canBeUndef: false,
                  }],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
        };
        const { result } = await actor.getOutput(action, <any>(await actor.test(action)).getSideData());

        // Validate output
        expect(result.type).toBe('bindings');
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
        ]);
        await expect(result.metadata()).resolves.toEqual({
          state: new MetadataValidationState(),
          cardinality: { type: 'estimate', value: 1200 },
          variables: [
            {
              variable: DF.variable('a'),
              canBeUndef: false,
            },
            {
              variable: DF.variable('b'),
              canBeUndef: false,
            },
            {
              variable: DF.variable('c'),
              canBeUndef: false,
            },
          ],
        });

        // Validate mock calls
        expect(mediatorQueryOperation.mediate).toHaveBeenCalledTimes(2);
        expect(mediatorQueryOperation.mediate).toHaveBeenNthCalledWith(1, {
          operation: FACTORY.createJoin([
            FACTORY.createPattern(DF.namedNode('ex:a1'), DF.namedNode('ex:p1'), DF.variable('b')),
            FACTORY.createPattern(DF.namedNode('ex:a1'), DF.namedNode('ex:p2'), DF.variable('c')),
          ]),
          context: new ActionContext({
            [KeysInitQuery.dataFactory.name]: DF,
            [KeysQueryOperation.joinLeftMetadata.name]: {
              state: expect.any(MetadataValidationState),
              cardinality: { type: 'estimate', value: 1 },
              variables: [{
                canBeUndef: false,
                variable: DF.variable('a'),
              }],
            },
            [KeysQueryOperation.joinRightMetadatas.name]: [
              {
                state: expect.any(MetadataValidationState),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }, {
                  canBeUndef: false,
                  variable: DF.variable('b'),
                }],
              },
              {
                state: expect.any(MetadataValidationState),
                cardinality: { type: 'estimate', value: 500 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }, {
                  canBeUndef: false,
                  variable: DF.variable('c'),
                }],
              },
            ],
            [KeysQueryOperation.joinBindings.name]: BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          }),
        });
        expect(mediatorQueryOperation.mediate).toHaveBeenNthCalledWith(2, {
          operation: FACTORY.createJoin([
            FACTORY.createPattern(DF.namedNode('ex:a2'), DF.namedNode('ex:p1'), DF.variable('b')),
            FACTORY.createPattern(DF.namedNode('ex:a2'), DF.namedNode('ex:p2'), DF.variable('c')),
          ]),
          context: new ActionContext({
            [KeysInitQuery.dataFactory.name]: DF,
            [KeysQueryOperation.joinLeftMetadata.name]: {
              state: expect.any(MetadataValidationState),
              cardinality: { type: 'estimate', value: 1 },
              variables: [{
                canBeUndef: false,
                variable: DF.variable('a'),
              }],
            },
            [KeysQueryOperation.joinRightMetadatas.name]: [
              {
                state: expect.any(MetadataValidationState),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }, {
                  canBeUndef: false,
                  variable: DF.variable('b'),
                }],
              },
              {
                state: expect.any(MetadataValidationState),
                cardinality: { type: 'estimate', value: 500 },
                variables: [{
                  canBeUndef: false,
                  variable: DF.variable('a'),
                }, {
                  canBeUndef: false,
                  variable: DF.variable('c'),
                }],
              },
            ],
            [KeysQueryOperation.joinBindings.name]: BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 500 },
                  variables: [
                    {
                      variable: DF.variable('a'),
                      canBeUndef: false,
                    },
                    {
                      variable: DF.variable('b'),
                      canBeUndef: false,
                    },
                  ],
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 1 },
                  variables: [
                    {
                      variable: DF.variable('a'),
                      canBeUndef: false,
                    },
                    {
                      variable: DF.variable('bound'),
                      canBeUndef: false,
                    },
                  ],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.variable('bound')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action, <any>(await actor.test(action)).getSideData());

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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 500 },
                  variables: [
                    {
                      variable: DF.variable('a'),
                      canBeUndef: false,
                    },
                    {
                      variable: DF.variable('b'),
                      canBeUndef: false,
                    },
                  ],
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(KeysBindings.isAddition, false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 1 },
                  variables: [{
                    variable: DF.variable('a'),
                    canBeUndef: false,
                  }],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action, <any>(await actor.test(action)).getSideData());

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 500 },
                  variables: [
                    {
                      variable: DF.variable('bound'),
                      canBeUndef: false,
                    },
                    {
                      variable: DF.variable('a'),
                      canBeUndef: false,
                    },
                  ],
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(KeysBindings.isAddition, false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 1 },
                  variables: [{
                    variable: DF.variable('a'),
                    canBeUndef: false,
                  }],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action, <any>(await actor.test(action)).getSideData());

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a3') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a3') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a3') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
        ]);
      });

      it('should handle two entries with ple identical bindings and removal', async() => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 500 },
                  variables: [
                    {
                      variable: DF.variable('bound'),
                      canBeUndef: false,
                    },
                    {
                      variable: DF.variable('a'),
                      canBeUndef: false,
                    },
                  ],
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(KeysBindings.isAddition, false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 1 },
                  variables: [{
                    variable: DF.variable('a'),
                    canBeUndef: false,
                  }],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action, <any>(await actor.test(action)).getSideData());

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
        ]);
      });

      it('should handle two entries with ple identical bindings and removal (other)', async() => {
        mediatorQueryOperation = <any> {
          mediate: jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
            return {
              bindingsStream: new ArrayIterator([
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(KeysBindings.isAddition, true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(KeysBindings.isAddition, true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
                ]).setContextEntry(KeysBindings.isAddition, true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(KeysBindings.isAddition, false),
              ], { autoStart: false }),
              metadata: () => Promise.resolve({
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 4 },
                variables: [{
                  variable: DF.variable('bound'),
                  canBeUndef: false,
                }],
              }),
              type: 'bindings',
            };
          }),
        };

        actor = new ActorRdfJoinInnerMemoryBind({
          name: 'actor',
          bus,
          selectivityModifier: 0.1,
          mediatorQueryOperation,
          mediatorJoinSelectivity,
          mediatorJoinEntriesSort,
          mediatorMergeBindingsContext,
          mediatorHashBindings,
        });

        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 500 },
                  variables: [
                    {
                      variable: DF.variable('b'),
                      canBeUndef: false,
                    },
                    {
                      variable: DF.variable('a'),
                      canBeUndef: false,
                    },
                  ],
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 1 },
                  variables: [{
                    variable: DF.variable('a'),
                    canBeUndef: false,
                  }],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action, <any>(await actor.test(action)).getSideData());

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
        ]);
      });

      it('should handle two entries with deletions (other)', async() => {
        mediatorQueryOperation = <any> {
          mediate: jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
            return {
              bindingsStream: new ArrayIterator([
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(KeysBindings.isAddition, true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
                ]).setContextEntry(KeysBindings.isAddition, true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(KeysBindings.isAddition, false),
              ], { autoStart: false }),
              metadata: () => Promise.resolve({
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  variable: DF.variable('bound'),
                  canBeUndef: false,
                }],
              }),
              type: 'bindings',
            };
          }),
        };

        actor = new ActorRdfJoinInnerMemoryBind({
          name: 'actor',
          bus,
          selectivityModifier: 0.1,
          mediatorQueryOperation,
          mediatorJoinSelectivity,
          mediatorJoinEntriesSort,
          mediatorMergeBindingsContext,
          mediatorHashBindings,
        });

        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 500 },
                  variables: [
                    {
                      variable: DF.variable('a'),
                      canBeUndef: false,
                    },
                    {
                      variable: DF.variable('b'),
                      canBeUndef: false,
                    },
                  ],
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 1 },
                  variables: [{
                    variable: DF.variable('a'),
                    canBeUndef: false,
                  }],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action, <any>(await actor.test(action)).getSideData());

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
        ]);
      });

      it('should handle two entries with immediate deletions (other)', async() => {
        mediatorQueryOperation = <any> {
          mediate: jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
            return {
              bindingsStream: new ArrayIterator([
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(KeysBindings.isAddition, true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(KeysBindings.isAddition, false),
              ], { autoStart: false }),
              metadata: () => Promise.resolve({
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                variables: [{
                  variable: DF.variable('bound'),
                  canBeUndef: false,
                }],
              }),
              type: 'bindings',
            };
          }),
        };

        actor = new ActorRdfJoinInnerMemoryBind({
          name: 'actor',
          bus,
          selectivityModifier: 0.1,
          mediatorQueryOperation,
          mediatorJoinSelectivity,
          mediatorJoinEntriesSort,
          mediatorMergeBindingsContext,
          mediatorHashBindings,
        });

        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 500 },
                  variables: [
                    {
                      variable: DF.variable('a'),
                      canBeUndef: false,
                    },
                    {
                      variable: DF.variable('b'),
                      canBeUndef: false,
                    },
                  ],
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
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 1 },
                  variables: [{
                    variable: DF.variable('a'),
                    canBeUndef: false,
                  }],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action, <any>(await actor.test(action)).getSideData());

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
        ]);
      });
    });
  });
});
