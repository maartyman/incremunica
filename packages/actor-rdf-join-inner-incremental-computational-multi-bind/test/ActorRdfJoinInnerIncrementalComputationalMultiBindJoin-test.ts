import type { EventEmitter } from 'node:events';
import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import type { MediatorMergeBindingsContext } from '@comunica/bus-merge-bindings-context';
import type { IActionQueryOperation } from '@comunica/bus-query-operation';
import type { IActionRdfJoin } from '@comunica/bus-rdf-join';
import type { IActionRdfJoinEntriesSort, MediatorRdfJoinEntriesSort } from '@comunica/bus-rdf-join-entries-sort';
import type { IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput } from '@comunica/bus-rdf-join-selectivity';
import 'jest-rdf';
import { KeysQueryOperation } from '@comunica/context-entries';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { ActionContext, Bus } from '@comunica/core';
import type { BindingsStream, IActionContext, IQueryOperationResultBindings } from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { MetadataValidationState } from '@comunica/utils-metadata';
import { KeysBindings, KeysStreamingSource } from '@incremunica/context-entries';
import { DevTools } from '@incremunica/dev-tools';
import arrayifyStream from 'arrayify-stream';
import { ArrayIterator, WrappingIterator } from 'asynciterator';
import { promisifyEventEmitter } from 'event-emitter-promisify/dist';
import { DataFactory } from 'rdf-data-factory';
import type { Stream } from 'readable-stream';
import { PassThrough } from 'readable-stream';
import { Factory } from 'sparqlalgebrajs';
import {
  ActorRdfJoinInnerIncrementalComputationalMultiBind,
} from '../lib/ActorRdfJoinInnerIncrementalComputationalMultiBind';
import Mock = jest.Mock;
import '@comunica/utils-jest';
import '@incremunica/incremental-jest';

const streamifyArray = require('streamify-array');

const DF = new DataFactory();
const FACTORY = new Factory();

async function partialArrayifyStream(stream: EventEmitter, num: number): Promise<any[]> {
  const array: any[] = [];
  for (let i = 0; i < num; i++) {
    await new Promise<void>(resolve => stream.once('data', (bindings: any) => {
      array.push(bindings);
      resolve();
    }));
  }
  return array;
}

describe('ActorRdfJoinIncrementalComputationalMultiBind', () => {
  let bus: any;
  let BF: BindingsFactory;

  beforeEach(async() => {
    bus = new Bus({ name: 'bus' });
    BF = await DevTools.createTestBindingsFactory(DF);
  });

  describe('An ActorRdfJoinIncrementalComputationalMultiBind instance', () => {
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
    let actor: ActorRdfJoinInnerIncrementalComputationalMultiBind;

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
      context = DevTools.createTestContextWithDataFactory(DF, context);
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
      mediatorMergeBindingsContext = DevTools.createTestMediatorMergeBindingsContext();
      mediatorHashBindings = DevTools.createTestMediatorHashBindings();
      actor = new ActorRdfJoinInnerIncrementalComputationalMultiBind({
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
      it('should handle three entries', async() => {
        const joinCoeficients = await actor.getJoinCoefficients(
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
          {
            metadatas: [
              {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 3 },
                pageSize: 100,
                requestTime: 10,
                variables: [{
                  variable: DF.variable('a'),
                  canBeUndef: false,
                }],
              },
              {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 2 },
                pageSize: 100,
                requestTime: 20,
                variables: [{
                  variable: DF.variable('a'),
                  canBeUndef: false,
                }],
              },
              {
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 5 },
                pageSize: 100,
                requestTime: 30,
                variables: [{
                  variable: DF.variable('a'),
                  canBeUndef: false,
                }],
              },
            ],
          },
        );
        expect(joinCoeficients.isPassed()).toBeTruthy();
        expect(joinCoeficients.get()).toEqual({
          iterations: 0,
          persistedItems: 0,
          blockingItems: 0,
          requestTime: 0,
        });
        expect(joinCoeficients.getSideData().entriesUnsorted.map(entry => entry.metadata.cardinality.value))
          .toEqual([ 3, 2, 5 ]);
        expect(joinCoeficients.getSideData().entriesSorted.map(entry => entry.metadata.cardinality.value))
          .toEqual([ 2, 3, 5 ]);
      });

      // TODO [2024-12-01]: re-enable these tests
      // eslint-disable-next-line jest/no-commented-out-tests
      // it('should handle three entries with a lower variable overlap', async() => {
      // expect(await actor.getJoinCoefficients(
      //     {
      //       type: 'inner',
      //       entries: [
      //         {
      //           output: <any>{},
      //           operation: <any>{},
      //         },
      //         {
      //           output: <any>{},
      //           operation: <any>{},
      //         },
      //         {
      //           output: <any>{},
      //           operation: <any>{},
      //         },
      //       ],
      //       context: new ActionContext(),
      //     },
      //     [
      //       {
      //         state: new MetadataValidationState(),
      //         cardinality: { type: 'estimate', value: 3 },
      //         pageSize: 100,
      //         requestTime: 10,
      //         canContainUndefs: false,
      //         variables: [ DF.variable('a'), DF.variable('b') ],
      //       },
      //       {
      //         state: new MetadataValidationState(),
      //         cardinality: { type: 'estimate', value: 2 },
      //         pageSize: 100,
      //         requestTime: 20,
      //         canContainUndefs: false,
      //         variables: [ DF.variable('a'), DF.variable('b') ],
      //       },
      //       {
      //         state: new MetadataValidationState(),
      //         cardinality: { type: 'estimate', value: 5 },
      //         pageSize: 100,
      //         requestTime: 30,
      //         canContainUndefs: false,
      //         variables: [ DF.variable('a'), DF.variable('b') ],
      //       },
      //     ],
      // )).toEqual({
      //     iterations: 1.280_000_000_000_000_2,
      //     persistedItems: 0,
      //     blockingItems: 0,
      //     requestTime: 0.440_96,
      // });
      // });
      // eslint-disable-next-line jest/no-commented-out-tests
      // it('should reject on a right stream of type extend', async() => {
      // await expect(actor.getJoinCoefficients(
      //     {
      //       type: 'inner',
      //       entries: [
      //         {
      //           output: <any>{
      //             metadata: () => Promise.resolve({
      //               state: new MetadataValidationState(),
      //               cardinality: { type: 'estimate', value: 3 },
      //               canContainUndefs: false,
      //             }),
      //
      //           },
      //           operation: <any>{ type: Algebra.types.EXTEND },
      //         },
      //         {
      //           output: <any>{
      //             metadata: () => Promise.resolve({
      //               state: new MetadataValidationState(),
      //               cardinality: { type: 'estimate', value: 2 },
      //               canContainUndefs: false,
      //             }),
      //           },
      //           operation: <any>{},
      //         },
      //       ],
      //       context: new ActionContext(),
      //     },
      //     [
      //       {
      //         state: new MetadataValidationState(),
      //         cardinality: { type: 'estimate', value: 3 },
      //         pageSize: 100,
      //         requestTime: 10,
      //         canContainUndefs: false,
      //         variables: [ DF.variable('a') ],
      //       },
      //       {
      //         state: new MetadataValidationState(),
      //         cardinality: { type: 'estimate', value: 2 },
      //         pageSize: 100,
      //         requestTime: 20,
      //         canContainUndefs: false,
      //         variables: [ DF.variable('a') ],
      //       },
      //     ],
      // )).rejects.toThrowError('Actor actor can not bind on Extend and Group operations');
      // });
      // eslint-disable-next-line jest/no-commented-out-tests
      // it('should reject on a right stream of type group', async() => {
      // await expect(actor.getJoinCoefficients(
      //     {
      //       type: 'inner',
      //       entries: [
      //         {
      //           output: <any> {},
      //           operation: <any> { type: Algebra.types.GROUP },
      //         },
      //         {
      //           output: <any> {},
      //           operation: <any> {},
      //         },
      //       ],
      //       context: new ActionContext(),
      //     },
      //     [
      //       {
      //         state: new MetadataValidationState(),
      //         cardinality: { type: 'estimate', value: 3 },
      //         pageSize: 100,
      //         requestTime: 10,
      //         canContainUndefs: false,
      //         variables: [ DF.variable('a') ],
      //       },
      //       {
      //         state: new MetadataValidationState(),
      //         cardinality: { type: 'estimate', value: 2 },
      //         pageSize: 100,
      //         requestTime: 20,
      //         canContainUndefs: false,
      //         variables: [ DF.variable('a') ]},
      //     ],
      // )).rejects.toThrowError('Actor actor can not bind on Extend and Group operations');
      // });
      // eslint-disable-next-line jest/no-commented-out-tests
      // it('should not reject on a left stream of type group', async() => {
      // expect(await actor.getJoinCoefficients(
      //     {
      //       type: 'inner',
      //       entries: [
      //         {
      //           output: <any> {},
      //           operation: <any> {},
      //         },
      //         {
      //           output: <any> {},
      //           operation: <any> { type: Algebra.types.GROUP },
      //         },
      //       ],
      //       context: new ActionContext(),
      //     },
      //     [
      //       {
      //         state: new MetadataValidationState(),
      //         cardinality: { type: 'estimate', value: 3 },
      //         pageSize: 100,
      //         requestTime: 10,
      //         canContainUndefs: false,
      //         variables: [ DF.variable('a') ],
      //       },
      //       {
      //         state: new MetadataValidationState(),
      //         cardinality: { type: 'estimate', value: 2 },
      //         pageSize: 100,
      //         requestTime: 20,
      //         canContainUndefs: false,
      //         variables: [ DF.variable('a') ],
      //       },
      //     ],
      // )).toEqual({
      //     iterations: 0.480_000_000_000_000_1,
      //     persistedItems: 0,
      //     blockingItems: 0,
      //     requestTime: 0.403_840_000_000_000_03,
      // });
      // });
      //
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
              output: <any> {},
              operation: <any> {},
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
              output: <any> {},
              operation: <any> {},
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
              output: <any>{
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
              output: <any>{
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
          cardinality: { type: 'estimate', value: 2.400_000_000_000_000_4 },
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
              output: <any>{
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
              output: <any>{
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
                  cardinality: { type: 'estimate', value: 4 },
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
              output: <any>{
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
          cardinality: { type: 'estimate', value: 9.600_000_000_000_001 },
          variables: [
            {
              canBeUndef: false,
              variable: DF.variable('a'),
            },
            {
              canBeUndef: false,
              variable: DF.variable('b'),
            },
            {
              canBeUndef: false,
              variable: DF.variable('c'),
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
            a: 'b',
            [KeysStreamingSource.matchOptions.name]: [],
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
                cardinality: { type: 'estimate', value: 4 },
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
            a: 'b',
            [KeysStreamingSource.matchOptions.name]: [],
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
                cardinality: { type: 'estimate', value: 4 },
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
                  ]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 4 },
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
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([]);
      });

      it('should work if the active iterator ends first', async() => {
        const haltMock = jest.fn();
        const resumeMock = jest.fn();
        let iterator: BindingsStream;
        const stopMatchJest = jest.fn();
        const streams: PassThrough[] = [];
        let num = 0;

        const mockStreamingStore = {
          halt: haltMock,
          resume: resumeMock,
        };

        mediatorQueryOperation = <any>{
          mediate: jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
            const unionStream = new PassThrough({ objectMode: true });
            const tempStream: Stream = streamifyArray([
              BF.bindings([
                [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
              ]).setContextEntry(KeysBindings.isAddition, true),
              BF.bindings([
                [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
              ]).setContextEntry(KeysBindings.isAddition, true),
              BF.bindings([
                [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
              ]).setContextEntry(KeysBindings.isAddition, true),
            ]);
            tempStream.pipe(unionStream, { end: false });

            const stream = new PassThrough({
              objectMode: true,
            });
            stream.pipe(unionStream, { end: false });
            streams.push(stream);

            let tempEnd = false;
            let streamEnd = false;

            tempStream.on('close', () => {
              tempEnd = true;
              if (streamEnd) {
                unionStream.end();
              }
            });

            stream.on('close', () => {
              streamEnd = true;
              if (tempEnd) {
                unionStream.end();
              }
            });

            iterator = new WrappingIterator(unionStream);

            const streamNum = num;
            num++;
            const stopMatchfn = () => {
              if (streamNum === 0) {
                iterator.close();
                iterator.on('end', () => {
                  for (const streami of streams) {
                    if (!streami.closed) {
                      streami.end();
                    }
                  }
                });
              }
              stopMatchJest();
            };
            const matchOptions = arg.context.get(KeysStreamingSource.matchOptions);
            expect(matchOptions).toBeDefined();
            if (matchOptions !== undefined) {
              (<({ stopMatch: () => void })[]>matchOptions).push({
                stopMatch: stopMatchfn,
              });
            }
            return {
              bindingsStream: iterator,
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

        actor = new ActorRdfJoinInnerIncrementalComputationalMultiBind({
          name: 'actor',
          bus,
          selectivityModifier: 0.1,
          mediatorQueryOperation,
          mediatorJoinSelectivity,
          mediatorJoinEntriesSort,
          mediatorMergeBindingsContext,
          mediatorHashBindings,
        });

        const tempStream: Stream = streamifyArray([
          BF.bindings([
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
        ]);
        const alteringStream = tempStream.pipe(new PassThrough({
          objectMode: true,
        }), { end: false });
        const wrapIterator = new WrappingIterator(alteringStream);

        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.namedNode('ex:b3') ],
                  ]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: { type: 'estimate', value: 4 },
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
              output: <any>{
                bindingsStream: wrapIterator,
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

        action.entries[0].operation.metadata.scopedSource = mockStreamingStore;

        const { result } = await actor.getOutput(action, <any>(await actor.test(action)).getSideData());

        await expect(partialArrayifyStream(result.bindingsStream, 3)).resolves.toBeIsomorphicBindingsArray([
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

        alteringStream.push(
          BF.bindings([
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
        );

        await expect(partialArrayifyStream(result.bindingsStream, 3)).resolves.toBeIsomorphicBindingsArray([
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

        alteringStream.end();

        await promisifyEventEmitter(result.bindingsStream);

        expect(haltMock).toHaveBeenCalledTimes(1);
        expect(resumeMock).toHaveBeenCalledTimes(1);
        expect(stopMatchJest).toHaveBeenCalledTimes(2);
      });

      describe('with mock store', () => {
        let haltMock: Mock<any, any>;
        let resumeMock: Mock<any, any>;
        let iterator: BindingsStream;
        let stopMatchJest: Mock<any, any>;
        const streams: PassThrough[] = [];
        const mockStreamingStore = {
          halt: haltMock,
          resume: resumeMock,
        };

        beforeEach(() => {
          haltMock = jest.fn();
          resumeMock = jest.fn();
          stopMatchJest = jest.fn();

          mediatorQueryOperation = <any> {
            mediate: jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
              const unionStream = new PassThrough({ objectMode: true });
              const tempStream: Stream = streamifyArray([
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
                ]).setContextEntry(KeysBindings.isAddition, true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
                ]).setContextEntry(KeysBindings.isAddition, true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
                ]).setContextEntry(KeysBindings.isAddition, true),
              ]);
              tempStream.pipe(unionStream, { end: false });

              const stream = new PassThrough({
                objectMode: true,
              });
              stream.pipe(unionStream, { end: false });
              streams.push(stream);

              let tempEnd = false;
              let streamEnd = false;

              tempStream.on('close', () => {
                tempEnd = true;
                if (streamEnd) {
                  unionStream.end();
                }
              });

              stream.on('close', () => {
                streamEnd = true;
                if (tempEnd) {
                  unionStream.end();
                }
              });

              iterator = new WrappingIterator(unionStream);

              const stopMatchfn = () => {
                stream.end();
                stopMatchJest();
              };
              const matchOptions = arg.context.get(KeysStreamingSource.matchOptions);

              // TODO [2024-12-01]: check if this check is needed
              // expect(matchOptions).toBeDefined();
              if (matchOptions !== undefined) {
                (<({ stopMatch: () => void })[]> matchOptions).push({
                  stopMatch: stopMatchfn,
                });
              }
              return {
                bindingsStream: iterator,
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

          actor = new ActorRdfJoinInnerIncrementalComputationalMultiBind({
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

        it('should handle entries', async() => {
          const action: IActionRdfJoin = {
            type: 'inner',
            entries: [
              {
                output: <any>{
                  bindingsStream: new ArrayIterator([
                    BF.bindings([
                      [ DF.variable('b'), DF.namedNode('ex:b1') ],
                    ]).setContextEntry(KeysBindings.isAddition, true),
                    BF.bindings([
                      [ DF.variable('b'), DF.namedNode('ex:b2') ],
                    ]).setContextEntry(KeysBindings.isAddition, true),
                    BF.bindings([
                      [ DF.variable('b'), DF.namedNode('ex:b3') ],
                    ]),
                  ], { autoStart: false }),
                  metadata: () => Promise.resolve({
                    state: new MetadataValidationState(),
                    cardinality: { type: 'estimate', value: 4 },
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
                output: <any>{
                  bindingsStream: new ArrayIterator([
                    BF.bindings([
                      [ DF.variable('a'), DF.namedNode('ex:a1') ],
                    ]).setContextEntry(KeysBindings.isAddition, true),
                  ]),
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

          action.entries[0].operation.metadata.scopedSource = mockStreamingStore;

          const { result } = await actor.getOutput(action, <any>(await actor.test(action)).getSideData());

          await expect(partialArrayifyStream(result.bindingsStream, 3)).resolves.toBeIsomorphicBindingsArray([
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

          for (const stream of streams) {
            stream.push(
              BF.bindings([
                [ DF.variable('bound'), DF.namedNode('ex:bound4') ],
              ]),
            );
          }

          await expect(partialArrayifyStream(result.bindingsStream, 1)).resolves.toBeIsomorphicBindingsArray([
            BF.bindings([
              [ DF.variable('bound'), DF.namedNode('ex:bound4') ],
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ]);
          expect(haltMock).toHaveBeenCalledTimes(0);
          expect(resumeMock).toHaveBeenCalledTimes(0);

          for (const stream of streams) {
            stream.push(
              BF.bindings([
                [ DF.variable('bound'), DF.namedNode('ex:bound4') ],
              ]).setContextEntry(KeysBindings.isAddition, false),
            );
          }

          await expect(partialArrayifyStream(result.bindingsStream, 1)).resolves.toBeIsomorphicBindingsArray([
            BF.bindings([
              [ DF.variable('bound'), DF.namedNode('ex:bound4') ],
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, false),
          ]);
          expect(haltMock).toHaveBeenCalledTimes(0);
          expect(resumeMock).toHaveBeenCalledTimes(0);
          expect(stopMatchJest).toHaveBeenCalledTimes(0);
        });

        it('should handle entries with deletions', async() => {
          const tempStream: Stream = streamifyArray([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ]);
          const alteringStream = tempStream.pipe(new PassThrough({
            objectMode: true,
          }), { end: false });
          const iterator = new WrappingIterator(alteringStream);

          const action: IActionRdfJoin = {
            type: 'inner',
            entries: [
              {
                output: <any>{
                  bindingsStream: new ArrayIterator([
                    BF.bindings([
                      [ DF.variable('b'), DF.namedNode('ex:b1') ],
                    ]).setContextEntry(KeysBindings.isAddition, true),
                    BF.bindings([
                      [ DF.variable('b'), DF.namedNode('ex:b2') ],
                    ]).setContextEntry(KeysBindings.isAddition, true),
                    BF.bindings([
                      [ DF.variable('b'), DF.namedNode('ex:b3') ],
                    ]),
                  ], { autoStart: false }),
                  metadata: () => Promise.resolve({
                    state: new MetadataValidationState(),
                    cardinality: { type: 'estimate', value: 4 },
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
                output: <any>{
                  bindingsStream: iterator,
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

          action.entries[0].operation.metadata.scopedSource = mockStreamingStore;

          const { result } = await actor.getOutput(action, <any>(await actor.test(action)).getSideData());

          await expect(partialArrayifyStream(result.bindingsStream, 3)).resolves.toBeIsomorphicBindingsArray([
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

          alteringStream.push(
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
            ]),
          );

          await expect(partialArrayifyStream(result.bindingsStream, 3)).resolves.toBeIsomorphicBindingsArray([
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
          expect(haltMock).toHaveBeenCalledTimes(0);
          expect(resumeMock).toHaveBeenCalledTimes(0);

          alteringStream.push(
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, false),
          );

          await expect(partialArrayifyStream(result.bindingsStream, 3)).resolves.toBeIsomorphicBindingsArray([
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

          for (const stream of streams) {
            stream.push(
              BF.bindings([
                [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
              ]).setContextEntry(KeysBindings.isAddition, false),
            );
          }

          await expect(partialArrayifyStream(result.bindingsStream, 1)).resolves.toBeIsomorphicBindingsArray([
            BF.bindings([
              [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
            ]).setContextEntry(KeysBindings.isAddition, false),
          ]);

          const promisses = [];
          alteringStream.end();
          for (const stream of streams) {
            if (!stream.closed) {
              stream.end();
              promisses.push(promisifyEventEmitter(stream));
            }
          }

          // Await Promise.all(promisses);

          await promisifyEventEmitter(result.bindingsStream);

          expect(haltMock).toHaveBeenCalledTimes(1);
          expect(resumeMock).toHaveBeenCalledTimes(1);
          expect(stopMatchJest).toHaveBeenCalledTimes(2);
        });

        it('should handle entries with too many deletions', async() => {
          const tempStream: Stream = streamifyArray([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ]);
          const alteringStream = tempStream.pipe(new PassThrough({
            objectMode: true,
          }), { end: false });
          const iterator = new WrappingIterator(alteringStream);

          const action: IActionRdfJoin = {
            type: 'inner',
            entries: [
              {
                output: <any>{
                  bindingsStream: new ArrayIterator([
                    BF.bindings([
                      [ DF.variable('b'), DF.namedNode('ex:b1') ],
                    ]).setContextEntry(KeysBindings.isAddition, true),
                    BF.bindings([
                      [ DF.variable('b'), DF.namedNode('ex:b2') ],
                    ]).setContextEntry(KeysBindings.isAddition, true),
                    BF.bindings([
                      [ DF.variable('b'), DF.namedNode('ex:b3') ],
                    ]),
                  ], { autoStart: false }),
                  metadata: () => Promise.resolve({
                    state: new MetadataValidationState(),
                    cardinality: { type: 'estimate', value: 4 },
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
                output: <any>{
                  bindingsStream: iterator,
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

          action.entries[0].operation.metadata.scopedSource = mockStreamingStore;

          const { result } = await actor.getOutput(action, <any>(await actor.test(action)).getSideData());

          await expect(partialArrayifyStream(result.bindingsStream, 3)).resolves.toBeIsomorphicBindingsArray([
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

          alteringStream.push(
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, false),
          );

          await expect(partialArrayifyStream(result.bindingsStream, 3)).resolves.toBeIsomorphicBindingsArray([
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

          alteringStream.push(
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, false),
          );

          const promisses = [];
          alteringStream.end();
          for (const stream of streams) {
            if (!stream.closed) {
              stream.end();
              promisses.push(promisifyEventEmitter(stream));
            }
          }
          await Promise.all(promisses);

          await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([

          ]);

          expect(haltMock).toHaveBeenCalledTimes(1);
          expect(resumeMock).toHaveBeenCalledTimes(1);
          expect(stopMatchJest).toHaveBeenCalledTimes(2);
        });
      });
    });
  });
});
