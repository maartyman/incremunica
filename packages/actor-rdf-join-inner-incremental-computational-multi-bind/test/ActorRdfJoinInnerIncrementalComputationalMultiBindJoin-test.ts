import { BindingsFactory} from '@comunica/bindings-factory';
import type { IActionQueryOperation } from '@comunica/bus-query-operation';
import type { IActionRdfJoin } from '@comunica/bus-rdf-join';
import type { IActionRdfJoinEntriesSort, MediatorRdfJoinEntriesSort } from '@comunica/bus-rdf-join-entries-sort';
import type { IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput } from '@comunica/bus-rdf-join-selectivity';
import 'jest-rdf';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { ActionContext, Bus } from '@comunica/core';
import type {BindingsStream, IActionContext, IQueryOperationResultBindings} from '@comunica/types';
import {ArrayIterator, WrappingIterator} from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { Factory } from 'sparqlalgebrajs';
import { ActorRdfJoinInnerIncrementalComputationalMultiBind } from '../lib/ActorRdfJoinInnerIncrementalComputationalMultiBind';
import Mock = jest.Mock;
import '@incremunica/incremental-jest';
import arrayifyStream from "arrayify-stream";
import {KeysQueryOperation} from "@comunica/context-entries";
import {EventEmitter} from "events";
import {PassThrough, Stream} from "readable-stream";
import {promisifyEventEmitter} from "event-emitter-promisify/dist";
import { MetadataValidationState } from '@comunica/metadata';
import {KeysStreamingSource} from "@incremunica/context-entries";
import {ActionContextKeyIsAddition} from "@incremunica/actor-merge-bindings-context-is-addition";
import {DevTools} from "@incremunica/dev-tools";


const streamifyArray = require('streamify-array');
const DF = new DataFactory();
const FACTORY = new Factory();

async function partialArrayifyStream(stream: EventEmitter, num: number): Promise<any[]> {
  let array: any[] = [];
  for (let i = 0; i < num; i++) {
    await new Promise<void>((resolve) => stream.once("data", (bindings: any) => {
      array.push(bindings);
      resolve();
    }));
  }
  return array;
}


describe('ActorRdfJoinIncrementalComputationalMultiBind', () => {
  let bus: any;
  let BF: BindingsFactory;

  beforeEach(async () => {
    bus = new Bus({name: 'bus'});
    BF = await DevTools.createBindingsFactory(DF);
  });

  describe('An ActorRdfJoinIncrementalComputationalMultiBind instance', () => {
    let mediatorJoinSelectivity: Mediator<
      Actor<IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>,
      IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>;
    let mediatorJoinEntriesSort: MediatorRdfJoinEntriesSort;
    let context: IActionContext;
    let mediatorQueryOperation: Mediator<Actor<IActionQueryOperation, IActorTest, IQueryOperationResultBindings>,
      IActionQueryOperation, IActorTest, IQueryOperationResultBindings>;
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
      actor = new ActorRdfJoinInnerIncrementalComputationalMultiBind({
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
        expect(await actor.getJoinCoefficients(
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
        )).toEqual({
          iterations: 0,
          persistedItems: 0,
          blockingItems: 0,
          requestTime: 0,
        });
      });

      /*
      it('should handle three entries with a lower variable overlap', async() => {
        expect(await actor.getJoinCoefficients(
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
        )).toEqual({
          iterations: 1.280_000_000_000_000_2,
          persistedItems: 0,
          blockingItems: 0,
          requestTime: 0.440_96,
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
                    state: new MetadataValidationState(),
                    cardinality: { type: 'estimate', value: 3 },
                    canContainUndefs: false,
                  }),

                },
                operation: <any>{ type: Algebra.types.EXTEND },
              },
              {
                output: <any>{
                  metadata: () => Promise.resolve({
                    state: new MetadataValidationState(),
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
        )).rejects.toThrowError('Actor actor can not bind on Extend and Group operations');
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
              variables: [ DF.variable('a') ]},
          ],
        )).rejects.toThrowError('Actor actor can not bind on Extend and Group operations');
      });

      it('should not reject on a left stream of type group', async() => {
        expect(await actor.getJoinCoefficients(
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
        )).toEqual({
          iterations: 0.480_000_000_000_000_1,
          persistedItems: 0,
          blockingItems: 0,
          requestTime: 0.403_840_000_000_000_03,
        });
      });
      */
    });

    describe('sortJoinEntries', () => {
      it('sorts 2 entries', async() => {
        expect(await actor.sortJoinEntries(
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
        )).toEqual([
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
        expect(await actor.sortJoinEntries([
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
          ],
          context)).toEqual([
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
        expect(await actor.sortJoinEntries(
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
        )).toEqual([
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
        expect(await actor.sortJoinEntries(
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
        )).toEqual([
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
        expect(await actor.sortJoinEntries(
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
        )).toEqual([
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
        expect(await actor.sortJoinEntries(
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
        )).toEqual([
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
      it('should handle two entries without context', async () => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b1')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b2')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b3')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: {type: 'estimate', value: 3},
                  canContainUndefs: false,
                  variables: [DF.variable('a'), DF.variable('b')],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a1')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a2')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: {type: 'estimate', value: 1},
                  canContainUndefs: false,
                  variables: [DF.variable('a')],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };
        const {result} = await actor.getOutput(action);

        // Validate output
        expect(result.type).toEqual('bindings');
        expect(await arrayifyStream(result.bindingsStream)).toBeIsomorphicBindingsArray([
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ]);
        expect(await result.metadata()).toEqual({
          state: new MetadataValidationState(),
          cardinality: {type: 'estimate', value: 2.400_000_000_000_000_4},
          canContainUndefs: false,
          variables: [DF.variable('a'), DF.variable('b')],
        });
      });

      it('should handle three entries', async () => {
        const action: IActionRdfJoin = {
          context,
          type: 'inner',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b1')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b2')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b3')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: {type: 'estimate', value: 3},
                  canContainUndefs: false,
                  variables: [DF.variable('a'), DF.variable('b')],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [DF.variable('c'), DF.namedNode('ex:c1')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [DF.variable('c'), DF.namedNode('ex:c2')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [DF.variable('c'), DF.namedNode('ex:c3')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: {type: 'estimate', value: 4},
                  canContainUndefs: false,
                  variables: [DF.variable('a'), DF.variable('c')],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.variable('c')),
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a1')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a2')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: {type: 'estimate', value: 1},
                  canContainUndefs: false,
                  variables: [DF.variable('a')],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
        };
        const {result} = await actor.getOutput(action);

        // Validate output
        expect(result.type).toEqual('bindings');
        expect(await arrayifyStream(result.bindingsStream)).toBeIsomorphicBindingsArray([
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ]);
        expect(await result.metadata()).toEqual({
          state: new MetadataValidationState(),
          cardinality: {type: 'estimate', value: 9.600_000_000_000_001},
          canContainUndefs: false,
          variables: [DF.variable('a'), DF.variable('b'), DF.variable('c')],
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
              cardinality: {type: 'estimate', value: 1},
              canContainUndefs: false,
              variables: [DF.variable('a')],
            },
            [KeysQueryOperation.joinRightMetadatas.name]: [
              {
                state: expect.any(MetadataValidationState),
                cardinality: {type: 'estimate', value: 3},
                canContainUndefs: false,
                variables: [DF.variable('a'), DF.variable('b')],
              },
              {
                state: expect.any(MetadataValidationState),
                cardinality: {type: 'estimate', value: 4},
                canContainUndefs: false,
                variables: [DF.variable('a'), DF.variable('c')],
              },
            ],
            [KeysQueryOperation.joinBindings.name]: BF.bindings([
              [DF.variable('a'), DF.namedNode('ex:a1')],
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
            [KeysStreamingSource.matchOptions.name]: [],
            [KeysQueryOperation.joinLeftMetadata.name]: {
              state: expect.any(MetadataValidationState),
              cardinality: {type: 'estimate', value: 1},
              canContainUndefs: false,
              variables: [DF.variable('a')],
            },
            [KeysQueryOperation.joinRightMetadatas.name]: [
              {
                state: expect.any(MetadataValidationState),
                cardinality: {type: 'estimate', value: 3},
                canContainUndefs: false,
                variables: [DF.variable('a'), DF.variable('b')],
              },
              {
                state: expect.any(MetadataValidationState),
                cardinality: {type: 'estimate', value: 4},
                canContainUndefs: false,
                variables: [DF.variable('a'), DF.variable('c')],
              },
            ],
            [KeysQueryOperation.joinBindings.name]: BF.bindings([
              [DF.variable('a'), DF.namedNode('ex:a2')],
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
                  ])
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
                    [ DF.variable('bound'), DF.namedNode('ex:bound4') ],
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
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
        expect(await arrayifyStream(result.bindingsStream)).toBeIsomorphicBindingsArray([]);
      });

      it("should work if the active iterator ends first", async () => {
        let haltMock: Mock<any, any>;
        let resumeMock: Mock<any, any>;
        let iterator: BindingsStream;
        let stopMatchJest: Mock<any, any>;
        let streams: PassThrough[] = [];
        let num = 0;

        haltMock = jest.fn();
        resumeMock = jest.fn();

        let mockStreamingStore = {
          halt: haltMock,
          resume: resumeMock
        }

        stopMatchJest = jest.fn();

        mediatorQueryOperation = <any>{
          mediate: jest.fn(async (arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
            const unionStream = new PassThrough({objectMode: true});
            const tempStream: Stream = streamifyArray([
              BF.bindings([
                [DF.variable('bound'), DF.namedNode('ex:bound1')],
              ]).setContextEntry(new ActionContextKeyIsAddition(), true),
              BF.bindings([
                [DF.variable('bound'), DF.namedNode('ex:bound2')],
              ]).setContextEntry(new ActionContextKeyIsAddition(), true),
              BF.bindings([
                [DF.variable('bound'), DF.namedNode('ex:bound3')],
              ]).setContextEntry(new ActionContextKeyIsAddition(), true),
            ]);
            tempStream.pipe(unionStream, {end: false});

            let stream = new PassThrough({
              objectMode: true
            });
            stream.pipe(unionStream, {end: false});
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

            let streamNum = num;
            num++;
            let stopMatchfn = () => {
              if (streamNum == 0) {
                iterator.close();
                iterator.on("end", () => {
                  for (const streami of streams) {
                    if (!streami.closed) {
                      streami.end();
                    }
                  }
                });
              }
              stopMatchJest();
            }
            let matchOptions = arg.context.get(KeysStreamingSource.matchOptions);
            -expect(matchOptions).not.toBeUndefined()
            if (matchOptions !== undefined) {
              (<({ stopMatch: () => void })[]>matchOptions).push({
                stopMatch: stopMatchfn
              });
            }
            return {
              bindingsStream: iterator,
              metadata: () => Promise.resolve({
                state: new MetadataValidationState(),
                cardinality: {type: 'estimate', value: 3},
                canContainUndefs: false,
                variables: [DF.variable('bound')],
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
        });

        const tempStream: Stream = streamifyArray([
          BF.bindings([
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ]);
        let alteringStream = tempStream.pipe(new PassThrough({
          objectMode: true
        }), {end: false});
        let wrapIterator = new WrappingIterator(alteringStream);

        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b1')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b2')],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b3')],
                  ])
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
                  state: new MetadataValidationState(),
                  cardinality: {type: 'estimate', value: 4},
                  canContainUndefs: false,
                  variables: [DF.variable('a'), DF.variable('b')],
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
                  cardinality: {type: 'estimate', value: 1},
                  canContainUndefs: false,
                  variables: [DF.variable('a')],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
            },
          ],
          context,
        };

        action.entries[0].operation.metadata.scopedSource = mockStreamingStore;

        const {result} = await actor.getOutput(action);

        expect(await partialArrayifyStream(result.bindingsStream, 3)).toBeIsomorphicBindingsArray([
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ]);

        alteringStream.push(
          BF.bindings([
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false)
        );

        expect(await partialArrayifyStream(result.bindingsStream, 3)).toBeIsomorphicBindingsArray([
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
        ]);

        alteringStream.end();

        await promisifyEventEmitter(result.bindingsStream)

        expect(haltMock).toHaveBeenCalledTimes(1);
        expect(resumeMock).toHaveBeenCalledTimes(1);
        expect(stopMatchJest).toHaveBeenCalledTimes(2);
      })

      describe("with mock store", () => {
        let haltMock: Mock<any, any>;
        let resumeMock: Mock<any, any>;
        let iterator: BindingsStream;
        let stopMatchJest: Mock<any, any>;
        let streams: PassThrough[] = [];
        let mockStreamingStore = {
          halt: haltMock,
          resume: resumeMock
        }

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
                ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
                ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                BF.bindings([
                  [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
                ]).setContextEntry(new ActionContextKeyIsAddition(), true),
              ]);
              tempStream.pipe(unionStream, {end: false});

              let stream = new PassThrough({
                objectMode: true
              });
              stream.pipe(unionStream, {end: false});
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

              let stopMatchfn = () => {
                stream.end();
                stopMatchJest();
              }
              let matchOptions = arg.context.get(KeysStreamingSource.matchOptions);
-              expect(matchOptions).not.toBeUndefined()
              if (matchOptions !== undefined) {
                (<({ stopMatch: () => void })[]> matchOptions).push({
                  stopMatch: stopMatchfn
                });
              }
              return {
                bindingsStream: iterator,
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

          actor = new ActorRdfJoinInnerIncrementalComputationalMultiBind({
            name: 'actor',
            bus,
            selectivityModifier: 0.1,
            mediatorQueryOperation,
            mediatorJoinSelectivity,
            mediatorJoinEntriesSort,
          });
        })

        it('should handle entries', async () => {
          const action: IActionRdfJoin = {
            type: 'inner',
            entries: [
              {
                output: <any>{
                  bindingsStream: new ArrayIterator([
                    BF.bindings([
                      [DF.variable('b'), DF.namedNode('ex:b1')],
                    ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                    BF.bindings([
                      [DF.variable('b'), DF.namedNode('ex:b2')],
                    ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                    BF.bindings([
                      [DF.variable('b'), DF.namedNode('ex:b3')],
                    ])
                  ], {autoStart: false}),
                  metadata: () => Promise.resolve({
                    state: new MetadataValidationState(),
                    cardinality: {type: 'estimate', value: 4},
                    canContainUndefs: false,
                    variables: [DF.variable('a'), DF.variable('b')],
                  }),
                  type: 'bindings',
                },
                operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
              },
              {
                output: <any>{
                  bindingsStream: new ArrayIterator([
                    BF.bindings([
                      [DF.variable('a'), DF.namedNode('ex:a1')],
                    ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  ]),
                  metadata: () => Promise.resolve({
                    state: new MetadataValidationState(),
                    cardinality: {type: 'estimate', value: 1},
                    canContainUndefs: false,
                    variables: [DF.variable('a')],
                  }),
                  type: 'bindings',
                },
                operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
              },
            ],
            context,
          };

          action.entries[0].operation.metadata.scopedSource = mockStreamingStore;

          const {result} = await actor.getOutput(action);

          expect(await partialArrayifyStream(result.bindingsStream, 3)).toBeIsomorphicBindingsArray([
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound1')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound2')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound3')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);

          for (const stream of streams) {
            stream.push(
              BF.bindings([
                [DF.variable('bound'), DF.namedNode('ex:bound4')],
              ])
            );
          }

          expect(await partialArrayifyStream(result.bindingsStream, 1)).toBeIsomorphicBindingsArray([
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound4')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);
          expect(haltMock).toHaveBeenCalledTimes(0);
          expect(resumeMock).toHaveBeenCalledTimes(0);

          for (const stream of streams) {
            stream.push(
              BF.bindings([
                [DF.variable('bound'), DF.namedNode('ex:bound4')],
              ]).setContextEntry(new ActionContextKeyIsAddition(), false)
            );
          }

          expect(await partialArrayifyStream(result.bindingsStream, 1)).toBeIsomorphicBindingsArray([
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound4')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          ]);
          expect(haltMock).toHaveBeenCalledTimes(0);
          expect(resumeMock).toHaveBeenCalledTimes(0);
          expect(stopMatchJest).toHaveBeenCalledTimes(0);
        });

        it('should handle entries with deletions', async () => {
          const tempStream: Stream = streamifyArray([
            BF.bindings([
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);
          let alteringStream = tempStream.pipe(new PassThrough({
            objectMode: true
          }), {end: false});
          let iterator = new WrappingIterator(alteringStream);

          const action: IActionRdfJoin = {
            type: 'inner',
            entries: [
              {
                output: <any>{
                  bindingsStream: new ArrayIterator([
                    BF.bindings([
                      [DF.variable('b'), DF.namedNode('ex:b1')],
                    ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                    BF.bindings([
                      [DF.variable('b'), DF.namedNode('ex:b2')],
                    ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                    BF.bindings([
                      [DF.variable('b'), DF.namedNode('ex:b3')],
                    ])
                  ], {autoStart: false}),
                  metadata: () => Promise.resolve({
                    state: new MetadataValidationState(),
                    cardinality: {type: 'estimate', value: 4},
                    canContainUndefs: false,
                    variables: [DF.variable('a'), DF.variable('b')],
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
                    cardinality: {type: 'estimate', value: 1},
                    canContainUndefs: false,
                    variables: [DF.variable('a')],
                  }),
                  type: 'bindings',
                },
                operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
              },
            ],
            context,
          };

          action.entries[0].operation.metadata.scopedSource = mockStreamingStore;

          const {result} = await actor.getOutput(action);

          expect(await partialArrayifyStream(result.bindingsStream, 3)).toBeIsomorphicBindingsArray([
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound1')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound2')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound3')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);

          alteringStream.push(
            BF.bindings([
              [DF.variable('a'), DF.namedNode('ex:a2')],
            ])
          );

          expect(await partialArrayifyStream(result.bindingsStream, 3)).toBeIsomorphicBindingsArray([
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound1')],
              [DF.variable('a'), DF.namedNode('ex:a2')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound2')],
              [DF.variable('a'), DF.namedNode('ex:a2')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound3')],
              [DF.variable('a'), DF.namedNode('ex:a2')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);
          expect(haltMock).toHaveBeenCalledTimes(0);
          expect(resumeMock).toHaveBeenCalledTimes(0);

          alteringStream.push(
            BF.bindings([
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), false)
          );

          expect(await partialArrayifyStream(result.bindingsStream, 3)).toBeIsomorphicBindingsArray([
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound1')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), false),
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound2')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), false),
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound3')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          ]);

          for (const stream of streams) {
            stream.push(
              BF.bindings([
                [DF.variable('bound'), DF.namedNode('ex:bound3')],
              ]).setContextEntry(new ActionContextKeyIsAddition(), false)
            );
          }

          expect(await partialArrayifyStream(result.bindingsStream, 1)).toBeIsomorphicBindingsArray([
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound3')],
              [DF.variable('a'), DF.namedNode('ex:a2')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          ]);

          let promisses = [];
          alteringStream.end();
          for (const stream of streams) {
            if (!stream.closed) {
              stream.end();
              promisses.push(promisifyEventEmitter(stream));
            }
          }

          //await Promise.all(promisses);

          await promisifyEventEmitter(result.bindingsStream)

          expect(haltMock).toHaveBeenCalledTimes(1);
          expect(resumeMock).toHaveBeenCalledTimes(1);
          expect(stopMatchJest).toHaveBeenCalledTimes(2);
        });

        it('should handle entries with too many deletions', async () => {
          const tempStream: Stream = streamifyArray([
            BF.bindings([
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);
          let alteringStream = tempStream.pipe(new PassThrough({
            objectMode: true
          }), {end: false});
          let iterator = new WrappingIterator(alteringStream);

          const action: IActionRdfJoin = {
            type: 'inner',
            entries: [
              {
                output: <any>{
                  bindingsStream: new ArrayIterator([
                    BF.bindings([
                      [DF.variable('b'), DF.namedNode('ex:b1')],
                    ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                    BF.bindings([
                      [DF.variable('b'), DF.namedNode('ex:b2')],
                    ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                    BF.bindings([
                      [DF.variable('b'), DF.namedNode('ex:b3')],
                    ])
                  ], {autoStart: false}),
                  metadata: () => Promise.resolve({
                    state: new MetadataValidationState(),
                    cardinality: {type: 'estimate', value: 4},
                    canContainUndefs: false,
                    variables: [DF.variable('a'), DF.variable('b')],
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
                    cardinality: {type: 'estimate', value: 1},
                    canContainUndefs: false,
                    variables: [DF.variable('a')],
                  }),
                  type: 'bindings',
                },
                operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
              },
            ],
            context,
          };

          action.entries[0].operation.metadata.scopedSource = mockStreamingStore;

          const {result} = await actor.getOutput(action);

          expect(await partialArrayifyStream(result.bindingsStream, 3)).toBeIsomorphicBindingsArray([
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound1')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound2')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound3')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);

          alteringStream.push(
            BF.bindings([
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), false)
          );

          expect(await partialArrayifyStream(result.bindingsStream, 3)).toBeIsomorphicBindingsArray([
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound1')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), false),
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound2')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), false),
            BF.bindings([
              [DF.variable('bound'), DF.namedNode('ex:bound3')],
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          ]);

          alteringStream.push(
            BF.bindings([
              [DF.variable('a'), DF.namedNode('ex:a1')],
            ]).setContextEntry(new ActionContextKeyIsAddition(), false)
          );

          let promisses = [];
          alteringStream.end();
          for (const stream of streams) {
            if (!stream.closed) {
              stream.end();
              promisses.push(promisifyEventEmitter(stream));
            }
          }
          await Promise.all(promisses);

          expect(await arrayifyStream(result.bindingsStream)).toBeIsomorphicBindingsArray([

          ]);

          expect(haltMock).toHaveBeenCalledTimes(1);
          expect(resumeMock).toHaveBeenCalledTimes(1);
          expect(stopMatchJest).toHaveBeenCalledTimes(2);
        });
      });
    });
  });
});
