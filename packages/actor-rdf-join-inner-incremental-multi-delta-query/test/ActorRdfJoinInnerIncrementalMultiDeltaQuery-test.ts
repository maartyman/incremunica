import type { MediatorMergeBindingsContext } from '@comunica/bus-merge-bindings-context';
import type { IActionQueryOperation, MediatorQueryOperation } from '@comunica/bus-query-operation';
import type { IActionRdfJoin } from '@comunica/bus-rdf-join';
import type {
  MediatorRdfJoinSelectivity,
} from '@comunica/bus-rdf-join-selectivity';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext, IQueryOperationResultBindings } from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { MetadataValidationState } from '@comunica/utils-metadata';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import { KeysDeltaQueryJoin } from '@incremunica/context-entries';
import { DevTools } from '@incremunica/dev-tools';
import arrayifyStream from 'arrayify-stream';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import '@incremunica/incremental-jest';
import { Factory } from 'sparqlalgebrajs';
import { ActorRdfJoinInnerIncrementalMultiDeltaQuery } from '../lib';

const DF = new DataFactory();
const FACTORY = new Factory();

describe('ActorRdfJoinInnerIncrementalMultiDeltaQuery', () => {
  let bus: any;
  let BF: BindingsFactory;

  beforeEach(async() => {
    bus = new Bus({ name: 'bus' });
    BF = await DevTools.createTestBindingsFactory(DF);
  });

  describe('An ActorRdfJoinInnerIncrementalMultiDeltaQuery instance', () => {
    let mediatorJoinSelectivity: MediatorRdfJoinSelectivity;
    let context: IActionContext;
    let mediatorQueryOperation: MediatorQueryOperation;
    let mediatorMergeBindingsContext: MediatorMergeBindingsContext;
    let actor: ActorRdfJoinInnerIncrementalMultiDeltaQuery;

    beforeEach(() => {
      mediatorJoinSelectivity = <any>{
        mediate: async() => ({ selectivity: 0.8 }),
      };
      context = new ActionContext({ a: 'b' });
      mediatorQueryOperation = <any>{
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
      actor = new ActorRdfJoinInnerIncrementalMultiDeltaQuery({
        name: 'actor',
        bus,
        selectivityModifier: 0.1,
        mediatorQueryOperation,
        mediatorJoinSelectivity,
        mediatorMergeBindingsContext,
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
        )).resolves.toEqual({
          iterations: 0,
          persistedItems: 0,
          blockingItems: 0,
          requestTime: 0,
        });
      });

      it('should fail if previous join was a delta query join', async() => {
        await expect(actor.getJoinCoefficients(
          {
            type: 'inner',
            entries: [
              {
                output: <any>{},
                operation: <any>{},
              },
            ],
            context: new ActionContext()
              .set(KeysDeltaQueryJoin.fromDeltaQuery, true),
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
            ],
          },
        )).rejects.toEqual(new Error('Can\'t do two delta query joins after each other'));
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
      });

      it('should handle two entries with one wrong binding (should not happen)', async() => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any>{
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
                    [ DF.variable('bound'), DF.namedNode('ex:bound4') ],
                    [ DF.variable('a'), DF.namedNode('ex:a1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
        const { result } = await actor.getOutput(action);

        // Validate output
        await expect(arrayifyStream(result.bindingsStream)).resolves.toBeIsomorphicBindingsArray([]);
      });

      it('should handle two entries with immediate deletions', async() => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any>{
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.namedNode('ex:a2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
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
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
        ]);
      });

      it('should handle two entries with deletions', async() => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any>{
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
              output: <any>{
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
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound2') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound1') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('bound'), DF.namedNode('ex:bound3') ],
            [ DF.variable('a'), DF.namedNode('ex:a1') ],
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
    });
  });
});
