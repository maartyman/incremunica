import { BindingsFactory } from '@incremunica/incremental-bindings-factory';
import {ActionContext, Bus} from '@comunica/core';
import { DataFactory } from 'rdf-data-factory';
import '@incremunica/incremental-jest';
import {Factory} from "sparqlalgebrajs";
import {
  MediatorRdfJoinSelectivity
} from "@comunica/bus-rdf-join-selectivity";
import {IActionContext, IQueryOperationResultBindings} from "@comunica/types";
import {IActionQueryOperation, MediatorQueryOperation} from "@comunica/bus-query-operation";
import {ArrayIterator} from "asynciterator";
import {IActionRdfJoin} from "@comunica/bus-rdf-join";
import arrayifyStream from "arrayify-stream";
import {ActorRdfJoinInnerIncrementalMultiDeltaQuery} from "../lib";
import { MetadataValidationState } from '@comunica/metadata';

const DF = new DataFactory();
const BF = new BindingsFactory();
const FACTORY = new Factory();

describe('ActorRdfJoinInnerIncrementalMultiDeltaQuery', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfJoinInnerIncrementalMultiDeltaQuery instance', () => {
    let mediatorJoinSelectivity: MediatorRdfJoinSelectivity;
    let context: IActionContext;
    let mediatorQueryOperation: MediatorQueryOperation;
    let actor: ActorRdfJoinInnerIncrementalMultiDeltaQuery;

    beforeEach(() => {
      mediatorJoinSelectivity = <any>{
        mediate: async () => ({selectivity: 0.8}),
      };
      context = new ActionContext({a: 'b'});
      mediatorQueryOperation = <any>{
        mediate: jest.fn(async (arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
          return {
            bindingsStream: new ArrayIterator([
              BF.bindings([
                [DF.variable('bound'), DF.namedNode('ex:bound1')],
              ]),
              BF.bindings([
                [DF.variable('bound'), DF.namedNode('ex:bound2')],
              ]),
              BF.bindings([
                [DF.variable('bound'), DF.namedNode('ex:bound3')],
              ]),
            ], {autoStart: false}),
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
      actor = new ActorRdfJoinInnerIncrementalMultiDeltaQuery({
        name: 'actor',
        bus,
        selectivityModifier: 0.1,
        mediatorQueryOperation,
        mediatorJoinSelectivity,
      });
    });

    describe('getJoinCoefficients', () => {
      it('should handle three entries', async () => {
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
              cardinality: {type: 'estimate', value: 3},
              pageSize: 100,
              requestTime: 10,
              canContainUndefs: false,
              variables: [DF.variable('a')],
            },
            {
              state: new MetadataValidationState(),
              cardinality: {type: 'estimate', value: 2},
              pageSize: 100,
              requestTime: 20,
              canContainUndefs: false,
              variables: [DF.variable('a')],
            },
            {
              state: new MetadataValidationState(),
              cardinality: {type: 'estimate', value: 5},
              pageSize: 100,
              requestTime: 30,
              canContainUndefs: false,
              variables: [DF.variable('a')],
            },
          ],
          //TODO
        )).toEqual({
          iterations: 0,
          persistedItems: 0,
          blockingItems: 0,
          requestTime: 0,
        });
      });

      it('should fail if previous join was a delta query join', async () => {
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
              .set(ActorRdfJoinInnerIncrementalMultiDeltaQuery.keyFromDeltaQuery, true),
          },
          [
            {
              state: new MetadataValidationState(),
              cardinality: {type: 'estimate', value: 3},
              pageSize: 100,
              requestTime: 10,
              canContainUndefs: false,
              variables: [DF.variable('a')],
            },
          ],
        )).rejects.toEqual(new Error("Can't do two delta query joins after each other"))
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
                  ]),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b2')],
                  ]),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b3')],
                  ]),
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
                  ]),
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a2')],
                  ]),
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
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
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
                  ]),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b2')],
                  ]),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b3')],
                  ]),
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
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
                  ]),
                  BF.bindings([
                    [DF.variable('c'), DF.namedNode('ex:c2')],
                  ]),
                  BF.bindings([
                    [DF.variable('c'), DF.namedNode('ex:c3')],
                  ]),
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
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
                  ]),
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a2')],
                  ]),
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
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
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
        ]);
      });

      it('should handle two entries with one wrong binding (should not happen)', async () => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b1')],
                  ]),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b2')],
                  ]),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b3')],
                  ])
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
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
                    [DF.variable('bound'), DF.namedNode('ex:bound4')],
                    [DF.variable('a'), DF.namedNode('ex:a1')],
                  ]),
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
                  cardinality: {type: 'estimate', value: 1},
                  canContainUndefs: false,
                  variables: [DF.variable('a'), DF.variable('bound')],
                }),
                type: 'bindings',
              },
              operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.variable('bound')),
            },
          ],
          context,
        };
        const {result} = await actor.getOutput(action);

        // Validate output
        expect(await arrayifyStream(result.bindingsStream)).toBeIsomorphicBindingsArray([]);
      });

      it('should handle two entries with immediate deletions', async () => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b1')],
                  ]),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b2')],
                  ]),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b3')],
                  ])
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
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
                  ]),
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a2')],
                  ]),
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a2')],
                  ], false),
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
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
        expect(await arrayifyStream(result.bindingsStream)).toBeIsomorphicBindingsArray([
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ],false),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ],false),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ],false),
        ]);
      });

      it('should handle two entries with deletions', async () => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b1')],
                  ]),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b2')],
                  ]),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b3')],
                  ])
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
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
                  ]),
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a2')],
                  ]),
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a3')],
                  ]),
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a1')],
                  ], false),
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
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
        expect(await arrayifyStream(result.bindingsStream)).toBeIsomorphicBindingsArray([
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a3')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a3')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a3')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ], false),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ], false),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ], false),
        ]);
      });

      it('should handle two entries with multiple identical bindings and removal', async () => {
        const action: IActionRdfJoin = {
          type: 'inner',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b1')],
                  ]),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b2')],
                  ]),
                  BF.bindings([
                    [DF.variable('b'), DF.namedNode('ex:b3')],
                  ])
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
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
                  ]),
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a2')],
                  ]),
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a1')],
                  ]),
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a2')],
                  ]),
                  BF.bindings([
                    [DF.variable('a'), DF.namedNode('ex:a1')],
                  ], false),
                ], {autoStart: false}),
                metadata: () => Promise.resolve({
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
        expect(await arrayifyStream(result.bindingsStream)).toBeIsomorphicBindingsArray([
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a2')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ]),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound1')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ], false),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound2')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ], false),
          BF.bindings([
            [DF.variable('bound'), DF.namedNode('ex:bound3')],
            [DF.variable('a'), DF.namedNode('ex:a1')],
          ], false),
        ]);
      });
    });
  });
});
