import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import type { IActionRdfJoin } from '@comunica/bus-rdf-join';
import type { IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput } from '@comunica/bus-rdf-join-selectivity';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { MetadataValidationState } from '@comunica/utils-metadata';
import { KeysBindings } from '@incremunica/context-entries';
import { createTestMediatorHashBindings, createTestBindingsFactory } from '@incremunica/dev-tools';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { ActorRdfJoinIncrementalMinusHash } from '../lib/ActorRdfJoinIncrementalMinusHash';
import '@comunica/utils-jest';
import type * as RDF from '@rdfjs/types';

const DF = new DataFactory();

describe('ActorRdfJoinIncrementalMinusHash', () => {
  let bus: any;
  let context: IActionContext;
  let BF: BindingsFactory;

  beforeEach(async() => {
    bus = new Bus({ name: 'bus' });
    context = new ActionContext();
    BF = await createTestBindingsFactory(DF);
  });

  describe('An ActorRdfJoinIncrementalMinusHash instance', () => {
    let mediatorJoinSelectivity: Mediator<
      Actor<IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>,
      IActionRdfJoinSelectivity,
IActorTest,
IActorRdfJoinSelectivityOutput
>;
    let actor: ActorRdfJoinIncrementalMinusHash;
    let mediatorHashBindings: MediatorHashBindings;
    let action: IActionRdfJoin;
    let variables0: { variable: RDF.Variable; canBeUndef: boolean }[];
    let variables1: { variable: RDF.Variable; canBeUndef: boolean }[];

    beforeEach(() => {
      mediatorJoinSelectivity = <any>{
        mediate: async() => ({ selectivity: 1 }),
      };
      mediatorHashBindings = createTestMediatorHashBindings();
      actor = new ActorRdfJoinIncrementalMinusHash({
        name: 'actor',
        bus,
        mediatorJoinSelectivity,
        mediatorHashBindings,
      });
      variables0 = [];
      variables1 = [];
      action = {
        type: 'minus',
        entries: [
          {
            output: {
              bindingsStream: new ArrayIterator([], { autoStart: false }),
              metadata: async() => ({
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 1 },
                pageSize: 100,
                requestTime: 10,
                variables: variables0,
              }),
              type: 'bindings',
            },
            operation: <any> {},
          },
          {
            output: {
              bindingsStream: new ArrayIterator([], { autoStart: false }),
              metadata: async() => ({
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 1 },
                pageSize: 100,
                requestTime: 20,
                variables: variables1,
              }),
              type: 'bindings',
            },
            operation: <any> {},
          },
        ],
        context,
      };
    });

    describe('test', () => {
      it('should not test on zero entries', async() => {
        await expect(actor.test({
          type: 'minus',
          entries: [],
          context,
        })).resolves.toFailTest('actor requires at least two join entries.');
      });

      it('should not test on one entry', async() => {
        await expect(actor.test({
          type: 'minus',
          entries: <any>[{}],
          context,
        })).resolves.toFailTest('actor requires at least two join entries.');
      });

      it('should not test on three entries', async() => {
        await expect(actor.test({
          type: 'minus',
          entries: <any>[{}, {}, {}],
          context,
        })).resolves.toFailTest('actor requires 2 join entries at most. The input contained 3.');
      });

      it('should not test on a non-minus operation', async() => {
        await expect(actor.test({
          type: 'inner',
          entries: <any>[{}, {}],
          context,
        })).resolves.toFailTest(`actor can only handle logical joins of type 'minus', while 'inner' was given.`);
      });

      it('should pass on undefs on overlapping vars in left stream', async() => {
        variables0 = [{ variable: DF.variable('a'), canBeUndef: true }];
        variables1 = [{ variable: DF.variable('a'), canBeUndef: false }];
        expect((await actor.test(action)).isPassed).toBeTruthy();
      });

      it('should pass on undefs on overlapping vars in right stream', async() => {
        variables0 = [{ variable: DF.variable('a'), canBeUndef: false }];
        variables1 = [{ variable: DF.variable('a'), canBeUndef: true }];
        expect((await actor.test(action)).isPassed).toBeTruthy();
      });

      it('should pass on undefs in left and right stream', async() => {
        variables0 = [{ variable: DF.variable('a'), canBeUndef: true }];
        variables1 = [{ variable: DF.variable('a'), canBeUndef: true }];
        expect((await actor.test(action)).isPassed).toBeTruthy();
      });

      it('should test on two entries', async() => {
        await expect(actor.test({
          type: 'minus',
          entries: <any>[
            {
              output: {
                type: 'bindings',
                metadata: () => Promise.resolve({
                  cardinality: { type: 'estimate', value: 4 },
                  pageSize: 100,
                  requestTime: 10,
                  canContainUndefs: false,
                }),
              },
            },
            {
              output: {
                type: 'bindings',
                metadata: () => Promise.resolve({
                  cardinality: { type: 'estimate', value: 4 },
                  pageSize: 100,
                  requestTime: 10,
                  canContainUndefs: false,
                }),
              },
            },
          ],
          context,
        })).resolves.toEqual({
          sideData: {
            metadatas: [
              {
                canContainUndefs: false,
                cardinality: {
                  type: 'estimate',
                  value: 4,
                },
                pageSize: 100,
                requestTime: 10,
              },
              {
                canContainUndefs: false,
                cardinality: {
                  type: 'estimate',
                  value: 4,
                },
                pageSize: 100,
                requestTime: 10,
              },
            ],
          },
          value: {
            iterations: 0,
            blockingItems: 0,
            persistedItems: 0,
            requestTime: 0,
          },
        });
      });
    });

    describe('getOutput', () => {
      it('should handle entries with common variables', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  variables: [
                    {
                      canBeUndef: false,
                      variable: DF.variable('a'),
                    },
                  ],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  variables: [
                    {
                      canBeUndef: false,
                      variable: DF.variable('a'),
                    },
                  ],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toBe('bindings');
        await expect(result.metadata()).resolves
          .toEqual({
            cardinality: 3,
            variables: [{
              canBeUndef: false,
              variable: DF.variable('a'),
            }],
          });
        await expect(result.bindingsStream).toEqualBindingsStream([
          BF.bindings([
            [ DF.variable('a'), DF.literal('3') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
        ]);
      });

      it('should handle entries with common variables and deletions', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  null,
                  null,
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, false),
                  null,
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, false),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('0') ],
                  ]).setContextEntry(KeysBindings.isAddition, false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toBe('bindings');
        await expect(result.bindingsStream).toEqualBindingsStream([
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('3') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
        ]);
      });

      it('should handle entries with common variables and deletions II', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('0') ],
                  ]).setContextEntry(KeysBindings.isAddition, false),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, false),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, false),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('3') ],
                  ]).setContextEntry(KeysBindings.isAddition, false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  null,
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toBe('bindings');
        await expect(result.bindingsStream).toEqualBindingsStream([
          BF.bindings([
            [ DF.variable('a'), DF.literal('3') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('3') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
        ]);
      });

      it('should handle entries with common variables and deletions III', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                    [ DF.variable('b'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                    [ DF.variable('b'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                    [ DF.variable('b'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  variables: [
                    {
                      canBeUndef: false,
                      variable: DF.variable('a'),
                    },
                    {
                      canBeUndef: false,
                      variable: DF.variable('b'),
                    },
                  ],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  null,
                  null,
                  null,
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toBe('bindings');
        await expect(result.bindingsStream).toEqualBindingsStream([
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('1') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('1') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
          ]).setContextEntry(KeysBindings.isAddition, false),
        ]);
      });

      it('should return null on ended', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toBe('bindings');
        await expect(result.bindingsStream).toEqualBindingsStream([]);
        expect(result.bindingsStream.read()).toBeNull();
      });

      it('should be able to end when buffer is full', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  null,
                  null,
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(action.entries[0].output.bindingsStream.readable).toBeTruthy();
        expect(action.entries[1].output.bindingsStream.readable).toBeTruthy();
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        expect(result.bindingsStream.read())
          .toEqualBindings(BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
          ]).setContextEntry(KeysBindings.isAddition, true));
        action.entries[0].output.bindingsStream.close();
        action.entries[1].output.bindingsStream.close();
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        expect(action.entries[0].output.bindingsStream.ended).toBeTruthy();
        expect(action.entries[1].output.bindingsStream.ended).toBeTruthy();
        expect(result.bindingsStream.ended).toBeFalsy();
        expect(result.bindingsStream.read())
          .toEqualBindings(BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
          ]).setContextEntry(KeysBindings.isAddition, true));
        expect(result.bindingsStream.read()).toBeNull();
        expect(action.entries[0].output.bindingsStream.ended).toBeTruthy();
        expect(action.entries[1].output.bindingsStream.ended).toBeTruthy();
        expect(result.bindingsStream.ended).toBeTruthy();
      });

      it('should be not readable if both inputs are not readable', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
          ],
          context,
        };
        action.entries[0].output.bindingsStream.readable = false;
        action.entries[1].output.bindingsStream.readable = false;
        const { result } = await actor.getOutput(action);
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        expect(result.bindingsStream.readable).toBeFalsy();
      });

      it('should be readable if input 1 is readable', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([ BF.bindings([[
                  DF.variable('a'),
                  DF.literal('1'),
                ]]) ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 1,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([ BF.bindings([[
                  DF.variable('a'),
                  DF.literal('1'),
                ]]) ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 1,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
          ],
          context,
        };
        action.entries[0].output.bindingsStream.readable = false;
        action.entries[1].output.bindingsStream.readable = false;
        const { result } = await actor.getOutput(action);
        expect(result.bindingsStream.readable).toBeFalsy();
        action.entries[0].output.bindingsStream.readable = true;
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        expect(result.bindingsStream.readable).toBeTruthy();
      });

      it('should be readable if input 2 is readable', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([ BF.bindings([[
                  DF.variable('a'),
                  DF.literal('1'),
                ]]) ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 1,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([ BF.bindings([[
                  DF.variable('a'),
                  DF.literal('1'),
                ]]) ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 1,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
          ],
          context,
        };
        action.entries[0].output.bindingsStream.readable = false;
        action.entries[1].output.bindingsStream.readable = false;
        const { result } = await actor.getOutput(action);
        expect(result.bindingsStream.readable).toBeFalsy();
        action.entries[1].output.bindingsStream.readable = true;
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        expect(result.bindingsStream.readable).toBeTruthy();
      });

      it('should handle errors from right iterator', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);
        const promise = new Promise<void>(resolve => result.bindingsStream.on('error', (e) => {
          expect(e.message).toBe('Test error');
          resolve();
        }));
        action.entries[0].output.bindingsStream.destroy(new Error('Test error'));
        await promise;
      });

      it('should handle errors from left iterator', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);
        const promise = new Promise<void>(resolve => result.bindingsStream.on('error', (e) => {
          expect(e.message).toBe('Test error');
          resolve();
        }));
        action.entries[1].output.bindingsStream.destroy(new Error('Test error'));
        await promise;
      });

      it('should handle entries without common variables', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('b'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('b'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  variables: [{
                    canBeUndef: false,
                    variable: DF.variable('b'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toBe('bindings');
        await expect(result.metadata()).resolves
          .toEqual({
            cardinality: 3,
            variables: [{
              canBeUndef: false,
              variable: DF.variable('a'),
            }],
          });
        await expect(result.bindingsStream).toEqualBindingsStream([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.isAddition, true),
        ]);
      });

      it('should handle multiple bindings with undefs', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('3') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([]).setContextEntry(KeysBindings.isAddition, false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 4,
                  variables: [{
                    canBeUndef: true,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
            {
              output: <any>{
                bindingsStream: new ArrayIterator([
                  null,
                  null,
                  null,
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(KeysBindings.isAddition, true),
                  BF.bindings([]).setContextEntry(KeysBindings.isAddition, false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 4,
                  variables: [{
                    canBeUndef: true,
                    variable: DF.variable('a'),
                  }],
                }),
                type: 'bindings',
              },
              operation: <any>{},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toBe('bindings');
        await expect(result.metadata()).resolves
          .toEqual({
            cardinality: 4,
            variables: [{
              canBeUndef: true,
              variable: DF.variable('a'),
            }],
          });
        await expect(result.bindingsStream).toEqualBindingsStream([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([]).setContextEntry(KeysBindings.isAddition, false),
        ]);
      });
    });
  });
});
