import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import type { IActionRdfJoin } from '@comunica/bus-rdf-join';
import type { IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput } from '@comunica/bus-rdf-join-selectivity';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import {
  ActionContextKeyIsAddition,
} from '@incremunica/actor-merge-bindings-context-is-addition';
import { DevTools } from '@incremunica/dev-tools';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { ActorRdfJoinIncrementalMinusHash } from '../lib/ActorRdfJoinIncrementalMinusHash';
import '@comunica/utils-jest';

const DF = new DataFactory();

describe('ActorRdfJoinIncrementalMinusHash', () => {
  let bus: any;
  let context: IActionContext;
  let BF: BindingsFactory;

  beforeEach(async() => {
    bus = new Bus({ name: 'bus' });
    context = new ActionContext();
    BF = await DevTools.createTestBindingsFactory(DF);
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

    beforeEach(() => {
      mediatorJoinSelectivity = <any>{
        mediate: async() => ({ selectivity: 1 }),
      };
      mediatorHashBindings = DevTools.createTestMediatorHashBindings();
      actor = new ActorRdfJoinIncrementalMinusHash({
        name: 'actor',
        bus,
        mediatorJoinSelectivity,
        mediatorHashBindings,
      });
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

      it('should test on two entries with undefs', async() => {
        await expect(actor.test({
          type: 'minus',
          entries: <any>[
            {
              output: {
                type: 'bindings',
                metadata: () => Promise.resolve(
                  { cardinality: 4, pageSize: 100, requestTime: 10 },
                ),
              },
            },
            {
              output: {
                type: 'bindings',
                metadata: () => Promise.resolve(
                  { cardinality: 4, pageSize: 100, requestTime: 10 },
                ),
              },
            },
          ],
          context,
        })).resolves.toEqual({
          sideData: {
            metadatas: [
              {
                cardinality: 4,
                pageSize: 100,
                requestTime: 10,
              },
              {
                cardinality: 4,
                pageSize: 100,
                requestTime: 10,
              },
            ],
          },
          value: {
            blockingItems: 0,
            iterations: 0,
            persistedItems: 0,
            requestTime: 0,
          },
        });
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('3') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('3') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  null,
                  null,
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
                  null,
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('0') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
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
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('3') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('3') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('0') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('3') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  null,
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('3') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                    [ DF.variable('b'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                    [ DF.variable('b'), DF.literal('1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
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
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  null,
                  null,
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), false),
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
          ]).setContextEntry(new ActionContextKeyIsAddition(), true));
        action.entries[0].output.bindingsStream.close();
        action.entries[1].output.bindingsStream.close();
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        expect(action.entries[0].output.bindingsStream.ended).toBeTruthy();
        expect(action.entries[1].output.bindingsStream.ended).toBeTruthy();
        expect(result.bindingsStream.ended).toBeFalsy();
        expect(result.bindingsStream.read())
          .toEqualBindings(BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true));
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('3') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
                  BF.bindings([
                    [ DF.variable('b'), DF.literal('2') ],
                  ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
          .toEqual({ cardinality: 3, variables: [{
            canBeUndef: false,
            variable: DF.variable('a'),
          }]});
        await expect(result.bindingsStream).toEqualBindingsStream([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ]);
      });
    });
  });
});
