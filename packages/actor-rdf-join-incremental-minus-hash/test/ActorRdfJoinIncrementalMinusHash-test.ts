import { BindingsFactory } from '@incremunica/incremental-bindings-factory';
import type { IActionRdfJoin } from '@comunica/bus-rdf-join';
import type { IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput } from '@comunica/bus-rdf-join-selectivity';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import {ArrayIterator, WrappingIterator} from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { ActorRdfJoinIncrementalMinusHash } from '../lib/ActorRdfJoinIncrementalMinusHash';
import '@comunica/jest';
import {BindingsStream} from "@incremunica/incremental-types";

const DF = new DataFactory();
const BF = new BindingsFactory();

describe('ActorRdfJoinIncrementalMinusHash', () => {
  let bus: any;
  let context: IActionContext;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    context = new ActionContext();
  });

  describe('An ActorRdfJoinIncrementalMinusHash instance', () => {
    let mediatorJoinSelectivity: Mediator<
      Actor<IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>,
      IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>;
    let actor: ActorRdfJoinIncrementalMinusHash;

    beforeEach(() => {
      mediatorJoinSelectivity = <any> {
        mediate: async() => ({ selectivity: 1 }),
      };
      actor = new ActorRdfJoinIncrementalMinusHash({ name: 'actor', bus, mediatorJoinSelectivity });
    });

    describe('test', () => {
      it('should not test on zero entries', async() => {
        await expect(actor.test({
          type: 'minus',
          entries: [],
          context,
        })).rejects.toThrow('actor requires at least two join entries.');
      });

      it('should not test on one entry', async() => {
        await expect(actor.test({
          type: 'minus',
          entries: <any> [{}],
          context,
        })).rejects.toThrow('actor requires at least two join entries.');
      });

      it('should not test on three entries', async() => {
        await expect(actor.test({
          type: 'minus',
          entries: <any> [{}, {}, {}],
          context,
        })).rejects.toThrow('actor requires 2 join entries at most. The input contained 3.');
      });

      it('should not test on a non-minus operation', async() => {
        await expect(actor.test({
          type: 'inner',
          entries: <any> [{}, {}],
          context,
        })).rejects.toThrow(`actor can only handle logical joins of type 'minus', while 'inner' was given.`);
      });

      it('should test on two entries with undefs', async() => {
        expect(await actor.test({
          type: 'minus',
          entries: <any> [
            {
              output: {
                type: 'bindings',
                metadata: () => Promise.resolve(
                  { cardinality: 4, pageSize: 100, requestTime: 10, canContainUndefs: true },
                ),
              },
            },
            {
              output: {
                type: 'bindings',
                metadata: () => Promise.resolve(
                  { cardinality: 4, pageSize: 100, requestTime: 10, canContainUndefs: true },
                ),
              },
            },
          ],
          context,
        })).toEqual({
          iterations: 0,
          blockingItems: 0,
          persistedItems: 0,
          requestTime: 0,
        });
      });

      it('should test on two entries', async() => {
        expect(await actor.test({
          type: 'minus',
          entries: <any> [
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
        })).toEqual({
          iterations: 0,
          blockingItems: 0,
          persistedItems: 0,
          requestTime: 0,
        });
      });
    });

    describe('getOutput', () => {
      it('should handle entries with common variables', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toEqual('bindings');
        expect(await result.metadata())
          .toEqual({ cardinality: 3, canContainUndefs: false, variables: [ DF.variable('a') ]});
        await expect(result.bindingsStream).toEqualBindingsStream([
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
        ]);
      });

      it('should handle entries with common variables and deletions', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
                  null,
                  null,
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]], false),
                  null,
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]], false),
                  BF.bindings([[ DF.variable('a'), DF.literal('0') ]], false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toEqual('bindings');
        await expect(result.bindingsStream).toEqualBindingsStream([
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ]
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ]
          ], false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ]
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('3') ]
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ]
          ]),
        ]);
      });

      it('should handle entries with common variables and deletions II', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('0') ]], false),
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]], false),
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]], false),
                  BF.bindings([[ DF.variable('a'), DF.literal('3') ]], false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
                  null,
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toEqual('bindings');
        await expect(result.bindingsStream).toEqualBindingsStream([
          BF.bindings([
            [ DF.variable('a'), DF.literal('3') ]
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('3') ]
          ], false),
        ]);
      });

      it('should handle entries with common variables and deletions III', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                    [ DF.variable('b'), DF.literal('1') ]
                  ]),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                    [ DF.variable('b'), DF.literal('2') ]
                  ]),
                  BF.bindings([
                    [ DF.variable('a'), DF.literal('1') ],
                    [ DF.variable('b'), DF.literal('1') ]
                  ], false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  canContainUndefs: false,
                  variables: [ DF.variable('a'), DF.variable('b') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  null,
                  null,
                  null,
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]], false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toEqual('bindings');
        await expect(result.bindingsStream).toEqualBindingsStream([
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('1') ]
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ]
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('1') ]
          ], false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ]
          ], false),
        ]);
      });

      it('should return null on ended', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toEqual('bindings');
        await expect(result.bindingsStream).toEqualBindingsStream([]);
        await expect(result.bindingsStream.read()).toEqual(null);
      });

      it('should be able to end when buffer is full', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
                  null,
                  null,
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]], false),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(action.entries[0].output.bindingsStream.readable = false);
        expect(action.entries[1].output.bindingsStream.readable = false);
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        expect(action.entries[0].output.bindingsStream.readable = true);
        expect(action.entries[1].output.bindingsStream.readable = true);
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        expect(result.bindingsStream.read()).toEqual(BF.bindings([[ DF.variable('a'), DF.literal('1') ]]));
        action.entries[0].output.bindingsStream.close();
        action.entries[1].output.bindingsStream.close();
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        expect(action.entries[0].output.bindingsStream.ended).toBeTruthy();
        expect(action.entries[1].output.bindingsStream.ended).toBeTruthy();
        expect(result.bindingsStream.ended).toBeFalsy();
        expect(result.bindingsStream.read()).toEqual(BF.bindings([[ DF.variable('a'), DF.literal('1') ]]));
        expect(result.bindingsStream.read()).toEqual(null);
        expect(action.entries[0].output.bindingsStream.ended).toBeTruthy();
        expect(action.entries[1].output.bindingsStream.ended).toBeTruthy();
        expect(result.bindingsStream.ended).toBeTruthy();
      });

      it('should be not readable if both inputs are not readable', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
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
              output: <any> {
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
          ],
          context,
        };
        action.entries[0].output.bindingsStream.readable = true;
        action.entries[1].output.bindingsStream.readable = false;
        const { result } = await actor.getOutput(action);
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        expect(result.bindingsStream.readable).toBeTruthy();
      });

      it('should be readable if input 2 is readable', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
          ],
          context,
        };
        action.entries[0].output.bindingsStream.readable = false;
        action.entries[1].output.bindingsStream.readable = true;
        const { result } = await actor.getOutput(action);
        await new Promise<void>(resolve => setTimeout(resolve, 0));
        expect(result.bindingsStream.readable).toBeTruthy();
      });

      it('should handle errors from right iterator', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);
        let promise = new Promise<void>(resolve => result.bindingsStream.on("error", (e) => {
          expect(e.message).toEqual("Test error");
          resolve();
        }));
        action.entries[0].output.bindingsStream.destroy(new Error("Test error"));
        await promise;
      });

      it('should handle errors from left iterator', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);
        let promise = new Promise<void>(resolve => result.bindingsStream.on("error", (e) => {
          expect(e.message).toEqual("Test error");
          resolve();
        }));
        action.entries[1].output.bindingsStream.destroy(new Error("Test error"));
        await promise;
      });

      it('should handle entries without common variables', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
                  BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 3,
                  canContainUndefs: false,
                  variables: [ DF.variable('a') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('b'), DF.literal('1') ]]),
                  BF.bindings([[ DF.variable('b'), DF.literal('2') ]]),
                ], { autoStart: false }),
                metadata: () => Promise.resolve({
                  cardinality: 2,
                  canContainUndefs: false,
                  variables: [ DF.variable('b') ],
                }),
                type: 'bindings',
              },
              operation: <any> {},
            },
          ],
          context,
        };
        const { result } = await actor.getOutput(action);

        // Validate output
        expect(result.type).toEqual('bindings');
        expect(await result.metadata())
          .toEqual({ cardinality: 3, canContainUndefs: false, variables: [ DF.variable('a') ]});
        await expect(result.bindingsStream).toEqualBindingsStream([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
        ]);
      });



    });
  });
});
