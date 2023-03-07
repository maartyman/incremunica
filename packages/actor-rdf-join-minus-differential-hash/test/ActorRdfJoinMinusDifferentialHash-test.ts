import {ActionContext, Actor, Bus, IActorTest, Mediator} from '@comunica/core';
import { ActorRdfJoinMinusDifferentialHash } from '../lib/ActorRdfJoinMinusDifferentialHash';
import {Bindings, IActionContext} from "@comunica/types";
import {ActorRdfJoin, IActionRdfJoin} from "@comunica/bus-rdf-join";
import {IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput} from "@comunica/bus-rdf-join-selectivity";
import {ArrayIterator} from "asynciterator";
import {DataFactory} from "rdf-data-factory";
import {BindingsFactory} from "@comunica/bindings-factory";
import '@comunica/jest';

const DF = new DataFactory();
const BF = new BindingsFactory();

describe('ActorRdfJoinMinusDifferentialHash', () => {
  let bus: any;
  let context: IActionContext;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    context = new ActionContext();
  });

  describe('The ActorRdfJoinMinusDifferentialHash module', () => {
    it('should be a function', () => {
      expect(ActorRdfJoinMinusDifferentialHash).toBeInstanceOf(Function);
    });

    it('should be a ActorRdfJoinMinusDifferentialHash constructor', () => {
      expect(new (<any> ActorRdfJoinMinusDifferentialHash)({ name: 'actor', bus })).toBeInstanceOf(ActorRdfJoinMinusDifferentialHash);
      expect(new (<any> ActorRdfJoinMinusDifferentialHash)({ name: 'actor', bus })).toBeInstanceOf(ActorRdfJoin);
    });

    it('should not be able to create new ActorRdfJoinMinusDifferentialHash objects without \'new\'', () => {
      expect(() => { (<any> ActorRdfJoinMinusDifferentialHash)(); }).toThrow();
    });
  });

  describe('An ActorRdfJoinMinusDifferentialHash instance', () => {
    let mediatorJoinSelectivity: Mediator<
      Actor<IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>,
      IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>;
    let actor: ActorRdfJoinMinusDifferentialHash;

    beforeEach(() => {
      mediatorJoinSelectivity = <any> {
        mediate: async() => ({ selectivity: 1 }),
      };
      actor = new ActorRdfJoinMinusDifferentialHash({ name: 'actor', bus, mediatorJoinSelectivity });
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

      it('should not test on two entries with undefs', async() => {
        await expect(actor.test({
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
        })).rejects.toThrowError('Actor actor can not join streams containing undefs');
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
          iterations: 8,
          blockingItems: 4,
          persistedItems: 4,
          requestTime: 0.8,
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
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]], true),
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]], true),
                  BF.bindings([[ DF.variable('a'), DF.literal('3') ]], true),
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
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]], true),
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]], true),
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
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]], true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]], true),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]], true),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]], false),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]], false),
        ]);
      });

      it('should handle entries with common variables and with left deletion', async() => {
        let arrayBindingStream = new ArrayIterator([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]], true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]], true),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]], false),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]], false),
        ], { autoStart: false });

        let leftBindingStream = arrayBindingStream.transform<Bindings>({transform: (bindings, done, push) => {
          setTimeout(() => {
            push(bindings);
            done();
          },10);
        }});

        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: leftBindingStream,
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
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]], true),
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
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]], true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]], false),
        ]);
      });

      it('should handle entries with common variables and with right deletion', async() => {
        let arrayBindingStream = new ArrayIterator([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]], true),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]], false),
        ], { autoStart: false });

        let rightBindingStream = arrayBindingStream.transform<Bindings>({transform: (bindings, done, push) => {
            setTimeout(() => {
              push(bindings);
              done();
            },10);
          }});

        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]], true),
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]], true),
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
                bindingsStream: rightBindingStream,
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
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]], true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]], true),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]], false),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]], true),
        ]);
      });

      it('should handle entries without common variables', async() => {
        const action: IActionRdfJoin = {
          type: 'minus',
          entries: [
            {
              output: <any> {
                bindingsStream: new ArrayIterator([
                  BF.bindings([[ DF.variable('a'), DF.literal('1') ]], true),
                  BF.bindings([[ DF.variable('a'), DF.literal('2') ]], true),
                  BF.bindings([[ DF.variable('a'), DF.literal('3') ]], true),
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
                  BF.bindings([[ DF.variable('b'), DF.literal('1') ]], true),
                  BF.bindings([[ DF.variable('b'), DF.literal('2') ]], true),
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
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]], true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]], true),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]], true),
        ]);
      });
    });
  });
});
