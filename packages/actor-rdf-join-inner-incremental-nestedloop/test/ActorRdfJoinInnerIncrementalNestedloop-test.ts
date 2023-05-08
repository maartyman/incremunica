import { BindingsFactory } from '@comunica/incremental-bindings-factory';
import type { IActionRdfJoin } from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput } from '@comunica/bus-rdf-join-selectivity';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { ActionContext, Bus } from '@comunica/core';
import type { IQueryOperationResultBindings, Bindings, IActionContext } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import arrayifyStream from 'arrayify-stream';
import {ArrayIterator, WrappingIterator} from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { ActorRdfJoinInnerIncrementalNestedloop } from '../lib/ActorRdfJoinInnerIncrementalNestedloop';
import '@comunica/incremental-jest';
import {Duplex, Readable} from "readable-stream";
const streamifyArray = require('streamify-array');

const DF = new DataFactory();
const BF = new BindingsFactory();

function bindingsToString(b: Bindings): string {
  // eslint-disable-next-line @typescript-eslint/require-array-sort-compare
  const keys = [ ...b.keys() ].sort();
  return keys.map(k => `${k.value}:${b.get(k)!.value}`).toString();
}

describe('ActorRdfJoinNestedLoop', () => {
  let bus: any;
  let context: IActionContext;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    context = new ActionContext();
  });

  describe('The ActorRdfJoinNestedLoop module', () => {
    it('should be a function', () => {
      expect(ActorRdfJoinInnerIncrementalNestedloop).toBeInstanceOf(Function);
    });

    it('should be a ActorRdfJoinNestedLoop constructor', () => {
      expect(new (<any> ActorRdfJoinInnerIncrementalNestedloop)({ name: 'actor', bus })).toBeInstanceOf(ActorRdfJoinInnerIncrementalNestedloop);
      expect(new (<any> ActorRdfJoinInnerIncrementalNestedloop)({ name: 'actor', bus })).toBeInstanceOf(ActorRdfJoin);
    });

    it('should not be able to create new ActorRdfJoinNestedLoop objects without \'new\'', () => {
      expect(() => { (<any> ActorRdfJoinInnerIncrementalNestedloop)(); }).toThrow();
    });
  });

  describe('An ActorRdfJoinNestedLoop instance', () => {
    let mediatorJoinSelectivity: Mediator<
      Actor<IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>,
      IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>;
    let actor: ActorRdfJoinInnerIncrementalNestedloop;
    let action: IActionRdfJoin;
    let variables0: RDF.Variable[];
    let variables1: RDF.Variable[];

    beforeEach(() => {
      mediatorJoinSelectivity = <any> {
        mediate: async() => ({ selectivity: 1 }),
      };
      actor = new ActorRdfJoinInnerIncrementalNestedloop({ name: 'actor', bus, mediatorJoinSelectivity });
      variables0 = [];
      variables1 = [];
      action = {
        type: 'inner',
        entries: [
          {
            output: {
              bindingsStream: new ArrayIterator([], { autoStart: false }),
              metadata: async() => ({
                cardinality: { type: 'estimate', value: 4 },
                pageSize: 100,
                requestTime: 10,
                canContainUndefs: false,
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
                cardinality: { type: 'estimate', value: 5 },
                pageSize: 100,
                requestTime: 20,
                canContainUndefs: false,
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

    describe('should test', () => {
      afterEach(() => {
        action.entries.forEach(output => output.output?.bindingsStream?.destroy());
      });

      it('should only handle 2 streams', () => {
        action.entries.push(<any> {});
        return expect(actor.test(action)).rejects.toBeTruthy();
      });

      it('should handle undefs in left stream', () => {
        action.entries[0].output.metadata = async() => ({
          cardinality: { type: 'estimate', value: 4 },
          pageSize: 100,
          requestTime: 10,
          canContainUndefs: true,
          variables: [],
        });
        return expect(actor.test(action)).resolves
          .toEqual({
            iterations: 20,
            persistedItems: 0,
            blockingItems: 0,
            requestTime: 1.4,
          });
      });

      it('should handle undefs in right stream', () => {
        action.entries[1].output.metadata = async() => ({
          cardinality: { type: 'estimate', value: 5 },
          pageSize: 100,
          requestTime: 20,
          canContainUndefs: true,
          variables: [],
        });
        return expect(actor.test(action)).resolves
          .toEqual({
            iterations: 20,
            persistedItems: 0,
            blockingItems: 0,
            requestTime: 1.4,
          });
      });

      it('should handle undefs in left and right stream', () => {
        action.entries[0].output.metadata = async() => ({
          cardinality: { type: 'estimate', value: 4 },
          pageSize: 100,
          requestTime: 10,
          canContainUndefs: true,
          variables: [],
        });
        action.entries[1].output.metadata = async() => ({
          cardinality: { type: 'estimate', value: 5 },
          pageSize: 100,
          requestTime: 20,
          canContainUndefs: true,
          variables: [],
        });
        return expect(actor.test(action)).resolves
          .toEqual({
            iterations: 20,
            persistedItems: 0,
            blockingItems: 0,
            requestTime: 1.4,
          });
      });

      it('should generate correct test metadata', async() => {
        await expect(actor.test(action)).resolves.toHaveProperty('iterations',
          (await (<any> action.entries[0].output).metadata()).cardinality.value *
          (await (<any> action.entries[1].output).metadata()).cardinality.value);
      });
    });

    it('should generate correct metadata', async() => {
      await actor.run(action).then(async(result: IQueryOperationResultBindings) => {
        await expect((<any> result).metadata()).resolves.toHaveProperty('cardinality',
          { type: 'estimate',
            value: (await (<any> action.entries[0].output).metadata()).cardinality.value *
              (await (<any> action.entries[1].output).metadata()).cardinality.value });

        await expect(result.bindingsStream).toEqualBindingsStream([]);
      });
    });

    it('should return an empty stream for empty input', () => {
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        expect((await output.metadata()).variables).toEqual([]);
        await expect(output.bindingsStream).toEqualBindingsStream([]);
      });
    });

    it('should return null on read if join has ended', () => {
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        expect((await output.metadata()).variables).toEqual([]);
        await expect(output.bindingsStream).toEqualBindingsStream([]);
        expect(output.bindingsStream.ended).toBeTruthy();
        expect(output.bindingsStream.read()).toBeNull();
      });
    });

    it('should end after both streams are ended and no new elements can be generated', () => {
      // Clean up the old bindings
      action.entries.forEach(output => output.output?.bindingsStream?.destroy());

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('3') ],
        ]),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        output.bindingsStream.read()
        await new Promise<void>((resolve) => setTimeout(() => resolve(), 100));
        expect(action.entries[0].output.bindingsStream.ended).toBeTruthy();
        expect(action.entries[1].output.bindingsStream.ended).toBeTruthy();
        expect(output.bindingsStream.ended).toBeFalsy();
        await arrayifyStream(output.bindingsStream)
        await new Promise<void>((resolve) => setTimeout(() => resolve(), 100));
        expect(output.bindingsStream.ended).toBeTruthy();
      });
    });

    it('should join bindings with matching values', () => {
      // Clean up the old bindings
      action.entries.forEach(output => output.output?.bindingsStream?.destroy());

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('b'), DF.literal('b') ],
        ]),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('c'), DF.literal('c') ],
        ]),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
        await expect(output.bindingsStream).toEqualBindingsStream([
          BF.bindings([
            [ DF.variable('a'), DF.literal('a') ],
            [ DF.variable('b'), DF.literal('b') ],
            [ DF.variable('c'), DF.literal('c') ],
          ]),
        ]);
      });
    });

    it('should not join bindings with incompatible values', () => {
      // Clean up the old bindings
      action.entries.forEach(output => output.output?.bindingsStream?.destroy());

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('b'), DF.literal('b') ],
        ]),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('d') ],
          [ DF.variable('c'), DF.literal('c') ],
        ]),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
        await expect(output.bindingsStream).toEqualBindingsStream([]);
      });
    });

    it('should join multiple bindings', () => {
      // Clean up the old bindings
      action.entries.forEach(output => output.output?.bindingsStream?.destroy());

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('3') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('3') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('3') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('4') ],
        ]),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('5') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('0') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('0') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('c'), DF.literal('7') ],
        ]),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        const expected = [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('5') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('3') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('3') ],
            [ DF.variable('c'), DF.literal('5') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('6') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('3') ],
            [ DF.variable('c'), DF.literal('6') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('3') ],
            [ DF.variable('b'), DF.literal('3') ],
            [ DF.variable('c'), DF.literal('7') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('3') ],
            [ DF.variable('b'), DF.literal('4') ],
            [ DF.variable('c'), DF.literal('7') ],
          ]),
        ];
        expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
        // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
        // eslint-disable-next-line @typescript-eslint/require-array-sort-compare
        expect((await arrayifyStream(output.bindingsStream)).map(bindingsToString).sort())
          // eslint-disable-next-line @typescript-eslint/require-array-sort-compare
          .toEqual(expected.map(bindingsToString).sort());
      });
    });

    it('should join multiple bindings with undefs', () => {
      // Clean up the old bindings
      action.entries.forEach(output => output.output?.bindingsStream?.destroy());

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('3') ],
        ]),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]),
        BF.bindings([
          [ DF.variable('c'), DF.literal('5') ],
        ]),
      ]);
      action.entries[1].output.metadata = async() => ({
        cardinality: { type: 'estimate', value: 5 },
        pageSize: 100,
        requestTime: 20,
        canContainUndefs: true,
        variables: variables1,
      });
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        const expected = [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('5') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('3') ],
            [ DF.variable('c'), DF.literal('5') ],
          ]),
        ];
        expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
        // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
        // eslint-disable-next-line @typescript-eslint/require-array-sort-compare
        expect((await arrayifyStream(output.bindingsStream)).map(bindingsToString).sort())
          // eslint-disable-next-line @typescript-eslint/require-array-sort-compare
          .toEqual(expected.map(bindingsToString).sort());
      });
    });

    it('should join multiple bindings with negative bindings (left)', () => {
      // Clean up the old bindings
      action.entries.forEach(output => output.output?.bindingsStream?.destroy());

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ], false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ], false),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        const expected = [
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('6') ],
          ]),
        ];
        expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
        // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
        expect(await arrayifyStream(output.bindingsStream)).toBeIsomorphicBindingsArray(
          expected
        );
      });
    });

    it('should join multiple bindings with negative bindings (right)', () => {
      // Clean up the old bindings
      action.entries.forEach(output => output.output?.bindingsStream?.destroy());

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ], false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ], false),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        const expected = [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('6') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ], false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ], false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ], false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ], false),
        ];
        expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
        // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
        expect(await arrayifyStream(output.bindingsStream)).toBeIsomorphicBindingsArray(
          expected
        );
      });
    });

    it('should join multiple bindings with negative bindings that are not in the result set (left)', () => {
      // Clean up the old bindings
      action.entries.forEach(output => output.output?.bindingsStream?.destroy());

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('b'), DF.literal('b') ],
        ], false),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('c'), DF.literal('c') ],
        ]),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        const expected = [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('6') ],
          ]),
        ];
        expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
        // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
        expect(await arrayifyStream(output.bindingsStream)).toBeIsomorphicBindingsArray(
          expected
        );
      });
    });

    it('should join multiple bindings with negative bindings that are not in the result set (right)', () => {
      // Clean up the old bindings
      action.entries.forEach(output => output.output?.bindingsStream?.destroy());

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('b'), DF.literal('b') ],
        ]),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('c'), DF.literal('c') ],
        ], false),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        const expected = [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('6') ],
          ]),
        ];
        expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
        // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
        expect(await arrayifyStream(output.bindingsStream)).toBeIsomorphicBindingsArray(
          expected
        );
      });
    });



  });
});
