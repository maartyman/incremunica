import { BindingsFactory } from '@comunica/bindings-factory';
import type { IActionRdfJoin } from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput } from '@comunica/bus-rdf-join-selectivity';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { ActionContext, Bus } from '@comunica/core';
import type { IQueryOperationResultBindings, Bindings, IActionContext } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import arrayifyStream from 'arrayify-stream';
import {ArrayIterator} from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { ActorRdfJoinInnerIncrementalPartialHash } from '../lib/ActorRdfJoinInnerIncrementalPartialHash';
import '@incremunica/incremental-jest';
import { MetadataValidationState } from '@comunica/metadata';
import {DevTools} from "@incremunica/dev-tools";
import {ActionContextKeyIsAddition} from "@incremunica/actor-merge-bindings-context-is-addition";

const DF = new DataFactory();

describe('ActorRdfJoinPartialHash', () => {
  let bus: any;
  let context: IActionContext;
  let BF: BindingsFactory;

  beforeEach(async () => {
    bus = new Bus({name: 'bus'});
    context = new ActionContext();
    BF = await DevTools.createBindingsFactory(DF);
  });

  describe('The ActorRdfJoinPartialHash module', () => {
    it('should be a function', () => {
      expect(ActorRdfJoinInnerIncrementalPartialHash).toBeInstanceOf(Function);
    });

    it('should be a ActorRdfJoinPartialHash constructor', () => {
      expect(new (<any> ActorRdfJoinInnerIncrementalPartialHash)({ name: 'actor', bus })).toBeInstanceOf(ActorRdfJoinInnerIncrementalPartialHash);
      expect(new (<any> ActorRdfJoinInnerIncrementalPartialHash)({ name: 'actor', bus })).toBeInstanceOf(ActorRdfJoin);
    });

    it('should not be able to create new ActorRdfJoinPartialHash objects without \'new\'', () => {
      expect(() => { (<any> ActorRdfJoinInnerIncrementalPartialHash)(); }).toThrow();
    });
  });

  describe('An ActorRdfJoinPartialHash instance', () => {
    let mediatorJoinSelectivity: Mediator<
      Actor<IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>,
      IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>;
    let actor: ActorRdfJoinInnerIncrementalPartialHash;
    let action: IActionRdfJoin;
    let variables0: RDF.Variable[];
    let variables1: RDF.Variable[];

    beforeEach(() => {
      mediatorJoinSelectivity = <any> {
        mediate: async() => ({ selectivity: 1 }),
      };
      actor = new ActorRdfJoinInnerIncrementalPartialHash({ name: 'actor', bus, mediatorJoinSelectivity });
      variables0 = [];
      variables1 = [];
      action = {
        type: 'inner',
        entries: [
          {
            output: {
              bindingsStream: new ArrayIterator([], { autoStart: false }),
              metadata: async() => ({
                state: new MetadataValidationState(),
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
                state: new MetadataValidationState(),
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

      it('should fail on undefs in left stream', () => {
        action.entries[0].output.metadata = () => Promise.resolve({
          state: new MetadataValidationState(),
          cardinality: { type: 'estimate', value: 4 },
          canContainUndefs: true,
          variables: [],
        });
        return expect(actor.test(action)).rejects
          .toThrow(new Error('Actor actor can not join streams containing undefs'));
      });

      it('should fail on undefs in right stream', () => {
        action.entries[1].output.metadata = () => Promise.resolve({
          state: new MetadataValidationState(),
          cardinality: { type: 'estimate', value: 4 },
          canContainUndefs: true,
          variables: [],
        });
        return expect(actor.test(action)).rejects
          .toThrow(new Error('Actor actor can not join streams containing undefs'));
      });

      it('should fail on undefs in left and right stream', () => {
        action.entries[0].output.metadata = () => Promise.resolve({
          state: new MetadataValidationState(),
          cardinality: { type: 'estimate', value: 4 },
          canContainUndefs: true,
          variables: [],
        });
        action.entries[1].output.metadata = () => Promise.resolve({
          state: new MetadataValidationState(),
          cardinality: { type: 'estimate', value: 4 },
          canContainUndefs: true,
          variables: [],
        });
        return expect(actor.test(action)).rejects
          .toThrow(new Error('Actor actor can not join streams containing undefs'));
      });

      it('should generate correct test metadata', async() => {
        await expect(actor.test(action)).resolves.toHaveProperty('iterations', 0);
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
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('3') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('c'), DF.literal('c') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
        await expect(output.bindingsStream).toEqualBindingsStream([
          BF.bindings([
            [ DF.variable('a'), DF.literal('a') ],
            [ DF.variable('b'), DF.literal('b') ],
            [ DF.variable('c'), DF.literal('c') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('d') ],
          [ DF.variable('c'), DF.literal('c') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('3') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('3') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('3') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('5') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('0') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('0') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('c'), DF.literal('7') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        const expected = [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('5') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('3') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('3') ],
            [ DF.variable('c'), DF.literal('5') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('6') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('3') ],
            [ DF.variable('c'), DF.literal('6') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('3') ],
            [ DF.variable('b'), DF.literal('3') ],
            [ DF.variable('c'), DF.literal('7') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('3') ],
            [ DF.variable('b'), DF.literal('4') ],
            [ DF.variable('c'), DF.literal('7') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ];
        expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
        // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
        // eslint-disable-next-line @typescript-eslint/require-array-sort-compare
        expect((await arrayifyStream(output.bindingsStream))).toBeIsomorphicBindingsArray(
          expected
        );
      });
    });

    it('should join multiple bindings with negative bindings (left)', () => {
      // Clean up the old bindings
      action.entries.forEach(output => output.output?.bindingsStream?.destroy());

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), false),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        const expected = [
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('6') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), false),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        const expected = [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('6') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), false),
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
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('b') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), false),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('c'), DF.literal('c') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        const expected = [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('6') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
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
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('b') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('c'), DF.literal('c') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), false),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        const expected = [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('b') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('6') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ];
        expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
        // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
        expect(await arrayifyStream(output.bindingsStream)).toBeIsomorphicBindingsArray(
          expected
        );
      });
    });

    it('should be symmetric', () => {
      // Clean up the old bindings
      action.entries.forEach(output => output.output?.bindingsStream?.destroy());

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]).transform({
        transform: (item: Bindings, done: () => void, push: (i: RDF.Bindings) => void) => {
          push(item);
          setTimeout(() => {
            push(item);
            done();
          }, 100)
        }
      });
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      return actor.run(action).then(async(output: IQueryOperationResultBindings) => {
        const expected = [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('2') ],
            [ DF.variable('c'), DF.literal('4') ],
          ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ];
        // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
        // eslint-disable-next-line @typescript-eslint/require-array-sort-compare
        expect((await arrayifyStream(output.bindingsStream))).toBeIsomorphicBindingsArray(
          expected
        );
      });
    });

  });
});
