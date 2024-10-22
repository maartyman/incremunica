import type { IActionRdfJoin } from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput } from '@comunica/bus-rdf-join-selectivity';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { ActionContext, Bus } from '@comunica/core';
import { MetadataValidationState } from '@comunica/metadata';
import type { IQueryOperationResultBindings, Bindings, IActionContext } from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import { DevTools } from '@incremunica/dev-tools';
import type * as RDF from '@rdfjs/types';
import arrayifyStream from 'arrayify-stream';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { ActorRdfJoinInnerIncrementalFullHash } from '../lib';
import '@incremunica/incremental-jest';

const DF = new DataFactory();

describe('ActorRdfJoinFullHash', () => {
  let bus: any;
  let context: IActionContext;
  let BF: BindingsFactory;

  beforeEach(async() => {
    bus = new Bus({ name: 'bus' });
    context = new ActionContext();
    BF = await DevTools.createBindingsFactory(DF);
  });

  describe('The ActorRdfJoinFullHash module', () => {
    it('should be a function', () => {
      expect(ActorRdfJoinInnerIncrementalFullHash).toBeInstanceOf(Function);
    });

    it('should be a ActorRdfJoinFullHash constructor', () => {
      expect(new (<any> ActorRdfJoinInnerIncrementalFullHash)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorRdfJoinInnerIncrementalFullHash);
      expect(new (<any> ActorRdfJoinInnerIncrementalFullHash)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorRdfJoin);
    });

    it('should not be able to create new ActorRdfJoinFullHash objects without \'new\'', () => {
      expect(() => {
        (<any> ActorRdfJoinInnerIncrementalFullHash)();
      }).toThrow('');
    });
  });

  describe('An ActorRdfJoinFullHash instance', () => {
    let mediatorJoinSelectivity: Mediator<
      Actor<IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>,
      IActionRdfJoinSelectivity,
IActorTest,
IActorRdfJoinSelectivityOutput
>;
    let actor: ActorRdfJoinInnerIncrementalFullHash;
    let action: IActionRdfJoin;
    let variables0: RDF.Variable[];
    let variables1: RDF.Variable[];

    beforeEach(() => {
      mediatorJoinSelectivity = <any> {
        mediate: async() => ({ selectivity: 1 }),
      };
      actor = new ActorRdfJoinInnerIncrementalFullHash({ name: 'actor', bus, mediatorJoinSelectivity });
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
        for (const output of action.entries) {
          output.output?.bindingsStream?.destroy();
        }
      });

      it('should only handle 2 streams', async() => {
        action.entries.push(<any>{});
        await expect(actor.test(action)).rejects.toBeTruthy();
      });

      it('should fail on undefs in left stream', async() => {
        action.entries[0].output.metadata = () => Promise.resolve({
          state: new MetadataValidationState(),
          cardinality: { type: 'estimate', value: 4 },
          canContainUndefs: true,
          variables: [],
        });
        await expect(actor.test(action)).rejects
          .toThrow(new Error('Actor actor can not join streams containing undefs'));
      });

      it('should fail on undefs in right stream', async() => {
        action.entries[1].output.metadata = () => Promise.resolve({
          state: new MetadataValidationState(),
          cardinality: { type: 'estimate', value: 4 },
          canContainUndefs: true,
          variables: [],
        });
        await expect(actor.test(action)).rejects
          .toThrow(new Error('Actor actor can not join streams containing undefs'));
      });

      it('should fail on undefs in left and right stream', async() => {
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
        await expect(actor.test(action)).rejects
          .toThrow(new Error('Actor actor can not join streams containing undefs'));
      });

      it('should generate correct test metadata', async() => {
        await expect(actor.test(action)).resolves.toHaveProperty('iterations', 0);
      });
    });

    it('should generate correct metadata', async() => {
      await actor.run(action).then(async(result: IQueryOperationResultBindings) => {
        await expect((<any> result).metadata()).resolves
          .toHaveProperty(
            'cardinality',
            {
              type: 'estimate',
              value: (await (<any> action.entries[0].output).metadata()).cardinality.value *
              (await (<any> action.entries[1].output).metadata()).cardinality.value,
            },
          );

        await expect(result.bindingsStream).toEqualBindingsStream([]);
      });
    });

    it('should return an empty stream for empty input', async() => {
      const output = await actor.run(action);
      expect((await output.metadata()).variables).toEqual([]);
      await expect(output.bindingsStream).toEqualBindingsStream([]);
    });

    it('should return null on read if join has ended', async() => {
      const output = await actor.run(action);
      expect((await output.metadata()).variables).toEqual([]);
      await expect(output.bindingsStream).toEqualBindingsStream([]);
      expect(output.bindingsStream.ended).toBeTruthy();
      expect(output.bindingsStream.read()).toBeNull();
    });

    it('should end after both streams are ended and no new elements can be generated', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('4') ],
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
      const output = await actor.run(action);
      output.bindingsStream.read();
      await new Promise<void>(resolve => setTimeout(() => resolve(), 100));
      expect(action.entries[0].output.bindingsStream.ended).toBeTruthy();
      expect(action.entries[1].output.bindingsStream.ended).toBeTruthy();
      expect(output.bindingsStream.ended).toBeFalsy();
      await arrayifyStream(output.bindingsStream);
      await new Promise<void>(resolve => setTimeout(() => resolve(), 100));
      expect(output.bindingsStream.ended).toBeTruthy();
    });

    it('should join bindings with matching values', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

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
      const output = await actor.run(action);
      expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('b'), DF.literal('b') ],
          [ DF.variable('c'), DF.literal('c') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
    });

    it('should not join bindings with incompatible values', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

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
      const output = await actor.run(action);
      expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
      await expect(output.bindingsStream).toEqualBindingsStream([]);
    });

    it('should join multiple bindings', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

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
      const output = await actor.run(action);
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
      await expect((arrayifyStream(output.bindingsStream))).resolves.toBeIsomorphicBindingsArray(
        expected,
      );
    });

    it('should join multiple bindings with negative bindings (left)', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

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
      const output = await actor.run(action);
      const expected = [
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ];
      expect((await output.metadata()).variables).toEqual([ DF.variable('a'), DF.variable('b'), DF.variable('c') ]);
      // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
      await expect(arrayifyStream(output.bindingsStream)).resolves.toBeIsomorphicBindingsArray(
        expected,
      );
    });

    it('should join multiple bindings with negative bindings (right)', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

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
      const output = await actor.run(action);
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
      await expect(arrayifyStream(output.bindingsStream)).resolves.toBeIsomorphicBindingsArray(
        expected,
      );
    });

    it('should join multiple bindings with negative bindings that are not in the result set (left)', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

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
      const output = await actor.run(action);
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
      await expect(arrayifyStream(output.bindingsStream)).resolves.toBeIsomorphicBindingsArray(
        expected,
      );
    });

    it('should join multiple bindings with negative bindings that are not in the result set (right)', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

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
      const output = await actor.run(action);
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
      await expect(arrayifyStream(output.bindingsStream)).resolves.toBeIsomorphicBindingsArray(
        expected,
      );
    });

    it('should be symmetric', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

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
          }, 100);
        },
      });
      variables0 = [ DF.variable('a'), DF.variable('b') ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      variables1 = [ DF.variable('a'), DF.variable('c') ];
      const output = await actor.run(action);
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
      await expect((arrayifyStream(output.bindingsStream))).resolves.toBeIsomorphicBindingsArray(
        expected,
      );
    });
  });
});
