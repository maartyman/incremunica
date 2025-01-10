import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import '@comunica/utils-jest';
import type { IActionRdfJoin } from '@comunica/bus-rdf-join';
import { ActorRdfJoin } from '@comunica/bus-rdf-join';
import type { IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput } from '@comunica/bus-rdf-join-selectivity';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { Bus } from '@comunica/core';
import type { IQueryOperationResultBindings, Bindings, IActionContext } from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { MetadataValidationState } from '@comunica/utils-metadata';
import { KeysBindings } from '@incremunica/context-entries';
import {
  createTestBindingsFactory,
  createTestContextWithDataFactory,
  createTestMediatorHashBindings,
} from '@incremunica/dev-tools';
import type * as RDF from '@rdfjs/types';
import { arrayifyStream } from 'arrayify-stream';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import '@incremunica/incremental-jest';
import { ActorRdfJoinIncrementalOptionalHash } from '../lib/ActorRdfJoinIncrementalOptionalHash';

const DF = new DataFactory();

describe('ActorRdfJoinIncrementalOptionalHash', () => {
  let bus: any;
  let context: IActionContext;
  let BF: BindingsFactory;

  beforeEach(async() => {
    bus = new Bus({ name: 'bus' });
    context = createTestContextWithDataFactory(DF);
    BF = await createTestBindingsFactory(DF);
  });

  describe('The ActorRdfJoinIncrementalOptionalHash module', () => {
    it('should be a function', () => {
      expect(ActorRdfJoinIncrementalOptionalHash).toBeInstanceOf(Function);
    });

    it('should be a ActorRdfJoinIncrementalOptionalHash constructor', () => {
      expect(new (<any> ActorRdfJoinIncrementalOptionalHash)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorRdfJoinIncrementalOptionalHash);
      expect(new (<any> ActorRdfJoinIncrementalOptionalHash)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorRdfJoin);
    });

    it('should not be able to create new ActorRdfJoinIncrementalOptionalHash objects without \'new\'', () => {
      expect(() => {
        (<any> ActorRdfJoinIncrementalOptionalHash)();
      }).toThrow('Class constructor ActorRdfJoinIncrementalOptionalHash cannot be invoked without \'new\'');
    });
  });

  describe('An ActorRdfJoinIncrementalOptionalHash instance', () => {
    let mediatorJoinSelectivity: Mediator<
      Actor<IActionRdfJoinSelectivity, IActorTest, IActorRdfJoinSelectivityOutput>,
      IActionRdfJoinSelectivity,
IActorTest,
IActorRdfJoinSelectivityOutput
>;
    let actor: ActorRdfJoinIncrementalOptionalHash;
    let action: IActionRdfJoin;
    let variables0: { variable: RDF.Variable; canBeUndef: boolean }[];
    let variables1: { variable: RDF.Variable; canBeUndef: boolean }[];
    let mediatorHashBindings: MediatorHashBindings;

    beforeEach(() => {
      mediatorJoinSelectivity = <any> {
        mediate: async() => ({ selectivity: 1 }),
      };
      mediatorHashBindings = createTestMediatorHashBindings();
      actor = new ActorRdfJoinIncrementalOptionalHash({
        name: 'actor',
        bus,
        mediatorJoinSelectivity,
        mediatorHashBindings,
      });
      variables0 = [];
      variables1 = [];
      action = {
        type: 'optional',
        entries: [
          {
            output: {
              bindingsStream: new ArrayIterator([], { autoStart: false }),
              metadata: async() => ({
                state: new MetadataValidationState(),
                cardinality: { type: 'estimate', value: 4 },
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
                cardinality: { type: 'estimate', value: 5 },
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

    describe('should test', () => {
      afterEach(() => {
        for (const output of action.entries) {
          output.output?.bindingsStream?.destroy();
        }
      });

      it('should only handle 2 streams', async() => {
        action.entries.push(<any>{});
        await expect(actor.test(action))
          .resolves.toFailTest('actor requires 2 join entries at most. The input contained 3.');
      });

      it('should fail on undefs on overlapping vars in left stream', async() => {
        variables0 = [{ variable: DF.variable('a'), canBeUndef: true }];
        variables1 = [{ variable: DF.variable('a'), canBeUndef: false }];
        await expect(actor.test(action)).resolves.toFailTest('Actor actor can not join streams containing undefs');
      });

      it('should fail on undefs on overlapping vars in right stream', async() => {
        variables0 = [{ variable: DF.variable('a'), canBeUndef: false }];
        variables1 = [{ variable: DF.variable('a'), canBeUndef: true }];
        await expect(actor.test(action)).resolves.toFailTest('Actor actor can not join streams containing undefs');
      });

      it('should fail on undefs in left and right stream', async() => {
        variables0 = [{ variable: DF.variable('a'), canBeUndef: true }];
        variables1 = [{ variable: DF.variable('a'), canBeUndef: true }];
        await expect(actor.test(action)).resolves.toFailTest('Actor actor can not join streams containing undefs');
      });

      it('should generate correct test metadata', async() => {
        await expect(actor.test(action)).resolves.toEqual({
          sideData: {
            metadatas: [
              {
                cardinality: {
                  type: 'estimate',
                  value: 4,
                },
                pageSize: 100,
                requestTime: 10,
                state: {
                  invalidateListeners: [],
                  valid: true,
                },
                variables: [],
              },
              {
                cardinality: {
                  type: 'estimate',
                  value: 5,
                },
                pageSize: 100,
                requestTime: 20,
                state: {
                  invalidateListeners: [],
                  valid: true,
                },
                variables: [],
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
    });

    it('should generate correct metadata', async() => {
      await actor.run(action, undefined).then(async(result: IQueryOperationResultBindings) => {
        await expect((<any> result).metadata()).resolves.toHaveProperty(
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
      const output = await actor.run(action, undefined);
      expect((await output.metadata()).variables).toEqual([]);
      await expect(output.bindingsStream).toEqualBindingsStream([]);
    });

    it('should return null on read if join has ended', async() => {
      const output = await actor.run(action, undefined);
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
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables0 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
      ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('3') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables1 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ];
      const output = await actor.run(action, undefined);
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
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables0 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
      ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('c'), DF.literal('c') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables1 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ];
      const output = await actor.run(action, undefined);
      expect((await output.metadata()).variables).toEqual([
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ]);
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('b'), DF.literal('b') ],
          [ DF.variable('c'), DF.literal('c') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
    });

    it('should return bindings from input 1 if incompatible values', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('b'), DF.literal('b') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables0 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
      ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('d') ],
          [ DF.variable('c'), DF.literal('c') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables1 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ];
      const output = await actor.run(action, undefined);
      expect((await output.metadata()).variables).toEqual([
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ]);
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('b'), DF.literal('b') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
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
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('3') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('3') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('3') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables0 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
      ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('5') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('0') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('0') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('c'), DF.literal('7') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables1 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ];
      const output = await actor.run(action, undefined);
      const expected = [
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('5') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('3') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('3') ],
          [ DF.variable('c'), DF.literal('5') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('3') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('3') ],
          [ DF.variable('c'), DF.literal('7') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('4') ],
          [ DF.variable('c'), DF.literal('7') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ];
      expect((await output.metadata()).variables).toEqual([
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ]);
      // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
      await expect((arrayifyStream(output.bindingsStream))).resolves.toBeIsomorphicBindingsArray(
        expected,
      );
    });

    it('should join multiple bindings with negative bindings (left) right first', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
      ]);
      variables0 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
      ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables1 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ];
      const output = await actor.run(action, undefined);
      const expected = [
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
      ];
      expect((await output.metadata()).variables).toEqual([
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ]);
      // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
      await expect(arrayifyStream(output.bindingsStream)).resolves.toBeIsomorphicBindingsArray(
        expected,
      );
    });

    it('should join multiple bindings with negative bindings (left) left first', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
      ]);
      variables0 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
      ];
      action.entries[1].output.bindingsStream = <any> new ArrayIterator([
        null,
        null,
        null,
        null,
        null,
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables1 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ];
      const output = await actor.run(action, undefined);
      const expected = [
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('3') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ];
      expect((await output.metadata()).variables).toEqual([
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ]);
      // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
      await expect(arrayifyStream(output.bindingsStream)).resolves.toBeIsomorphicBindingsArray(
        expected,
      );
    });

    it('should join multiple bindings with negative bindings (right) right first', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables0 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
      ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
      ]);
      variables1 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ];
      const output = await actor.run(action, undefined);
      const expected = [
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ];
      expect((await output.metadata()).variables).toEqual([
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ]);
      // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
      await expect(arrayifyStream(output.bindingsStream)).resolves.toBeIsomorphicBindingsArray(
        expected,
      );
    });

    it('should join multiple bindings with negative bindings (right) left first', async() => {
      // Clean up the old bindings
      for (const output of action.entries) {
        output.output?.bindingsStream?.destroy();
      }

      action.entries[0].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables0 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
      ];
      action.entries[1].output.bindingsStream = <any> new ArrayIterator([
        null,
        null,
        null,
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
      ]);
      variables1 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ];
      const output = await actor.run(action, undefined);
      const expected = [
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ];
      expect((await output.metadata()).variables).toEqual([
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ]);
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
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('b') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
      ]);
      variables0 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
      ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('c'), DF.literal('c') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables1 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ];
      const output = await actor.run(action, undefined);
      const expected = [
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ];
      expect((await output.metadata()).variables).toEqual([
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ]);
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
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('b') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables0 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
      ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('a') ],
          [ DF.variable('c'), DF.literal('c') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
      ]);
      variables1 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ];
      const output = await actor.run(action, undefined);
      const expected = [
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('b') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('6') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ];
      expect((await output.metadata()).variables).toEqual([
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ]);
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
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]).transform({
        transform: (item: Bindings, done: () => void, push: (i: RDF.Bindings) => void) => {
          push(item);
          setTimeout(() => {
            push(item);
            done();
          }, 100);
        },
      });
      variables0 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('b'), canBeUndef: false },
      ];
      action.entries[1].output.bindingsStream = new ArrayIterator([
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);
      variables1 = [
        { variable: DF.variable('a'), canBeUndef: false },
        { variable: DF.variable('c'), canBeUndef: false },
      ];
      const output = await actor.run(action, undefined);
      const expected = [
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('a'), DF.literal('1') ],
          [ DF.variable('b'), DF.literal('2') ],
          [ DF.variable('c'), DF.literal('4') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ];
      // Mapping to string and sorting since we don't know order (well, we sort of know, but we might not!)
      await expect((arrayifyStream(output.bindingsStream))).resolves.toBeIsomorphicBindingsArray(
        expected,
      );
    });
  });
});
