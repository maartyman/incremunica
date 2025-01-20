import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import type { MediatorHashQuads } from '@comunica/bus-hash-quads';
import { ActionContext, Bus } from '@comunica/core';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { getSafeBindings, getSafeQuads } from '@comunica/utils-query-operation';
import { KeysBindings } from '@incremunica/context-entries';
import { createTestMediatorHashBindings, createTestBindingsFactory, quad } from '@incremunica/dev-tools';
import { arrayifyStream } from 'arrayify-stream';
import { ArrayIterator } from 'asynciterator';
import type { Quad } from 'rdf-data-factory';
import { DataFactory } from 'rdf-data-factory';
import { ActorQueryOperationDistinctHash } from '../lib';
import '@comunica/utils-jest';

const DF = new DataFactory();

describe('ActorQueryOperationDistinctHash', () => {
  let bus: any;
  let mediatorQueryOperation: any;
  let BF: BindingsFactory;

  beforeEach(async() => {
    BF = await createTestBindingsFactory(DF);
    bus = new Bus({ name: 'bus' });
    mediatorQueryOperation = {
      mediate: (arg: any) => Promise.resolve({
        bindingsStream: new ArrayIterator([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, false),
        ]),
        metadata: () => Promise.resolve({
          cardinality: 5,
          variables: [
            {
              variable: DF.variable('a'),
              isUndef: false,
            },
          ],
        }),
        operated: arg,
        type: 'bindings',
      }),
    };
  });

  describe('newHashFilterQuads', () => {
    let actor: ActorQueryOperationDistinctHash;
    let mediatorHashBindings: MediatorHashBindings;
    let mediatorHashQuads: MediatorHashQuads;

    beforeEach(() => {
      mediatorHashBindings = createTestMediatorHashBindings();
      mediatorHashQuads = <any> {
        mediate: () => {
          return {
            hashFunction: (quad: Quad) => {
              return JSON.stringify(quad.subject) + JSON.stringify(quad.predicate) + JSON.stringify(quad.object.value);
            },
          };
        },
      };
      actor = new ActorQueryOperationDistinctHash(
        { name: 'actor', bus, mediatorQueryOperation, mediatorHashBindings, mediatorHashQuads },
      );
    });

    it('should create a filter', async() => {
      await expect(actor.newHashFilterQuads(<any>{})).resolves.toBeInstanceOf(Function);
    });

    it('should create a filter that is a predicate', async() => {
      const filter = await actor.newHashFilterQuads(<any>{});
      expect(filter(quad('s', 'p', 'o', undefined, true))).toBe(true);
    });

    it('should create a filter that only returns true once for equal objects', async() => {
      const filter = await actor.newHashFilterQuads(<any>{});
      expect(filter(quad('a', 'p', 'a', undefined, true))).toBe(true);
      expect(filter(quad('a', 'p', 'a', undefined, true))).toBe(false);
      expect(filter(quad('a', 'p', 'a', undefined, true))).toBe(false);
      expect(filter(quad('a', 'p', 'a', undefined, true))).toBe(false);

      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(true);
      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(false);
    });

    it('should create a filters that are independent', async() => {
      const filter1 = await actor.newHashFilterQuads(<any>{});
      const filter2 = await actor.newHashFilterQuads(<any>{});
      const filter3 = await actor.newHashFilterQuads(<any>{});
      expect(filter1(quad('a', 'p', 'b', undefined, true))).toBe(true);
      expect(filter1(quad('a', 'p', 'b', undefined, true))).toBe(false);

      expect(filter2(quad('a', 'p', 'b', undefined, true))).toBe(true);
      expect(filter2(quad('a', 'p', 'b', undefined, true))).toBe(false);

      expect(filter3(quad('a', 'p', 'b', undefined, true))).toBe(true);
      expect(filter3(quad('a', 'p', 'b', undefined, true))).toBe(false);
    });

    it('should create a filter that returns true if everything is deleted', async() => {
      const filter = await actor.newHashFilterQuads(<any>{});
      expect(filter(quad('a', 'p', 'a', undefined, true))).toBe(true);
      expect(filter(quad('a', 'p', 'a', undefined, true))).toBe(false);
      expect(filter(quad('a', 'p', 'a', undefined, false))).toBe(false);
      expect(filter(quad('a', 'p', 'a', undefined, false))).toBe(true);
      expect(filter(quad('a', 'p', 'a', undefined, true))).toBe(true);
      expect(filter(quad('a', 'p', 'a', undefined, true))).toBe(false);

      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(true);
      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, false))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, false))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, false))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, false))).toBe(true);
      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(true);
    });

    it('should create a filter that returns false if too much is deleted', async() => {
      const filter = await actor.newHashFilterQuads(<any>{});
      expect(filter(quad('a', 'p', 'a', undefined, true))).toBe(true);
      expect(filter(quad('a', 'p', 'a', undefined, true))).toBe(false);
      expect(filter(quad('a', 'p', 'a', undefined, false))).toBe(false);
      expect(filter(quad('a', 'p', 'a', undefined, false))).toBe(true);
      expect(filter(quad('a', 'p', 'a', undefined, false))).toBe(false);
      expect(filter(quad('a', 'p', 'a', undefined, true))).toBe(true);
      expect(filter(quad('a', 'p', 'a', undefined, true))).toBe(false);

      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(true);
      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, false))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, false))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, false))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, false))).toBe(true);
      expect(filter(quad('a', 'p', 'a', undefined, false))).toBe(false);
      expect(filter(quad('a', 'p', 'b', undefined, true))).toBe(true);
    });
  });

  describe('newHashFilterBindings', () => {
    let actor: ActorQueryOperationDistinctHash;
    let mediatorHashBindings: MediatorHashBindings;
    let mediatorHashQuads: MediatorHashQuads;

    beforeEach(() => {
      mediatorHashBindings = createTestMediatorHashBindings();
      mediatorHashQuads = <any> {
        mediate: () => {
          return {
            hashFunction: (quad: Quad) => {
              return JSON.stringify(quad.subject) + JSON.stringify(quad.predicate) + JSON.stringify(quad.object.value);
            },
          };
        },
      };
      actor = new ActorQueryOperationDistinctHash(
        { name: 'actor', bus, mediatorQueryOperation, mediatorHashBindings, mediatorHashQuads },
      );
    });

    it('should create a filter', async() => {
      await expect(actor.newHashFilterBindings(<any>{}, [])).resolves.toBeInstanceOf(Function);
    });

    it('should create a filter that is a predicate', async() => {
      const filter = await actor.newHashFilterBindings(<any>{}, []);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
    });

    it('should create a filter that only returns true once for equal objects', async() => {
      const filter = await actor.newHashFilterBindings(<any>{}, [ DF.variable('a') ]);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);

      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
    });

    it('should create a filters that are independent', async() => {
      const filter1 = await actor.newHashFilterBindings(<any>{}, []);
      const filter2 = await actor.newHashFilterBindings(<any>{}, []);
      const filter3 = await actor.newHashFilterBindings(<any>{}, []);
      expect(filter1(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
      expect(filter1(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);

      expect(filter2(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
      expect(filter2(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);

      expect(filter3(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
      expect(filter3(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
    });

    it('should create a filter that returns true if everything is deleted', async() => {
      const filter = await actor.newHashFilterBindings(<any>{}, [ DF.variable('a') ]);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);

      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
    });

    it('should create a filter that returns false if too much is deleted', async() => {
      const filter = await actor.newHashFilterBindings(<any>{}, [ DF.variable('a') ]);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);

      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
    });
  });

  describe('An ActorQueryOperationDistinctHash instance', () => {
    let actor: ActorQueryOperationDistinctHash;
    let mediatorHashBindings: MediatorHashBindings;
    let mediatorHashQuads: MediatorHashQuads;

    beforeEach(() => {
      mediatorHashBindings = createTestMediatorHashBindings();
      mediatorHashQuads = <any> {
        mediate: () => {
          return {
            hashFunction: (quad: Quad) => {
              return JSON.stringify(quad.subject) + JSON.stringify(quad.predicate) + JSON.stringify(quad.object.value);
            },
          };
        },
      };
      actor = new ActorQueryOperationDistinctHash(
        { name: 'actor', bus, mediatorQueryOperation, mediatorHashBindings, mediatorHashQuads },
      );
    });

    it('should test on distinct', async() => {
      const op: any = { operation: { type: 'distinct' }, context: new ActionContext() };
      await expect(actor.test(op)).resolves.toBeTruthy();
    });

    it('should not test on non-distinct', async() => {
      const op: any = { operation: { type: 'some-other-type' }, context: new ActionContext() };
      await expect(actor.test(op)).resolves
        .toFailTest('Actor actor only supports distinct operations, but got some-other-type');
    });

    it('should run with bindings', async() => {
      const op: any = { operation: { type: 'distinct' }, context: new ActionContext() };
      const output = getSafeBindings(await actor.runOperation(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: 5,
        variables: [
          {
            variable: DF.variable('a'),
            isUndef: false,
          },
        ],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, false),
      ]);
    });

    it('should run with quads', async() => {
      mediatorQueryOperation.mediate = (arg: any) => Promise.resolve({
        quadStream: new ArrayIterator([
          quad('s1', 'p1', 'o1', undefined, true),
          quad('s2', 'p2', 'o2', undefined, true),
          quad('s1', 'p1', 'o1', undefined, true),
          quad('s3', 'p3', 'o3', undefined, true),
          quad('s2', 'p2', 'o2', undefined, false),
        ]),
        metadata: () => Promise.resolve({
          cardinality: 5,
        }),
        operated: arg,
        type: 'quads',
      });

      const op: any = { operation: { type: 'distinct' }, context: new ActionContext() };
      const output = getSafeQuads(await actor.runOperation(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: 5,
      });
      expect(output.type).toBe('quads');
      await expect(arrayifyStream(output.quadStream)).resolves.toEqual([
        quad('s1', 'p1', 'o1', undefined, true),
        quad('s2', 'p2', 'o2', undefined, true),
        quad('s3', 'p3', 'o3', undefined, true),
        quad('s2', 'p2', 'o2', undefined, false),
      ]);
    });
  });
});
