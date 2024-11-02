import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import type { MediatorHashQuads } from '@comunica/bus-hash-quads';
import { ActionContext, Bus } from '@comunica/core';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { getSafeBindings, getSafeQuads } from '@comunica/utils-query-operation';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import { DevTools } from '@incremunica/dev-tools';
import arrayifyStream from 'arrayify-stream';
import { ArrayIterator } from 'asynciterator';
import type { Quad } from 'rdf-data-factory';
import { DataFactory } from 'rdf-data-factory';
import { ActorQueryOperationIncrementalDistinctHash } from '../lib';
import '@comunica/utils-jest';

const quad = require('rdf-quad');

const DF = new DataFactory();

describe('ActorQueryOperationIncrementalDistinctHash', () => {
  let bus: any;
  let mediatorQueryOperation: any;
  let BF: BindingsFactory;

  beforeEach(async() => {
    BF = await DevTools.createTestBindingsFactory(DF);
    bus = new Bus({ name: 'bus' });
    mediatorQueryOperation = {
      mediate: (arg: any) => Promise.resolve({
        bindingsStream: new ArrayIterator([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(new ActionContextKeyIsAddition(), false),
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
    let actor: ActorQueryOperationIncrementalDistinctHash;
    let mediatorHashBindings: MediatorHashBindings;
    let mediatorHashQuads: MediatorHashQuads;

    beforeEach(() => {
      mediatorHashBindings = DevTools.createTestMediatorHashBindings();
      mediatorHashQuads = <any> {
        mediate: () => {
          return {
            hashFunction: (quad: Quad) => {
              return JSON.stringify(quad.subject) + JSON.stringify(quad.predicate) + JSON.stringify(quad.object.value);
            },
          };
        },
      };
      actor = new ActorQueryOperationIncrementalDistinctHash(
        { name: 'actor', bus, mediatorQueryOperation, mediatorHashBindings, mediatorHashQuads },
      );
    });

    it('should create a filter', async() => {
      await expect(actor.newHashFilterQuads(<any>{})).resolves.toBeInstanceOf(Function);
    });

    it('should create a filter that is a predicate', async() => {
      const filter = await actor.newHashFilterQuads(<any>{});
      expect(filter(DevTools.quad(true, 's', 'p', 'o'))).toBe(true);
    });

    it('should create a filter that only returns true once for equal objects', async() => {
      const filter = await actor.newHashFilterQuads(<any>{});
      expect(filter(DevTools.quad(true, 'a', 'p', 'a'))).toBe(true);
      expect(filter(DevTools.quad(true, 'a', 'p', 'a'))).toBe(false);
      expect(filter(DevTools.quad(true, 'a', 'p', 'a'))).toBe(false);
      expect(filter(DevTools.quad(true, 'a', 'p', 'a'))).toBe(false);

      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(true);
      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(false);
    });

    it('should create a filters that are independent', async() => {
      const filter1 = await actor.newHashFilterQuads(<any>{});
      const filter2 = await actor.newHashFilterQuads(<any>{});
      const filter3 = await actor.newHashFilterQuads(<any>{});
      expect(filter1(DevTools.quad(true, 'a', 'p', 'b'))).toBe(true);
      expect(filter1(DevTools.quad(true, 'a', 'p', 'b'))).toBe(false);

      expect(filter2(DevTools.quad(true, 'a', 'p', 'b'))).toBe(true);
      expect(filter2(DevTools.quad(true, 'a', 'p', 'b'))).toBe(false);

      expect(filter3(DevTools.quad(true, 'a', 'p', 'b'))).toBe(true);
      expect(filter3(DevTools.quad(true, 'a', 'p', 'b'))).toBe(false);
    });

    it('should create a filter that returns true if everything is deleted', async() => {
      const filter = await actor.newHashFilterQuads(<any>{});
      expect(filter(DevTools.quad(true, 'a', 'p', 'a'))).toBe(true);
      expect(filter(DevTools.quad(true, 'a', 'p', 'a'))).toBe(false);
      expect(filter(DevTools.quad(false, 'a', 'p', 'a'))).toBe(false);
      expect(filter(DevTools.quad(false, 'a', 'p', 'a'))).toBe(true);
      expect(filter(DevTools.quad(true, 'a', 'p', 'a'))).toBe(true);
      expect(filter(DevTools.quad(true, 'a', 'p', 'a'))).toBe(false);

      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(true);
      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(false, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(false, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(false, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(false, 'a', 'p', 'b'))).toBe(true);
      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(true);
    });

    it('should create a filter that returns false if too much is deleted', async() => {
      const filter = await actor.newHashFilterQuads(<any>{});
      expect(filter(DevTools.quad(true, 'a', 'p', 'a'))).toBe(true);
      expect(filter(DevTools.quad(true, 'a', 'p', 'a'))).toBe(false);
      expect(filter(DevTools.quad(false, 'a', 'p', 'a'))).toBe(false);
      expect(filter(DevTools.quad(false, 'a', 'p', 'a'))).toBe(true);
      expect(filter(DevTools.quad(false, 'a', 'p', 'a'))).toBe(false);
      expect(filter(DevTools.quad(true, 'a', 'p', 'a'))).toBe(true);
      expect(filter(DevTools.quad(true, 'a', 'p', 'a'))).toBe(false);

      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(true);
      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(false, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(false, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(false, 'a', 'p', 'b'))).toBe(false);
      expect(filter(DevTools.quad(false, 'a', 'p', 'b'))).toBe(true);
      expect(filter(DevTools.quad(false, 'a', 'p', 'a'))).toBe(false);
      expect(filter(DevTools.quad(true, 'a', 'p', 'b'))).toBe(true);
    });
  });

  describe('newHashFilterBindings', () => {
    let actor: ActorQueryOperationIncrementalDistinctHash;
    let mediatorHashBindings: MediatorHashBindings;
    let mediatorHashQuads: MediatorHashQuads;

    beforeEach(() => {
      mediatorHashBindings = DevTools.createTestMediatorHashBindings();
      mediatorHashQuads = <any> {
        mediate: () => {
          return {
            hashFunction: (quad: Quad) => {
              return JSON.stringify(quad.subject) + JSON.stringify(quad.predicate) + JSON.stringify(quad.object.value);
            },
          };
        },
      };
      actor = new ActorQueryOperationIncrementalDistinctHash(
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
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
    });

    it('should create a filter that only returns true once for equal objects', async() => {
      const filter = await actor.newHashFilterBindings(<any>{}, [ DF.variable('a') ]);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);

      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
    });

    it('should create a filters that are independent', async() => {
      const filter1 = await actor.newHashFilterBindings(<any>{}, []);
      const filter2 = await actor.newHashFilterBindings(<any>{}, []);
      const filter3 = await actor.newHashFilterBindings(<any>{}, []);
      expect(filter1(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter1(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);

      expect(filter2(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter2(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);

      expect(filter3(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter3(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
    });

    it('should create a filter that returns true if everything is deleted', async() => {
      const filter = await actor.newHashFilterBindings(<any>{}, [ DF.variable('a') ]);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);

      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
    });

    it('should create a filter that returns false if too much is deleted', async() => {
      const filter = await actor.newHashFilterBindings(<any>{}, [ DF.variable('a') ]);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);

      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(true);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('b') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
    });
  });

  describe('An ActorQueryOperationIncrementalDistinctHash instance', () => {
    let actor: ActorQueryOperationIncrementalDistinctHash;
    let mediatorHashBindings: MediatorHashBindings;
    let mediatorHashQuads: MediatorHashQuads;

    beforeEach(() => {
      mediatorHashBindings = DevTools.createTestMediatorHashBindings();
      mediatorHashQuads = <any> {
        mediate: () => {
          return {
            hashFunction: (quad: Quad) => {
              return JSON.stringify(quad.subject) + JSON.stringify(quad.predicate) + JSON.stringify(quad.object.value);
            },
          };
        },
      };
      actor = new ActorQueryOperationIncrementalDistinctHash(
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
        BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(new ActionContextKeyIsAddition(), false),
      ]);
    });

    it('should run with quads', async() => {
      mediatorQueryOperation.mediate = (arg: any) => Promise.resolve({
        quadStream: new ArrayIterator([
          DevTools.quad(true, 's1', 'p1', 'o1'),
          DevTools.quad(true, 's2', 'p2', 'o2'),
          DevTools.quad(true, 's1', 'p1', 'o1'),
          quad('s3', 'p3', 'o3'),
          DevTools.quad(false, 's2', 'p2', 'o2'),
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
        DevTools.quad(true, 's1', 'p1', 'o1'),
        DevTools.quad(true, 's2', 'p2', 'o2'),
        DevTools.quad(true, 's3', 'p3', 'o3'),
        DevTools.quad(false, 's2', 'p2', 'o2'),
      ]);
    });
  });
});
