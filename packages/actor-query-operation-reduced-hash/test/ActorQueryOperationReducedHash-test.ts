import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import { ActionContext, Bus } from '@comunica/core';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { getSafeBindings } from '@comunica/utils-query-operation';
import { KeysBindings } from '@incremunica/context-entries';
import { createTestMediatorHashBindings, createTestBindingsFactory } from '@incremunica/dev-tools';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { ActorQueryOperationReducedHash } from '../lib';
import '@comunica/utils-jest';

const DF = new DataFactory();

describe('ActorQueryOperationReducedHash', () => {
  let bus: any;
  let mediatorQueryOperation: any;
  let BF: BindingsFactory;

  beforeEach(async() => {
    BF = await createTestBindingsFactory(DF);
    bus = new Bus({ name: 'bus' });
    mediatorQueryOperation = {
      mediate: (arg: any) => Promise.resolve({
        bindingsStream: new ArrayIterator([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
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

  describe('newHashFilter', () => {
    let actor: ActorQueryOperationReducedHash;
    let mediatorHashBindings: MediatorHashBindings;

    beforeEach(() => {
      mediatorHashBindings = createTestMediatorHashBindings();
      actor = new ActorQueryOperationReducedHash(
        { name: 'actor', bus, mediatorQueryOperation, mediatorHashBindings },
      );
    });

    it('should create a filter', async() => {
      await expect(actor.newHashFilter(<any>{}, [])).resolves.toBeInstanceOf(Function);
    });

    it('should create a filter that is a predicate', async() => {
      const filter = await actor.newHashFilter(<any>{}, []);
      expect(filter(BF.bindings([
        [ DF.variable('a'), DF.literal('a') ],
      ]).setContextEntry(KeysBindings.isAddition, true))).toBe(true);
    });

    it('should create a filter that only returns true once for equal objects', async() => {
      const filter = await actor.newHashFilter(<any>{}, [ DF.variable('a') ]);
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
      const filter1 = await actor.newHashFilter(<any>{}, []);
      const filter2 = await actor.newHashFilter(<any>{}, []);
      const filter3 = await actor.newHashFilter(<any>{}, []);
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
      const filter = await actor.newHashFilter(<any>{}, [ DF.variable('a') ]);
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
      const filter = await actor.newHashFilter(<any>{}, [ DF.variable('a') ]);
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

  describe('An ActorQueryOperationReducedHash instance', () => {
    let actor: ActorQueryOperationReducedHash;
    let mediatorHashBindings: MediatorHashBindings;

    beforeEach(() => {
      mediatorHashBindings = createTestMediatorHashBindings();
      actor = new ActorQueryOperationReducedHash(
        { name: 'actor', bus, mediatorQueryOperation, mediatorHashBindings },
      );
    });

    it('should test on reduced', async() => {
      const op: any = { operation: { type: 'reduced' }, context: new ActionContext() };
      await expect(actor.test(op)).resolves.toBeTruthy();
    });

    it('should not test on non-reduced', async() => {
      const op: any = { operation: { type: 'some-other-type' }, context: new ActionContext() };
      await expect(actor.test(op)).resolves
        .toFailTest('Actor actor only supports reduced operations, but got some-other-type');
    });

    it('should run with bindings', async() => {
      const op: any = { operation: { type: 'reduced' }, context: new ActionContext() };
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
  });
});
