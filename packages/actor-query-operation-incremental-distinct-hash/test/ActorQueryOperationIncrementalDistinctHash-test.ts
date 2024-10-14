import { BindingsFactory } from '@comunica/bindings-factory';
import { ActionContext, Bus } from '@comunica/core';
import type { IQueryOperationResultBindings } from '@comunica/types';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { ActorQueryOperationIncrementalDistinctHash } from '../lib';
import '@comunica/jest';
import {ActionContextKeyIsAddition} from "@incremunica/actor-merge-bindings-context-is-addition";
import {DevTools} from "@incremunica/dev-tools";

const DF = new DataFactory();

describe('ActorQueryOperationIncrementalDistinctHash', () => {
  let bus: any;
  let mediatorQueryOperation: any;
  let BF: BindingsFactory;

  beforeEach(async () => {
    BF = await DevTools.createBindingsFactory(DF);
    bus = new Bus({name: 'bus'});
    mediatorQueryOperation = {
      mediate: (arg: any) => Promise.resolve({
        bindingsStream: new ArrayIterator([
          BF.bindings([[DF.variable('a'), DF.literal('1')]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[DF.variable('a'), DF.literal('2')]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[DF.variable('a'), DF.literal('1')]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[DF.variable('a'), DF.literal('3')]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[DF.variable('a'), DF.literal('2')]]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ]),
        metadata: () => Promise.resolve({cardinality: 5, variables: [DF.variable('a')]}),
        operated: arg,
        type: 'bindings',
      }),
    };
  });

  describe('#newDistinctHashFilter', () => {
    let actor: ActorQueryOperationIncrementalDistinctHash;

    beforeEach(() => {
      actor = new ActorQueryOperationIncrementalDistinctHash(
        {name: 'actor', bus, mediatorQueryOperation},
      );
    });
    it('should create a filter', async () => {
      expect(actor.newHashFilter()).toBeInstanceOf(Function);
    });

    it('should create a filter that is a predicate', async () => {
      const filter = actor.newHashFilter();
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
    });

    it('should create a filter that only returns true once for equal objects', async () => {
      const filter = actor.newHashFilter();
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);

      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
    });

    it('should create a filters that are independent', async () => {
      const filter1 = actor.newHashFilter();
      const filter2 = actor.newHashFilter();
      const filter3 = actor.newHashFilter();
      expect(filter1(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter1(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);

      expect(filter2(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter2(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);

      expect(filter3(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter3(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
    });

    it('should create a filter that returns true if everything is deleted', async () => {
      const filter = actor.newHashFilter();
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(true);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);

      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(true);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
    });

    it('should create a filter that returns false if too much is deleted', async () => {
      const filter = actor.newHashFilter();
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(true);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);

      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(true);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('a')]]).setContextEntry(new ActionContextKeyIsAddition(), false))).toBe(false);
      expect(filter(BF.bindings([[DF.variable('a'), DF.literal('b')]]).setContextEntry(new ActionContextKeyIsAddition(), true))).toBe(true);
    });
  });

  describe('An ActorQueryOperationIncrementalDistinctHash instance', () => {
    let actor: ActorQueryOperationIncrementalDistinctHash;
    beforeEach(() => {
      actor = new ActorQueryOperationIncrementalDistinctHash(
        {name: 'actor', bus, mediatorQueryOperation},
      );
    });

    it('should test on distinct', () => {
      const op: any = {operation: {type: 'distinct'}, context: new ActionContext()};
      return expect(actor.test(op)).resolves.toBeTruthy();
    });

    it('should not test on non-distinct', () => {
      const op: any = {operation: {type: 'some-other-type'}, context: new ActionContext()};
      return expect(actor.test(op)).rejects.toBeTruthy();
    });

    it('should run', () => {
      const op: any = {operation: {type: 'distinct'}, context: new ActionContext()};
      return actor.run(op).then(async (output: IQueryOperationResultBindings) => {
        expect(await output.metadata()).toEqual({cardinality: 5, variables: [DF.variable('a')]});
        expect(output.type).toEqual('bindings');
        await expect(output.bindingsStream).toEqualBindingsStream([
          BF.bindings([[DF.variable('a'), DF.literal('1')]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[DF.variable('a'), DF.literal('2')]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[DF.variable('a'), DF.literal('3')]]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ]);
      });
    });
  });
});
