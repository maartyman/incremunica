import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { KeysBindings } from '@incremunica/context-entries';
import { createTestBindingsFactory } from '@incremunica/dev-tools';
import { DataFactory } from 'rdf-data-factory';
import { ActorMergeBindingsContextIsAddition } from '../lib';
import '@incremunica/jest';

const DF = new DataFactory();

describe('ActorMergeBindingsContextIsAddition', () => {
  let bus: any;
  let actor: ActorMergeBindingsContextIsAddition;
  let context: IActionContext;

  beforeEach(async() => {
    bus = new Bus({ name: 'bus' });
    actor = new ActorMergeBindingsContextIsAddition({ name: 'actor', bus });
    context = new ActionContext();
  });

  it('should test', async() => {
    expect((await actor.test({ context })).get()).toBe(true);
  });

  it('should run', async() => {
    await expect(actor.run({ context })).resolves.toMatchObject(
      {
        mergeHandlers: {
          '@incremunica/bindings:isAddition': {
            run: expect.any(Function),
          },
        },
      },
    );
  });
  describe('merge handler', () => {
    let mergeHandler: (...args: boolean[]) => boolean;
    beforeEach(async() => {
      mergeHandler = (await actor.run({ context }))
        .mergeHandlers['@incremunica/bindings:isAddition'].run;
    });

    it('should return false if the first is false', async() => {
      const inputSets = [ false, true ];
      expect(mergeHandler(...inputSets)).toBe(false);
    });

    it('should return false if the second is false', async() => {
      const inputSets = [ true, false ];
      expect(mergeHandler(...inputSets)).toBe(false);
    });

    it('should return false if both are false', async() => {
      const inputSets = [ false, false ];
      expect(mergeHandler(...inputSets)).toBe(false);
    });

    it('should return true if both are true', async() => {
      const inputSets = [ true, true ];
      expect(mergeHandler(...inputSets)).toBe(true);
    });

    it('should work with multiple values', async() => {
      const inputSets = [ true, true, false, true ];
      expect(mergeHandler(...inputSets)).toBe(false);
    });
  });

  describe('actual bindings', () => {
    let BF: BindingsFactory;

    beforeEach(async() => {
      BF = await createTestBindingsFactory(DF);
    });

    it('should work with addition bindings', async() => {
      const bindings1 = BF.bindings([
        [ DF.variable('a'), DF.literal('1') ],
      ]).setContextEntry(KeysBindings.isAddition, true);
      const bindings2 = BF.bindings([
        [ DF.variable('a'), DF.literal('1') ],
      ]).setContextEntry(KeysBindings.isAddition, true);
      expect(bindings1.merge(bindings2)).toEqualBindings(BF.bindings([
        [ DF.variable('a'), DF.literal('1') ],
      ]).setContextEntry(KeysBindings.isAddition, true));
    });

    it('should work with deletion bindings', async() => {
      const bindings1 = BF.bindings([
        [ DF.variable('a'), DF.literal('1') ],
      ]).setContextEntry(KeysBindings.isAddition, false);
      const bindings2 = BF.bindings([
        [ DF.variable('a'), DF.literal('1') ],
      ]).setContextEntry(KeysBindings.isAddition, true);
      expect(bindings1.merge(bindings2)).toEqualBindings(BF.bindings([
        [ DF.variable('a'), DF.literal('1') ],
      ]).setContextEntry(KeysBindings.isAddition, false));
    });
  });
});
