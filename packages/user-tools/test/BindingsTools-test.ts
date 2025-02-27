import 'jest';
import { KeysBindings } from '@incremunica/context-entries';
import { BF, DF } from '@incremunica/dev-tools';
import { getBindingsIndex, isAddition } from '../lib';

describe('BindingsTools', () => {
  describe('isAddition', () => {
    it('should return true for bindings with no context', async() => {
      expect(isAddition(BF.bindings())).toBeTruthy();
    });

    it('should return true for bindings with isAddition context true', async() => {
      expect(isAddition(BF.bindings().setContextEntry(KeysBindings.isAddition, true))).toBeTruthy();
    });

    it('should return false for bindings with isAddition context false', async() => {
      expect(isAddition(BF.bindings().setContextEntry(KeysBindings.isAddition, true))).toBeTruthy();
    });
  });

  describe('getBindingsIndex', () => {
    it('should return -1 for bindings with no context', async() => {
      expect(getBindingsIndex(BF.bindings())).toBe(-1);
    });

    it('should return the index for bindings with a order context', async() => {
      expect(getBindingsIndex(BF.bindings().setContextEntry(KeysBindings.order, {
        hash: 'abc',
        result: DF.literal('abc'),
        index: 5,
      }))).toBe(5);
    });
  });
});
