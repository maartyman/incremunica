import 'jest';
import { KeysBindings } from '@incremunica/context-entries';
import { BF } from '@incremunica/dev-tools';
import { isAddition } from '../lib';

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
});
