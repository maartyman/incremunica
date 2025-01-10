import type { Bindings } from '@comunica/utils-bindings-factory';
import type { AsyncIterator } from 'asynciterator';
import { ArrayIterator } from 'asynciterator';
import '@incremunica/incremental-jest';
import { IncrementalInnerJoin } from '../lib';

const cleanupMock = jest.fn();
const hasResultMock = jest.fn();
const readMock = jest.fn();

let results = false;

class ExtendedClass extends IncrementalInnerJoin {
  public read(): Bindings | null {
    readMock();
    return null;
  }

  public _cleanup(): void {
    cleanupMock();
  }

  public hasResults(): boolean {
    hasResultMock();
    return results;
  }
}

describe('IncrementalInnerJoin', () => {
  describe('The HashBindings module', () => {
    let leftIterator: AsyncIterator<Bindings>;
    let rightIterator: AsyncIterator<Bindings>;
    const funJoin = () => null;

    beforeEach(() => {
      leftIterator = new ArrayIterator([ <any>{} ], { autoStart: false });
      rightIterator = new ArrayIterator([ <any>{} ], { autoStart: false });
    });

    it('should be a HashBindings constructor', () => {
      expect(new (<any> ExtendedClass)(
        leftIterator,
        rightIterator,
        funJoin,
      )).toBeInstanceOf(IncrementalInnerJoin);
      expect(new (<any> ExtendedClass)(
        leftIterator,
        rightIterator,
        funJoin,
      )).toBeInstanceOf(ExtendedClass);
    });
  });

  describe('An HashBindings instance', () => {
    let leftIterator: AsyncIterator<Bindings>;
    let rightIterator: AsyncIterator<Bindings>;
    const funJoin = () => null;

    beforeEach(() => {
      leftIterator = new ArrayIterator([ <any>{} ], { autoStart: false });
      rightIterator = new ArrayIterator([ <any>{} ], { autoStart: false });
    });

    it('should be readable if one of the iterators is readable', () => {
      const extendedClass = new ExtendedClass(
        new ArrayIterator([ <any>{} ], { autoStart: false }),
        new ArrayIterator([], { autoStart: false }),
        funJoin,
      );
      expect(extendedClass.readable).toBeTruthy();
    });

    it('should not be readable if none of the iterators are readable', () => {
      leftIterator.readable = false;
      rightIterator.readable = false;

      const extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin,
      );

      expect(extendedClass.readable).toBeFalsy();
    });

    it('should be destroyed if the left iterator throws an error', async() => {
      const extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin,
      );

      const error = new Promise<string>((resolve) => {
        extendedClass.once('error', (error) => {
          resolve(error);
        });
      });

      leftIterator.emit('error', 'test error');

      await expect(error).resolves.toBe('test error');
      expect(extendedClass.ended).toBeTruthy();
    });

    it('should be destroyed if the right iterator throws an error', async() => {
      const extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin,
      );

      const error = new Promise<string>((resolve) => {
        extendedClass.once('error', (error) => {
          resolve(error);
        });
      });

      rightIterator.emit('error', 'test error');

      await expect(error).resolves.toBe('test error');
      expect(extendedClass.ended).toBeTruthy();
    });

    it('should become readable if one of the iterators becomes readable', () => {
      leftIterator.readable = false;
      rightIterator.readable = false;

      const extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin,
      );

      expect(extendedClass.readable).toBeFalsy();

      leftIterator.emit('readable');

      expect(extendedClass.readable).toBeTruthy();
    });

    it('should destroy left and right iterator if no results and end left', async() => {
      const extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin,
      );

      leftIterator.emit('end');

      expect(leftIterator.done).toBeTruthy();
      expect(rightIterator.done).toBeTruthy();
    });

    it('should destroy left and right iterator if no results and end right', () => {
      const extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin,
      );

      rightIterator.emit('end');

      expect(leftIterator.done).toBeTruthy();
      expect(rightIterator.done).toBeTruthy();
    });

    it('should not end if results', () => {
      results = true;

      const extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin,
      );

      leftIterator.close();

      expect(extendedClass.ended).toBeFalsy();
    });

    it('should destroy iterators if end', () => {
      const extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin,
      );

      extendedClass._end();

      expect(leftIterator.destroyed).toBeTruthy();
      expect(rightIterator.destroyed).toBeTruthy();
    });

    it('should have all abstract functions', () => {
      const extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin,
      );

      extendedClass._cleanup();
      expect(cleanupMock).toHaveBeenCalledWith();

      extendedClass.hasResults();
      expect(hasResultMock).toHaveBeenCalledWith();

      extendedClass.read();
      expect(readMock).toHaveBeenCalledWith();
    });
  });
});
