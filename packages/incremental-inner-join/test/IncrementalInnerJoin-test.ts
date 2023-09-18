import {ArrayIterator, AsyncIterator} from 'asynciterator';
import '@incremunica/incremental-jest';
import {IncrementalInnerJoin} from "../lib";
import { Bindings } from '@incremunica/incremental-types';

let cleanupMock = jest.fn();
let hasResultMock = jest.fn();
let readMock = jest.fn();

let results = false;

class ExtendedClass extends IncrementalInnerJoin {
  read(): Bindings | null {
    readMock();
    return null;
  }
  _cleanup(): void {
    cleanupMock();
  }

  hasResults(): boolean {
    hasResultMock();
    return results;
  }
}

describe('IncrementalInnerJoin', () => {
  describe('The HashBindings module', () => {
    let leftIterator: AsyncIterator<Bindings>;
    let rightIterator: AsyncIterator<Bindings>;
    let funJoin = () => null;

    beforeEach(() => {
      leftIterator = new ArrayIterator([<any>{}], {autoStart: false});
      rightIterator = new ArrayIterator([<any>{}], {autoStart: false});
    });

    it('should be a HashBindings constructor', () => {
      expect(new (<any> ExtendedClass)(
        leftIterator,
        rightIterator,
        funJoin
      )).toBeInstanceOf(IncrementalInnerJoin);
      expect(new (<any> ExtendedClass)(
        leftIterator,
        rightIterator,
        funJoin
      )).toBeInstanceOf(ExtendedClass);
    });
  });

  describe('An HashBindings instance', () => {
    let leftIterator: AsyncIterator<Bindings>;
    let rightIterator: AsyncIterator<Bindings>;
    let funJoin = () => null;

    beforeEach(() => {
      leftIterator = new ArrayIterator([<any>{}], {autoStart: false});
      rightIterator = new ArrayIterator([<any>{}], {autoStart: false});
    });

    it('should be readable if one of the iterators is readable', () => {
      let extendedClass = new ExtendedClass(
        new ArrayIterator([<any>{}], {autoStart: false}),
        new ArrayIterator([], {autoStart: false}),
        funJoin
      );
      expect(extendedClass.readable).toBeTruthy();
    });

    it('should not be readable if none of the iterators are readable', () => {
      leftIterator.readable = false;
      rightIterator.readable = false;

      let extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin
      );

      expect(extendedClass.readable).toBeFalsy();
    });

    it('should be destroyed if the left iterator throws an error', async () => {
      let extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin
      );

      let error = new Promise<string>((resolve) => {
        extendedClass.once("error", (error) => {
          resolve(error);
        });
      });

      leftIterator.emit("error", "test error");

      expect(await error).toEqual("test error");
      expect(extendedClass.ended).toBeTruthy();
    });

    it('should be destroyed if the right iterator throws an error', async () => {
      let extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin
      );

      let error = new Promise<string>((resolve) => {
        extendedClass.once("error", (error) => {
          resolve(error);
        });
      });

      rightIterator.emit("error", "test error");

      expect(await error).toEqual("test error");
      expect(extendedClass.ended).toBeTruthy();
    });

    it('should become readable if one of the iterators becomes readable', () => {
      leftIterator.readable = false;
      rightIterator.readable = false;

      let extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin
      );

      expect(extendedClass.readable).toBeFalsy();

      leftIterator.emit("readable")


      expect(extendedClass.readable).toBeTruthy();
    });

    it('should destroy left and right iterator if no results and end left', async () => {
      let extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin
      );

      leftIterator.emit("end")

      expect(leftIterator.done).toBeTruthy()
      expect(rightIterator.done).toBeTruthy()
    });

    it('should destroy left and right iterator if no results and end right', () => {
      let extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin
      );

      rightIterator.emit("end")

      expect(leftIterator.done).toBeTruthy()
      expect(rightIterator.done).toBeTruthy()
    });

    it('should not end if results', () => {
      results = true

      let extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin
      );

      leftIterator.close()

      expect(extendedClass.ended).toBeFalsy();
    });

    it('should destroy iterators if end', () => {
      let extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin
      );

      extendedClass._end()

      expect(leftIterator.destroyed).toBeTruthy();
      expect(rightIterator.destroyed).toBeTruthy();
    });

    it('should have all abstract functions', () => {
      let extendedClass = new ExtendedClass(
        leftIterator,
        rightIterator,
        funJoin
      );

      extendedClass._cleanup();
      expect(cleanupMock).toBeCalled();

      extendedClass.hasResults();
      expect(hasResultMock).toBeCalled();

      extendedClass.read();
      expect(readMock).toBeCalled();
    });

  });
});
