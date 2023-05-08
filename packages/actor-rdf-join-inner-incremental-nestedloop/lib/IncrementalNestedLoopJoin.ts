import { IncrementalInnerJoin, Side } from '@comunica/incremental-inner-join';
import type { Bindings } from '@comunica/incremental-types';

export class IncrementalNestedLoopJoin extends IncrementalInnerJoin {
  private readonly rightMemory: Bindings[] = [];
  private readonly leftMemory: Bindings[] = [];
  private activeElement: Bindings | null = null;
  private activeSide: Side = Side.left;
  private index = 0;

  protected _cleanup(): void {
    this.activeElement = null;
  }

  protected hasResults(): boolean {
    return !this.leftIterator.done ||
      !this.rightIterator.done ||
      this.activeElement !== null;
  }

  private addOrDeleteFromMemory(item: Bindings, memory: Bindings[]): boolean {
    if (item.diff) {
      memory.push(item);
      return true;
    }
    const index = memory.findIndex((bindings: Bindings) => item.equals(bindings));
    if (index !== -1) {
      memory[index] = memory[memory.length - 1];
      memory.pop();
      return true;
    }
    return false;
  }

  public read(): Bindings | null {
    let otherArray;
    if (this.activeSide === Side.right) {
      otherArray = this.leftMemory;
    } else {
      otherArray = this.rightMemory;
    }
    // eslint-disable-next-line no-constant-condition
    while (true) {
      if (this.ended) {
        return null;
      }

      // There is an active element
      if (this.activeElement !== null) {
        if (this.index === otherArray.length) {
          this.index = 0;
          this.activeElement = null;
          continue;
        }

        const resultingBindings = this.activeSide === Side.right ?
          this.funJoin(otherArray[this.index], this.activeElement) :
          this.funJoin(this.activeElement, otherArray[this.index]);

        this.index++;

        if (resultingBindings !== null) {
          return resultingBindings;
        }
        continue;
      }

      if (!this.hasResults()) {
        this._end();
      }

      let item = this.leftIterator.read();
      if (item !== null) {
        if (this.addOrDeleteFromMemory(item, this.leftMemory)) {
          this.activeElement = item;
          this.activeSide = Side.left;
          otherArray = this.rightMemory;
        }
        continue;
      }

      item = this.rightIterator.read();
      if (item !== null) {
        if (this.addOrDeleteFromMemory(item, this.rightMemory)) {
          this.activeElement = item;
          this.activeSide = Side.right;
          otherArray = this.leftMemory;
        }
        continue;
      }

      this.readable = false;
      return null;
    }
  }

  //
  // public read () {
  //
  //
  // let activeArray;
  // let otherArray;
  // if (this.activeSide === Side.right) {
  // activeArray = this.rightMemory;
  // otherArray = this.leftMemory;
  // }
  // else {
  // activeArray = this.leftMemory;
  // otherArray = this.rightMemory;
  // }
  // while (true) {
  // if (this.ended)
  //       return null;
  // if (this.activeElement === null) {
  //       this.activeSide = (this.activeSide == Side.right)? Side.left : Side.right;
  //
  //       if (!this.hasResults()) {
  //         this._end();
  //       }
  //
  //       if (!(this.rightIterator.readable || this.leftIterator.readable)) {
  //         this.readable = false;
  //         return null;
  //       }
  //
  //       if (this.activeSide == Side.right) {
  //         this.activeElement = this.rightIterator.read();
  //         if (this.activeElement === null) {
  //           continue;
  //         }
  //         activeArray = this.rightMemory;
  //         otherArray = this.leftMemory;
  //       }
  //       else {
  //         this.activeElement = this.leftIterator.read();
  //         if (this.activeElement === null) {
  //           continue;
  //         }
  //         activeArray = this.leftMemory;
  //         otherArray = this.rightMemory;
  //       }
  //
  //       if (this.activeElement.diff) {
  //         activeArray.push(this.activeElement);
  //       }
  //       else {
  //         let index = activeArray.findIndex((bindings: Bindings) => {
  //           if (!this.activeElement) return false;
  //           return this.activeElement.equals(bindings);
  //         });
  //         if (index != -1) {
  //           activeArray[index] = activeArray[activeArray.length-1];
  //           activeArray.pop();
  //         }
  //         else {
  //           this.activeElement = null;
  //           continue;
  //         }
  //       }
  // }
  //
  // if (this.index == otherArray.length) {
  //       this.index = 0;
  //       this.activeElement = null;
  //       continue;
  // }
  //
  // this.index++;
  //
  // let resultingBindings = (this.activeSide == Side.right)?
  //       this.funJoin(otherArray[this.index - 1], this.activeElement) :
  //       this.funJoin(this.activeElement, otherArray[this.index - 1]);
  //
  // if (resultingBindings != null) {
  //       return resultingBindings;
  // }
  // }
  // }
  //
}
