import type { Bindings } from '@comunica/utils-bindings-factory';
import { KeysBindings } from '@incremunica/context-entries';
import { InnerJoin, Side } from '@incremunica/inner-join';

export class NestedLoopJoin extends InnerJoin {
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
    if (item.getContextEntry(KeysBindings.isAddition) ?? true) {
      memory.push(item);
      return true;
    }
    const index = memory.findIndex((bindings: Bindings) => item.equals(bindings));
    if (index !== -1) {
      memory[index] = memory.at(-1)!;
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

        const resultingBindings = (this.activeSide === Side.right ?
          this.funJoin(otherArray[this.index], this.activeElement) :
          this.funJoin(this.activeElement, otherArray[this.index]));

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
}
