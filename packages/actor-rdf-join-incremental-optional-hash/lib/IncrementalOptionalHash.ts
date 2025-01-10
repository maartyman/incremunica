import type { Bindings } from '@comunica/utils-bindings-factory';
import { KeysBindings } from '@incremunica/context-entries';
import { IncrementalInnerJoin } from '@incremunica/incremental-inner-join';
import type { AsyncIterator } from 'asynciterator';

export class IncrementalOptionalHash extends IncrementalInnerJoin {
  private readonly rightMemory: Map<number, Bindings[]> = new Map<number, Bindings[]>();
  private readonly leftMemory: Map<number, Bindings[]> = new Map<number, Bindings[]>();
  private activeElement: Bindings | null = null;
  private otherArray: Bindings[] = [];
  private index = 0;
  private readonly joinHash: (entry: Bindings) => number;
  private prependArray: boolean;
  private appendArray: boolean;

  public constructor(
    left: AsyncIterator<Bindings>,
    right: AsyncIterator<Bindings>,
    funJoin: (...bindings: Bindings[]) => Bindings | null,
    joinHash: (entry: Bindings) => number,
  ) {
    super(left, right, funJoin);
    this.joinHash = joinHash;
  }

  protected _cleanup(): void {
    this.leftMemory.clear();
    this.rightMemory.clear();
    this.activeElement = null;
  }

  protected hasResults(): boolean {
    return !this.leftIterator.done ||
      !this.rightIterator.done ||
      this.activeElement !== null;
  }

  private addOrDeleteFromMemory(item: Bindings, hash: number, memory: Map<number, Bindings[]>): boolean {
    let array = memory.get(hash);
    if (item.getContextEntry(KeysBindings.isAddition)) {
      if (array === undefined) {
        array = [];
        memory.set(hash, array);
      }
      array.push(item);
      return true;
    }

    if (array === undefined) {
      return false;
    }

    if (array.length === 1 && array[0].equals(item)) {
      memory.delete(hash);
      return true;
    }

    const index = array.findIndex((bindings: Bindings) => item.equals(bindings));
    if (index !== -1) {
      array[index] = array.at(-1)!;
      array.pop();
      return true;
    }
    return false;
  }

  public read(): Bindings | null {
    while (true) {
      if (this.ended) {
        return null;
      }

      // There is an active element
      if (this.activeElement !== null || this.appendArray) {
        if (this.index === this.otherArray.length) {
          if (this.prependArray) {
            this.prependArray = false;
            this.index = 0;
            continue;
          }
          if (this.appendArray && this.activeElement !== null) {
            this.index = 0;
            this.activeElement = null;
            continue;
          }
          this.appendArray = false;
          this.index = 0;
          this.activeElement = null;
          continue;
        }

        let resultingBindings: null | Bindings = null;
        if (this.prependArray) {
          // We need to delete the bindings with no optional bindings
          resultingBindings = this.otherArray[this.index];
          resultingBindings = resultingBindings.setContextEntry(KeysBindings.isAddition, false);
        } else if (this.activeElement === null) {
          // If this.activeElement is null, then appendArray is true
          // we need to add the bindings with no optional bindings
          resultingBindings = this.otherArray[this.index];
          resultingBindings = resultingBindings.setContextEntry(KeysBindings.isAddition, true);
        } else {
          // Otherwise merge bindings
          resultingBindings = this.funJoin(this.activeElement, this.otherArray[this.index]);
        }

        this.index++;

        if (resultingBindings !== null) {
          return resultingBindings;
        }
        continue;
      }

      if (!this.hasResults()) {
        this._end();
      }

      let item = this.rightIterator.read();
      if (item !== null) {
        const hash = this.joinHash(item);
        const rightMemEl = this.rightMemory.get(hash);
        if (this.addOrDeleteFromMemory(item, hash, this.rightMemory)) {
          const otherArray = this.leftMemory.get(hash);
          if (otherArray !== undefined) {
            if (
              item.getContextEntry(KeysBindings.isAddition) &&
              (rightMemEl === undefined || rightMemEl.length === 0)) {
              this.prependArray = true;
            }
            if (!item.getContextEntry(KeysBindings.isAddition) && this.rightMemory.get(hash)?.length === 1) {
              this.appendArray = true;
            }
            this.activeElement = item;
            this.otherArray = otherArray;
          }
        }
        continue;
      }

      item = this.leftIterator.read();
      if (item !== null) {
        const hash = this.joinHash(item);
        if (this.addOrDeleteFromMemory(item, hash, this.leftMemory)) {
          const otherArray = this.rightMemory.get(hash);
          if (otherArray === undefined) {
            return item;
          }
          this.activeElement = item;
          this.otherArray = otherArray;
        }
        continue;
      }

      this.readable = false;
      return null;
    }
  }
}
