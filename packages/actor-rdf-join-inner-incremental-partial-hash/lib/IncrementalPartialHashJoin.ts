import { IncrementalInnerJoin } from '@incremunica/incremental-inner-join';
import type { Bindings, BindingsStream } from '@incremunica/incremental-types';

export class IncrementalPartialHashJoin extends IncrementalInnerJoin {
  private readonly rightMemory: Map<string, Bindings[]> = new Map<string, Bindings[]>();
  private readonly leftMemory: Map<string, Bindings[]> = new Map<string, Bindings[]>();
  private activeElement: Bindings | null = null;
  private otherArray: Bindings[] = [];
  private index = 0;
  private readonly funHash: (entry: Bindings) => string;

  public constructor(
    left: BindingsStream,
    right: BindingsStream,
    funHash: (entry: Bindings) => string,
    funJoin: (...bindings: Bindings[]) => Bindings | null,
  ) {
    super(left, right, funJoin);
    this.funHash = funHash;
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

  private addOrDeleteFromMemory(item: Bindings, hash: string, memory: Map<string, Bindings[]>): boolean {
    let array = memory.get(hash);
    if (item.diff) {
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

    if (array.length < 2 && array[0].equals(item)) {
      memory.delete(hash);
      return true;
    }

    const index = array.findIndex((bindings: Bindings) => item.equals(bindings));
    if (index !== -1) {
      array[index] = array[array.length - 1];
      array.pop();
      return true;
    }
    return false;
  }

  public read(): Bindings | null {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      if (this.ended) {
        return null;
      }

      // There is an active element
      if (this.activeElement !== null) {
        if (this.index === this.otherArray.length) {
          this.index = 0;
          this.activeElement = null;
          continue;
        }

        const resultingBindings = this.funJoin(this.activeElement, this.otherArray[this.index]);

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
        const hash = this.funHash(item);
        if (this.addOrDeleteFromMemory(item, hash, this.leftMemory)) {
          const otherArray = this.rightMemory.get(hash);
          if (otherArray !== undefined) {
            this.activeElement = item;
            this.otherArray = otherArray;
          }
        }
        continue;
      }

      item = this.rightIterator.read();
      if (item !== null) {
        const hash = this.funHash(item);
        if (this.addOrDeleteFromMemory(item, hash, this.rightMemory)) {
          const otherArray = this.leftMemory.get(hash);
          if (otherArray !== undefined) {
            this.activeElement = item;
            this.otherArray = otherArray;
          }
        }
        continue;
      }

      this.readable = false;
      if (this.leftIterator.upToDate && this.rightIterator.upToDate) {
        this.upToDate = true;
      }
      return null;
    }
  }
}
