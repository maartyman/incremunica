import type { Bindings } from '@comunica/bindings-factory';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import { IncrementalInnerJoin } from '@incremunica/incremental-inner-join';
import type { AsyncIterator } from 'asynciterator';

export class IncrementalPartialHashJoin extends IncrementalInnerJoin {
  private readonly rightMemory: Map<string, Bindings[]> = new Map<string, Bindings[]>();
  private readonly leftMemory: Map<string, Bindings[]> = new Map<string, Bindings[]>();
  private activeElement: Bindings | null = null;
  private otherArray: Bindings[] = [];
  private index = 0;
  private readonly funHash: (entry: Bindings) => string;

  public constructor(
    left: AsyncIterator<Bindings>,
    right: AsyncIterator<Bindings>,
    funJoin: (...bindings: Bindings[]) => Bindings | null,
    funHash: (entry: Bindings) => string,
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
    if (item.getContextEntry(new ActionContextKeyIsAddition())) {
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
      return null;
    }
  }
}
