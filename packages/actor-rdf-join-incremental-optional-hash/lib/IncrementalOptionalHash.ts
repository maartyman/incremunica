import type { Bindings as BindingsFactoryBindings } from '@incremunica/incremental-bindings-factory';
import { BindingsFactory } from '@incremunica/incremental-bindings-factory';
import { IncrementalInnerJoin } from '@incremunica/incremental-inner-join';
import type { Bindings, BindingsStream } from '@incremunica/incremental-types';

export class IncrementalOptionalHash extends IncrementalInnerJoin {
  private readonly rightMemory: Map<string, Bindings[]> = new Map<string, Bindings[]>();
  private readonly leftMemory: Map<string, Bindings[]> = new Map<string, Bindings[]>();
  private activeElement: Bindings | null = null;
  private otherArray: Bindings[] = [];
  private index = 0;
  private readonly funHash: (entry: Bindings) => string;
  private prependArray: boolean;
  private appendArray: boolean;
  private readonly bindingsFactory = new BindingsFactory();

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

        let resultingBindings = null;
        if (this.prependArray) {
          // We need to delete the bindings with no optional bindings
          resultingBindings = this.bindingsFactory.fromBindings(<BindingsFactoryBindings> this.otherArray[this.index]);
          resultingBindings.diff = false;
        } else if (this.activeElement === null) {
          // If this.activeElement is null, then appendArray is true
          // we need to add the bindings with no optional bindings
          resultingBindings = this.bindingsFactory.fromBindings(<BindingsFactoryBindings> this.otherArray[this.index]);
          resultingBindings.diff = true;
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
        const hash = this.funHash(item);
        const rightMemEl = this.rightMemory.get(hash);
        if (this.addOrDeleteFromMemory(item, hash, this.rightMemory)) {
          const otherArray = this.leftMemory.get(hash);
          if (otherArray !== undefined) {
            if (item.diff && (rightMemEl === undefined || rightMemEl.length === 0)) {
              this.prependArray = true;
            }
            if (!item.diff && this.rightMemory.get(hash)?.length === 1) {
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
        const hash = this.funHash(item);
        if (this.addOrDeleteFromMemory(item, hash, this.leftMemory)) {
          const otherArray = this.rightMemory.get(hash);
          if (otherArray !== undefined) {
            this.activeElement = item;
            this.otherArray = otherArray;
          } else {
            return item;
          }
        }
        continue;
      }

      this.readable = false;
      return null;
    }
  }
}
