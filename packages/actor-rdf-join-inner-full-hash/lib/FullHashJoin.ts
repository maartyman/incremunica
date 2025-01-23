import type { Bindings } from '@comunica/utils-bindings-factory';
import { KeysBindings } from '@incremunica/context-entries';
import { InnerJoin } from '@incremunica/inner-join';
import type { AsyncIterator } from 'asynciterator';
import type { IMapObject } from './DualKeyHashMap';
import { DualKeyHashMap } from './DualKeyHashMap';

export class FullHashJoin extends InnerJoin {
  private readonly rightMemory: DualKeyHashMap<Bindings> = new DualKeyHashMap<Bindings>();
  private readonly leftMemory: DualKeyHashMap<Bindings> = new DualKeyHashMap<Bindings>();
  private activeElement: Bindings | null = null;
  private otherArray: IterableIterator<IMapObject<Bindings>> = [][Symbol.iterator]();
  private otherElement: IMapObject<Bindings> | null = null;
  private count = 0;
  private readonly joinHash: (entry: Bindings) => string;
  private readonly leftHash: (entry: Bindings) => string;
  private readonly rightHash: (entry: Bindings) => string;

  public constructor(
    left: AsyncIterator<Bindings>,
    right: AsyncIterator<Bindings>,
    funJoin: (...bindings: Bindings[]) => Bindings | null,
    joinHash: (entry: Bindings) => string,
    leftHash: (entry: Bindings) => string,
    rightHash: (entry: Bindings) => string,
  ) {
    super(left, right, funJoin);
    this.joinHash = joinHash;
    this.leftHash = leftHash;
    this.rightHash = rightHash;
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

  private addOrDeleteFromMemory(
    item: Bindings,
    joinHash: string,
    memory: DualKeyHashMap<Bindings>,
    hash: string,
  ): boolean {
    const isAddition = item.getContextEntry(KeysBindings.isAddition) ?? true;
    if (isAddition) {
      memory.set(hash, joinHash, item);
      return true;
    }
    return memory.delete(hash, joinHash);
  }

  public read(): Bindings | null {
    while (true) {
      if (this.ended) {
        return null;
      }

      // There is an active element
      if (this.activeElement !== null) {
        if (this.otherElement !== null) {
          this.count++;

          const resultingBindings = this.funJoin(this.activeElement, this.otherElement.value);

          if (this.otherElement.count === this.count) {
            this.otherElement = null;
            this.count = 0;
          }

          if (resultingBindings !== null) {
            return resultingBindings;
          }
        }
        const next = this.otherArray.next();
        if (next.done) {
          this.activeElement = null;
          this.otherElement = null;
        } else {
          this.otherElement = next.value;
        }
        continue;
      }

      if (!this.hasResults()) {
        this._end();
      }

      let item = this.leftIterator.read();
      if (item !== null) {
        const hash = this.joinHash(item);
        if (this.addOrDeleteFromMemory(item, hash, this.leftMemory, this.leftHash(item))) {
          const otherArray = this.rightMemory.getAll(hash);
          if (otherArray !== undefined) {
            this.activeElement = item;
            this.otherArray = otherArray;
          }
        }
        continue;
      }

      item = this.rightIterator.read();
      if (item !== null) {
        const hash = this.joinHash(item);
        if (this.addOrDeleteFromMemory(item, hash, this.rightMemory, this.rightHash(item))) {
          const otherArray = this.leftMemory.getAll(hash);
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
