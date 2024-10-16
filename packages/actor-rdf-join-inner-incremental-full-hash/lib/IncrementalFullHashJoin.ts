import type { Bindings } from '@comunica/bindings-factory';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import { HashBindings } from '@incremunica/hash-bindings';
import { IncrementalInnerJoin } from '@incremunica/incremental-inner-join';
import type { AsyncIterator } from 'asynciterator';
import type { IMapObject } from './DualKeyHashMap';
import { DualKeyHashMap } from './DualKeyHashMap';

export class IncrementalFullHashJoin extends IncrementalInnerJoin {
  private readonly rightMemory: DualKeyHashMap<Bindings> = new DualKeyHashMap<Bindings>();
  private readonly leftMemory: DualKeyHashMap<Bindings> = new DualKeyHashMap<Bindings>();
  private activeElement: Bindings | null = null;
  private otherArray: IterableIterator<IMapObject<Bindings>> = [][Symbol.iterator]();
  private otherElement: IMapObject<Bindings> | null = null;
  private count = 0;
  private readonly funHash: (entry: Bindings) => string;
  private readonly hashBindings = new HashBindings();

  public constructor(
    left: AsyncIterator<Bindings>,
    right: AsyncIterator<Bindings>,
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

  private addOrDeleteFromMemory(item: Bindings, joinHash: string, memory: DualKeyHashMap<Bindings>): boolean {
    if (item.getContextEntry(new ActionContextKeyIsAddition())) {
      memory.set(this.hashBindings.hash(item), joinHash, item);
      return true;
    }
    return memory.delete(this.hashBindings.hash(item), joinHash);
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
        const hash = this.funHash(item);
        if (this.addOrDeleteFromMemory(item, hash, this.leftMemory)) {
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
        const hash = this.funHash(item);
        if (this.addOrDeleteFromMemory(item, hash, this.rightMemory)) {
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
