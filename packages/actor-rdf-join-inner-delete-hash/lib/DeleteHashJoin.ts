import type { Bindings } from '@comunica/utils-bindings-factory';
import { KeysBindings } from '@incremunica/context-entries';
import { InnerJoin } from '@incremunica/inner-join';
import type { AsyncIterator } from 'asynciterator';

interface IBindingsWithCount {
  bindings: Bindings;
  count: number;
}

export class DeleteHashJoin extends InnerJoin {
  private readonly rightMemory: Map<number, IBindingsWithCount> = new Map<number, IBindingsWithCount>();
  private readonly leftMemory: Map<number, IBindingsWithCount> = new Map<number, IBindingsWithCount>();
  private activeElement: Bindings | null = null;
  private otherArray: IterableIterator<IBindingsWithCount> = [][Symbol.iterator]();
  private count = 0;
  private otherArrayElement: IteratorResult<IBindingsWithCount, IBindingsWithCount | null> =
    { done: true, value: null };

  private readonly overlappingVariables: string[];
  private readonly hashLeft: (entry: Bindings) => number;
  private readonly hashRight: (entry: Bindings) => number;

  public constructor(
    left: AsyncIterator<Bindings>,
    right: AsyncIterator<Bindings>,
    funJoin: (...bindings: Bindings[]) => Bindings | null,
    overlappingVariables: string[],
    hashLeft: (bindings: Bindings) => number,
    hashRight: (bindings: Bindings) => number,
  ) {
    super(left, right, funJoin);
    this.overlappingVariables = overlappingVariables;
    this.hashLeft = hashLeft;
    this.hashRight = hashRight;
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
    memory: Map<number, IBindingsWithCount>,
    hashFunc: (bindings: Bindings) => number,
  ): boolean {
    const hash = hashFunc(item);
    const el = memory.get(hash);
    if (item.getContextEntry(KeysBindings.isAddition) ?? true) {
      if (el === undefined) {
        memory.set(hash, { bindings: item, count: 1 });
      } else {
        el.count++;
      }
      return true;
    }
    if (el !== undefined) {
      if (el.count <= 1) {
        memory.delete(hash);
        return true;
      }
      el.count--;
    }
    return true;
  }

  public read(): Bindings | null {
    while (true) {
      if (this.ended) {
        return null;
      }

      // There is an active element
      if (this.activeElement !== null) {
        if (this.otherArrayElement.value === null || this.count === this.otherArrayElement.value.count) {
          this.otherArrayElement = this.otherArray.next();
          this.count = 0;
          // eslint-disable-next-line ts/prefer-nullish-coalescing
          if (this.otherArrayElement.done || !this.otherArrayElement.value) {
            this.activeElement = null;
            this.otherArrayElement = { done: true, value: null };
            continue;
          }
        }
        let areCompatible = true;
        for (const variable of this.overlappingVariables) {
          const activeElementTerm = this.activeElement.get(variable);
          const otherArrayTerm = this.otherArrayElement.value.bindings.get(variable);
          if (activeElementTerm && otherArrayTerm && !activeElementTerm.equals(otherArrayTerm)) {
            areCompatible = false;
          }
        }
        let resultingBindings = null;
        if (areCompatible) {
          resultingBindings = this.funJoin(this.activeElement, this.otherArrayElement.value.bindings);
        }

        this.count++;

        if (resultingBindings !== null) {
          return resultingBindings;
        }
        continue;
      }

      if (!this.hasResults()) {
        this._end();
      }

      let item = null;
      if (this.leftIterator.readable) {
        item = this.leftIterator.read();
      }
      if (item !== null) {
        if (this.addOrDeleteFromMemory(item, this.leftMemory, this.hashLeft)) {
          this.activeElement = item;
          this.otherArray = this.rightMemory.values();
        }
        continue;
      }

      if (this.rightIterator.readable) {
        item = this.rightIterator.read();
      }
      if (item !== null) {
        if (this.addOrDeleteFromMemory(item, this.rightMemory, this.hashRight)) {
          this.activeElement = item;
          this.otherArray = this.leftMemory.values();
        }
        continue;
      }

      this.readable = false;
      return null;
    }
  }
}
