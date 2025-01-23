import type { Bindings } from '@comunica/utils-bindings-factory';
import { KeysBindings } from '@incremunica/context-entries';
import { AsyncIterator } from 'asynciterator';

export class MinusHash extends AsyncIterator<Bindings> {
  private readonly leftIterator: AsyncIterator<Bindings>;
  private readonly rightIterator: AsyncIterator<Bindings>;
  private readonly leftMemory: Map<string, Bindings[]> = new Map<string, Bindings[]>();
  private readonly rightMemory: Map<string, number> = new Map<string, number>();
  private readonly bindingBuffer: Bindings[] = [];
  private readonly joinHash: (entry: Bindings) => string;

  public constructor(
    leftIterator: AsyncIterator<Bindings>,
    rightIterator: AsyncIterator<Bindings>,
    joinHash: (entry: Bindings) => string,
  ) {
    super();

    this.leftIterator = leftIterator;
    this.rightIterator = rightIterator;
    this.joinHash = joinHash;

    this.on('end', () => this._cleanup());

    if (this.leftIterator.readable || this.rightIterator.readable) {
      this.readable = true;
    }

    this.leftIterator.on('error', error => this.destroy(error));
    this.rightIterator.on('error', error => this.destroy(error));

    this.leftIterator.on('readable', () => {
      this.readable = true;
    });
    this.rightIterator.on('readable', () => {
      this.readable = true;
    });

    this.leftIterator.on('end', () => {
      if (!this.hasResults()) {
        this._end();
      }
    });
    this.rightIterator.on('end', () => {
      if (!this.hasResults()) {
        this._end();
      }
    });
  }

  protected _cleanup(): void {
    this.leftMemory.clear();
    this.rightMemory.clear();
  }

  protected override _end(): void {
    super._end();
    this.leftIterator.destroy();
    this.rightIterator.destroy();
  }

  public hasResults(): boolean {
    return !this.leftIterator.ended || !this.rightIterator.ended || (this.bindingBuffer.length > 0);
  }

  public override read(): Bindings | null {
    if (this.ended) {
      return null;
    }

    if (!this.hasResults()) {
      this._end();
    }

    const buffer = this.bindingBuffer.pop();
    if (buffer !== undefined) {
      return buffer;
    }

    let element = this.rightIterator.read();
    if (element) {
      const hash = this.joinHash(element);
      const isAddition = element.getContextEntry(KeysBindings.isAddition) ?? true;
      if (isAddition) {
        let currentCount = this.rightMemory.get(hash);
        if (currentCount === undefined) {
          currentCount = 0;
        }
        currentCount++;
        this.rightMemory.set(hash, currentCount);

        if (currentCount === 1) {
          const matchingBindings = this.leftMemory.get(hash);
          if (matchingBindings !== undefined) {
            for (let matchingBinding of matchingBindings) {
              matchingBinding = matchingBinding.setContextEntry(KeysBindings.isAddition, false);
              this.bindingBuffer.push(matchingBinding);
            }
          }
        }
        return this.read();
      }
      let currentCount = this.rightMemory.get(hash);
      if (currentCount === undefined) {
        return this.read();
      }
      if (currentCount === 1) {
        this.rightMemory.delete(hash);
      } else {
        currentCount--;
        this.rightMemory.set(hash, currentCount);
      }

      const matchingBindings = this.leftMemory.get(hash);
      if (matchingBindings !== undefined) {
        for (const matchingBinding of matchingBindings) {
          this.bindingBuffer.push(matchingBinding);
        }
      }
      return this.read();
    }
    element = this.leftIterator.read();
    if (element) {
      const hash = this.joinHash(element);
      const isAddition = element.getContextEntry(KeysBindings.isAddition) ?? true;
      if (isAddition) {
        let currentArray = this.leftMemory.get(hash);
        if (currentArray === undefined) {
          currentArray = [];
          this.leftMemory.set(hash, currentArray);
        }
        currentArray.push(element);
        if (this.rightMemory.has(hash)) {
          return this.read();
        }
        return element;
      }
      const currentArray = this.leftMemory.get(hash);
      if (currentArray === undefined) {
        return this.read();
      }
      if (currentArray.length === 1) {
        this.leftMemory.delete(hash);
      } else {
        for (let i = 0; i < currentArray.length; i++) {
          if (currentArray[i].equals(element)) {
            currentArray[i] = currentArray.at(-1)!;
            currentArray.pop();
            break;
          }
        }
      }
      if (this.rightMemory.has(hash)) {
        return this.read();
      }
      return element;
    }
    this.readable = false;
    return null;
  }
}
