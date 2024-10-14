import { HashBindings } from '@incremunica/hash-bindings';
import type { Bindings } from '@comunica/bindings-factory';
import type { BindingsStream } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import { AsyncIterator } from 'asynciterator';
import {ActionContextKeyIsAddition} from "@incremunica/actor-merge-bindings-context-is-addition";

export class IncrementalMinusHash extends AsyncIterator<Bindings> {
  private readonly leftIterator: BindingsStream;
  private readonly rightIterator: BindingsStream;
  private readonly hashBindings: HashBindings;
  private readonly leftMemory: Map<string, Bindings[]> = new Map<string, Bindings[]>();
  private readonly rightMemory: Map<string, number> = new Map<string, number>();
  private readonly bindingBuffer: Bindings[] = [];
  public constructor(
    leftIterator: BindingsStream,
    rightIterator: BindingsStream,
    commonVariables: RDF.Variable[],
  ) {
    super();

    this.leftIterator = leftIterator;
    this.rightIterator = rightIterator;
    this.hashBindings = new HashBindings(commonVariables);

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

  protected _end(): void {
    super._end();
    this.leftIterator.destroy();
    this.rightIterator.destroy();
  }

  public hasResults(): boolean {
    return !this.leftIterator.ended || !this.rightIterator.ended || (this.bindingBuffer.length > 0);
  }

  public read(): RDF.Bindings | null {
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

    let element = <Bindings>this.rightIterator.read();
    if (element) {
      const hash = this.hashBindings.hash(element);
      if (element.getContextEntry(new ActionContextKeyIsAddition())) {
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
              //TODO check if the 2 bindings are equal for common variables
              matchingBinding = matchingBinding.setContextEntry(new ActionContextKeyIsAddition(), false);
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
    element = <Bindings>this.leftIterator.read();
    if (element) {
      const hash = this.hashBindings.hash(element);
      if (element.getContextEntry(new ActionContextKeyIsAddition())) {
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
            currentArray[i] = currentArray[currentArray.length - 1];
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
