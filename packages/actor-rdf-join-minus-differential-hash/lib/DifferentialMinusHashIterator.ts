import {AsyncIterator} from "asynciterator";
import {Bindings, BindingsStream} from "@comunica/types";
import * as RDF from "@rdfjs/types";
import {BindingsFactory, Bindings as bindingsFactoryBindings} from "@comunica/bindings-factory";

export class DifferentialMinusHashIterator extends AsyncIterator<Bindings> {
  private left: BindingsStream;
  private right: BindingsStream;
  private commonVariables: RDF.Variable[];
  private hashFunc: (bindings: Bindings, variables: RDF.Variable[]) => string;

  private leftMap: Map<string, Bindings[]> | null = new Map<string, Bindings[]>();
  private rightMap: Map<string, number> | null = new Map<string, number>();
  private bindingBuffer: Bindings[] = [];
  private bindingsFactory = new BindingsFactory();

  constructor(
    left: BindingsStream,
    right: BindingsStream,
    commonVariables: RDF.Variable[],
    hashFunc: (bindings: Bindings, variables: RDF.Variable[]) => string
    ) {
    super();

    this.left = left;
    this.right = right;
    this.commonVariables = commonVariables;
    this.hashFunc = hashFunc;

    this.on('end', () => this._cleanup() );


    if (this.left.readable || this.right.readable)
    {
      this.readable = true;
    }

    this.left.on('error', (error: any) => this.destroy(error));
    this.right.on('error', (error: any) => this.destroy(error));

    this.left.on('readable', () => this.readable = true);
    this.right.on('readable', () => this.readable = true);

    // this needs to be here since it's possible the left/right streams only get ended after there are no more results left
    this.left.on ('end', () => { if (!this.hasResults()) this._end(); });
    this.right.on('end', () => { if (!this.hasResults()) this._end(); });

  }

  _cleanup ()
  {
    // motivate garbage collector to remove these
    this.leftMap = null;
    this.rightMap = null;
  }

  _end ()
  {
    super._end();
    this.left.destroy();
    this.right.destroy();
  }

  hasResults()
  {
    return !this.left.ended  || !this.right.ended || (this.bindingBuffer.length > 0);
  }

  read (): Bindings | null
  {
    if (this.ended)
      return null;

    if (!this.hasResults())
      this._end();

    if (this.done)
    {
      this.readable = false;
      return null;
    }

    let buffer = this.bindingBuffer.pop();
    if (buffer != undefined) {
      return buffer;
    }

    let element = this.right.read();
    if (element != null) {
      const hash = this.hashFunc(element, this.commonVariables);
      if (element.diff) {
        let currentCount = this.rightMap?.get(hash);
        if (currentCount == undefined) {
          currentCount = 0;
        }
        currentCount++;
        this.rightMap?.set(hash, currentCount);

        if (currentCount == 1) {
          let matchingBindings = this.leftMap?.get(hash);
          if (matchingBindings != null) {
            for (let matchingBinding of matchingBindings) {
              matchingBinding = <Bindings><any>this.bindingsFactory.fromBindings(<bindingsFactoryBindings><any>matchingBinding);
              matchingBinding.diff = false;
              this.bindingBuffer.push(matchingBinding);
            }
          }
        }
        return this.read();
      }
      else {
        let currentCount = this.rightMap?.get(hash);
        if (currentCount == undefined) {
          return this.read();
        }
        if (currentCount == 1) {
          this.rightMap?.delete(hash);
        }
        else {
          currentCount--;
          this.rightMap?.set(hash, currentCount);
        }

        let matchingBindings = this.leftMap?.get(hash);
        if (matchingBindings != null) {
          for (let matchingBinding of matchingBindings) {
            this.bindingBuffer.push(matchingBinding);
          }
        }
        return this.read();
      }
    }
    element = this.left.read();
    if (element != null) {
      const hash = this.hashFunc(element, this.commonVariables);
      if (element.diff) {
        let currentArray = this.leftMap?.get(hash);
        if (currentArray == undefined) {
          currentArray = [];
        }
        currentArray.push(element);
        this.leftMap?.set(hash, currentArray);
        if (this.rightMap?.has(hash)) {
          return this.read();
        }
        return element;
      }
      else {
        let currentArray = this.leftMap?.get(hash);
        if (currentArray == undefined) {
          return this.read();
        }
        if (currentArray.length == 1) {
          this.leftMap?.delete(hash);
        }
        else {
          for (let i = 0; i < currentArray.length; i++) {
            if (currentArray[i].equals(element)) {
              currentArray[i] = currentArray[currentArray.length - 1];
              currentArray.pop();
              break;
            }
          }
        }
        if (this.rightMap?.has(hash)) {
          return this.read();
        }
        return element;
      }
    }
    this.readable = false;
    return null;
  }
}
