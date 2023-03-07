import {Bindings, BindingsStream} from "@comunica/types";
import {AsyncIterator} from "asynciterator";

export class DifferentialSymmetricHashJoin extends AsyncIterator<Bindings>
{
  private left: BindingsStream;
  private right: BindingsStream;

  private readonly funHash: (bindings: Bindings) => string;
  private readonly funJoin: (...bindings: Bindings[]) => Bindings | null;

  private usedLeft = false;
  private leftBindingMap: Map<string, number> | null = new Map<string, number>();
  private leftJoinMap: Map<string, Array<Bindings>> | null = new Map<string, Array<Bindings>>();
  private rightBindingMap: Map<string, number> | null = new Map<string, number>();
  private rightJoinMap: Map<string, Array<Bindings>> | null = new Map<string, Array<Bindings>>();

  private match: Bindings | null = null;
  private matches: Bindings[] | null = [];
  private matchIdx = 0;

  private diff = true;

  constructor (
    left: BindingsStream,
    right: BindingsStream,
    funHash: (bindings: Bindings) => string,
    funJoin: (...bindings: Bindings[]) => Bindings | null
  ) {
    super();

    this.left  = left;
    this.right = right;

    this.funHash = funHash;
    this.funJoin = funJoin;

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

  hasResults()
  {
    // The "!!this.match" condition was added as a workaround to race
    // conditions and/or duplicate "end" events that may lead to premature
    // cleanups of the "this.matches" array.
    // See https://github.com/joachimvh/asyncjoin/issues/7
    return !this.left.ended  || !this.right.ended || (!!this.matches && this.matchIdx < this.matches.length);
  }

  _cleanup ()
  {
    // motivate garbage collector to remove these
    this.leftBindingMap = null;
    this.leftJoinMap = null;
    this.rightJoinMap = null;
    this.rightBindingMap = null;
    this.matches = null;
  }

  _end ()
  {
    super._end();
    this.left.destroy();
    this.right.destroy();
  }

  read (): Bindings | null
  {
    if (this.ended)
      return null;

    if (this.matches == null) {
      return null;
    }

    while (this.matchIdx < this.matches.length)
    {
      let item = this.matches[this.matchIdx++];
      if (this.match == null) break;
      let result = this.usedLeft ? this.funJoin(this.match, item) : this.funJoin(item, this.match);
      if (result !== null) {
        result.diff = this.diff;
        return result;
      }
    }

    if (!this.hasResults())
      this._end();

    let item = null;
    // try both streams if the first one has no value
    for (let i = 0; i < 2; ++i)
    {
      item = this.usedLeft ? this.right.read() : this.left.read();
      this.usedLeft = !this.usedLeft; // try other stream next time

      // found a result, no need to check the other stream this run
      if (item !== null)
        break;
    }

    if (this.done || item === null)
    {
      this.readable = false;
      return null;
    }

    if (this.leftJoinMap == null || this.rightJoinMap == null || this.leftBindingMap == null || this.rightBindingMap == null) {
      return null;
    }

    let joinHash = this.funHash(item);
    let bindingHash = this.hashBindings(item);
    let joinMap = this.usedLeft ? this.leftJoinMap : this.rightJoinMap;
    let bindingMap = this.usedLeft ? this.leftBindingMap : this.rightBindingMap;
    let joinArray = joinMap.get(joinHash);
    //if addition
    if (item.diff || item.diff == undefined) {
      this.diff = true;
      if (joinArray == undefined) {
        //if the no collection for this hash exists make an empty array and set the binding map and the join map
        joinArray = [item];
        bindingMap.set(bindingHash, 0);
        joinMap.set(joinHash, joinArray);
      } else {
        //if a collection for this hash does exist just add the new item to it and save the index and array to the binding map
        bindingMap.set(
          bindingHash,
          joinArray.push(item)
        );
      }
    } else {
      this.diff = false;
      if (joinArray == undefined) {
        //should usually not happen as when a triple gets deleted it already should be in the joinMap
        return this.read();
      }
      if (joinArray.length == 1) {
        joinMap.delete(joinHash);
        bindingMap.delete(bindingHash);
      }
      else {
        let index = bindingMap.get(bindingHash);
        if (index == undefined) {
          return this.read(); //is this right?
        }
        bindingMap.set(this.hashBindings(joinArray[joinArray.length - 1]), index);
        joinArray[index] = joinArray[joinArray.length - 1];
        joinArray.pop();
        bindingMap.delete(bindingHash);
      }
    }

    this.match = item;
    this.matches = (this.usedLeft ? this.rightJoinMap : this.leftJoinMap).get(joinHash) || [];
    this.matchIdx = 0;

    // array is filled again so recursive call can have results
    return this.read();
  }

  private hashBindings(bindings: Bindings): string {
    let hash: string = "";
    for (const binding of bindings) {
      hash += binding[1].value + " ";
    }
    return hash;
  }
}
