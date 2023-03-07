import {Bindings, BindingsStream} from "@comunica/types";

let AsyncIterator = require('asynciterator').AsyncIterator;

export class SymmetricHashJoin extends AsyncIterator
{
  private left: BindingsStream;
  private right: BindingsStream;

  private readonly funHash: (bindings: Bindings) => string;
  private readonly funJoin: (...bindings: Bindings[]) => Bindings | null;

  private usedLeft = false;
  private leftMap: Map<string, Map<string, Bindings>> | null = new Map<string, Map<string, Bindings>>();
  private rightMap: Map<string, Map<string, Bindings>> | null = new Map<string, Map<string, Bindings>>();

  private match: Bindings | null = null;
  private matches: Bindings[] | null = [];
  private matchIdx = 0;

  private readonly emptyMap = new Map();

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
    this.leftMap = null;
    this.rightMap = null;
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
    //TODO pass through the diff
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
      if (result !== null)
        return result;
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

    if (this.leftMap == null || this.rightMap == null) {
      return null;
    }

    let hash = this.funHash(item);
    let map = this.usedLeft ? this.leftMap : this.rightMap;
    let bindingMap = map.get(hash);
    if (item.diff) {
      if (bindingMap == undefined) {
        bindingMap = new Map<string, Bindings>().set(this.hashBindings(item), item);
        map.set(hash, bindingMap);
      }
      else
        bindingMap.set(this.hashBindings(item), item);
    } else {
      if (bindingMap == undefined) {
        return this.read(); //Is this right?1
      }
      if (bindingMap.size == 1) {
        map.delete(hash);
      }
      else
        bindingMap.delete(this.hashBindings(item));
    }

    this.match = item;
    //TODO find another way of doing this
    let arr = [];
    for (const binding of ((this.usedLeft ? this.rightMap : this.leftMap).get(hash) || this.emptyMap).values()) {
      arr.push(binding);
    }
    this.matches = arr || [];
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
