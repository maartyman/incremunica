export interface IMapObject<T> {
  value: T;
  count: number;
}

export class DualKeyHashMap<O extends { equals: (item: O) => boolean }> {
  private readonly map = new Map<string, Map<string, IMapObject<O>>>();

  public set(mainKey: string, secondaryKey: string, value: O): void {
    const mainMap = this.map.get(secondaryKey);
    if (mainMap) {
      const mapObject = mainMap.get(mainKey);
      if (mapObject) {
        if (value.equals(mapObject.value)) {
          mapObject.count++;
        } else {
          throw new Error(`Current value: ${JSON.stringify(mapObject.value)} and given value: ${JSON.stringify(value)} are different. With hash functions mainKey: ${mainKey}, secondary key: ${secondaryKey}!`);
        }
      } else {
        mainMap.set(mainKey, { value, count: 1 });
      }
    } else {
      const newMap = new Map<string, IMapObject<O>>();
      newMap.set(mainKey, { value, count: 1 });
      this.map.set(secondaryKey, newMap);
    }
  }

  public delete(mainKey: string, secondaryKey: string): boolean {
    const mainMap = this.map.get(secondaryKey);
    if (mainMap) {
      const mapObject = mainMap.get(mainKey);
      if (mapObject) {
        if (mapObject.count > 1) {
          mapObject.count--;
          return true;
        }

        mainMap.delete(mainKey);
        if (mainMap.size === 0) {
          this.map.delete(secondaryKey);
        }
        return true;
      }
    }
    return false;
  }

  public get(mainKey: string, secondaryKey: string): IMapObject<O> | undefined {
    return this.map.get(secondaryKey)?.get(mainKey);
  }

  public getAll(secondaryKey: string): IterableIterator<IMapObject<O>> {
    const mainMap = this.map.get(secondaryKey);
    if (mainMap) {
      return mainMap.values();
    }
    return [][Symbol.iterator]();
  }

  public has(secondaryKey: string): boolean {
    return this.map.has(secondaryKey);
  }

  public clear(): void {
    this.map.clear();
  }
}
