import '@incremunica/incremental-jest';
import { DualKeyHashMap } from '../lib/DualKeyHashMap';

class TestObject {
  public constructor(public readonly id: number) {}

  public equals(item: TestObject): boolean {
    return this.id === item.id;
  }
}

describe('DualKeyHashMap', () => {
  let map: DualKeyHashMap<TestObject>;

  beforeEach(() => {
    map = new DualKeyHashMap<TestObject>();
  });

  it('sets and gets a value', () => {
    const value = new TestObject(10);
    map.set(0, 0, value);
    expect(map.get(0, 0)).toEqual({ value, count: 1 });
  });

  it('increments count when setting the same value', () => {
    const value = new TestObject(10);
    map.set(0, 0, value);
    map.set(0, 0, value);
    expect(map.get(0, 0)).toEqual({ value, count: 2 });
  });

  it('throws error when setting different value with same key', () => {
    const value1 = new TestObject(10);
    const value2 = new TestObject(20);
    expect(() => {
      map.set(0, 0, value1);
      map.set(0, 0, value2);
    }).toThrow(`Current value: ${JSON.stringify(value1)} and given value: ${JSON.stringify(value2)} are different. With hash functions mainKey: 0, secondary key: 0!`);
  });

  it('returns undefined when getting non-existing value', () => {
    expect(map.get(0, 0)).toBeUndefined();
  });

  it('deletes value when count is greater than 1', () => {
    const value = new TestObject(10);
    map.set(0, 0, value);
    map.set(0, 0, value);
    expect(map.delete(0, 0)).toBe(true);
    expect(map.get(0, 0)).toEqual({ value, count: 1 });
  });

  it('deletes value when count is equal to 1', () => {
    const value = new TestObject(10);
    map.set(0, 0, value);
    expect(map.delete(0, 0)).toBe(true);
    expect(map.get(0, 0)).toBeUndefined();
  });

  it('returns false when deleting non-existing value', () => {
    expect(map.delete(0, 0)).toBe(false);
  });

  it('returns an iterator with all values of a secondary key', () => {
    const value1 = new TestObject(10);
    const value2 = new TestObject(20);
    map.set(0, 0, value1);
    map.set(1, 0, value2);
    expect([ ...<any>map.getAll(0) ]).toEqual([{ value: value1, count: 1 }, { value: value2, count: 1 }]);
  });

  it('returns true when the secondary key exists', () => {
    map.set(0, 0, new TestObject(10));
    expect(map.has(0)).toBe(true);
  });

  it('returns false when the secondary key does not exist', () => {
    expect(map.has(0)).toBe(false);
  });

  it('returns an empty iterator;', () => {
    expect([ ...<any>map.getAll(0) ]).toEqual([]);
  });

  it('should clear', () => {
    const value1 = new TestObject(10);
    const value2 = new TestObject(20);
    map.set(0, 0, value1);
    map.set(1, 0, value2);
    map.clear();
    expect([ ...<any>map.getAll(0) ]).toEqual([]);
  });

  describe('DualKeyHashMap with equality checks', () => {
    let map: DualKeyHashMap<{ num: number; str: string; equals: (item: { num: number; str: string }) => boolean }>;

    beforeEach(() => {
      map = new DualKeyHashMap<{ num: number; str: string; equals: (item: { num: number; str: string }) => boolean }>();
    });

    it('sets and gets values with equality checks', () => {
      const item1 = {
        num: 1,
        str: 'foo',
        equals: (other: { num: number; str: string }) => other.num === 1 && other.str === 'foo',
      };
      const item2 = {
        num: 2,
        str: 'bar',
        equals: (other: { num: number; str: string }) => other.num === 2 && other.str === 'bar',
      };

      map.set(0, 0, item1);
      expect(map.get(0, 0)).toEqual({ value: item1, count: 1 });
      expect(map.get(0, 1)).toBeUndefined();

      map.set(0, 0, item1);
      expect(map.get(0, 0)).toEqual({ value: item1, count: 2 });

      map.set(0, 1, item2);
      expect(map.get(0, 1)).toEqual({ value: item2, count: 1 });

      expect([ ...<any>map.getAll(0) ]).toEqual([{ value: item1, count: 2 }]);
    });

    it('deletes values with equality checks', () => {
      const item1 = {
        num: 1,
        str: 'foo',
        equals: (other: { num: number; str: string }) => other.num === 1 && other.str === 'foo',
      };
      const item2 = {
        num: 2,
        str: 'bar',
        equals: (other: { num: number; str: string }) => other.num === 2 && other.str === 'bar',
      };

      map.set(0, 0, item1);
      map.set(0, 1, item2);
      map.set(1, 0, item1);
      map.set(1, 1, item2);

      expect(map.delete(0, 0)).toBe(true);
      expect(map.has(0)).toBe(true);
      expect(map.get(0, 0)).toBeUndefined();
      expect(map.get(1, 0)).toEqual({ value: item1, count: 1 });

      expect(map.delete(0, 0)).toBe(false);
      expect(map.get(0, 1)).toEqual({ value: item2, count: 1 });
      expect(map.get(1, 1)).toEqual({ value: item2, count: 1 });

      expect(map.delete(0, 1)).toBe(true);
      expect(map.has(1)).toBe(true);
      expect(map.get(0, 1)).toBeUndefined();
      expect([ ...<any>map.getAll(1) ]).toEqual([{ value: item2, count: 1 }]);
    });
  });
});
