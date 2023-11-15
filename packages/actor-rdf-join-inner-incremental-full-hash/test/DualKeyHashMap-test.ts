import '@incremunica/incremental-jest';
import {DualKeyHashMap} from "../lib/DualKeyHashMap";

class TestObject {
  constructor(public readonly id: number) {}

  equals(item: TestObject): boolean {
    return this.id === item.id;
  }
}

describe('DualKeyHashMap with equality checks', () => {
  let map: DualKeyHashMap<{ num: number, str: string, equals: (item: { num: number, str: string }) => boolean }>;

  beforeEach(() => {
    map = new DualKeyHashMap<{ num: number, str: string, equals: (item: { num: number, str: string }) => boolean }>();
  });

  it('sets and gets values with equality checks', () => {
    const item1 = {num: 1, str: 'foo', equals: (other: { num: number; str: string; }) => other.num === 1 && other.str === 'foo'};
    const item2 = {num: 2, str: 'bar', equals: (other: { num: number; str: string; }) => other.num === 2 && other.str === 'bar'};

    map.set('key1', 'secondaryKey1', item1);
    expect(map.get('key1', 'secondaryKey1')).toEqual({value: item1, count: 1});
    expect(map.get('key1', 'secondaryKey2')).toBeUndefined();

    map.set('key1', 'secondaryKey1', item1);
    expect(map.get('key1', 'secondaryKey1')).toEqual({value: item1, count: 2});

    map.set('key1', 'secondaryKey2', item2);
    expect(map.get('key1', 'secondaryKey2')).toEqual({value: item2, count: 1});

    expect([...map.getAll('secondaryKey1')]).toEqual([{value: item1, count: 2}]);
  });

  it('deletes values with equality checks', () => {
    const item1 = {num: 1, str: 'foo', equals: (other: { num: number; str: string; }) => other.num === 1 && other.str === 'foo'};
    const item2 = {num: 2, str: 'bar', equals: (other: { num: number; str: string; }) => other.num === 2 && other.str === 'bar'};

    map.set('key1', 'secondaryKey1', item1);
    map.set('key1', 'secondaryKey2', item2);
    map.set('key2', 'secondaryKey1', item1);
    map.set('key2', 'secondaryKey2', item2);

    expect(map.delete('key1', 'secondaryKey1')).toBe(true);
    expect(map.has('secondaryKey1')).toBe(true);
    expect(map.get('key1', 'secondaryKey1')).toBeUndefined();
    expect(map.get('key2', 'secondaryKey1')).toEqual({value: item1, count: 1});

    expect(map.delete('key1', 'secondaryKey1')).toBe(false);
    expect(map.get('key1', 'secondaryKey2')).toEqual({value: item2, count: 1});
    expect(map.get('key2', 'secondaryKey2')).toEqual({value: item2, count: 1});

    expect(map.delete('key1', 'secondaryKey2')).toBe(true);
    expect(map.has('secondaryKey2')).toBe(true);
    expect(map.get('key1', 'secondaryKey2')).toBeUndefined();
    expect([...map.getAll('secondaryKey2')]).toEqual([{value: item2, count: 1}]);
  });
});
describe('DualKeyHashMap', () => {
  let map: DualKeyHashMap<TestObject>;

  beforeEach(() => {
    map = new DualKeyHashMap<TestObject>();
  });

  it('sets and gets a value', () => {
    const value = new TestObject(10);
    map.set('key1', 'secondaryKey1', value);
    expect(map.get('key1', 'secondaryKey1')).toEqual({value, count: 1});
  });

  it('increments count when setting the same value', () => {
    const value = new TestObject(10);
    map.set('key1', 'secondaryKey1', value);
    map.set('key1', 'secondaryKey1', value);
    expect(map.get('key1', 'secondaryKey1')).toEqual({value, count: 2});
  });

  it('throws error when setting different value with same key', () => {
    const value1 = new TestObject(10);
    const value2 = new TestObject(20);
    expect(() => {
      map.set('key1', 'secondaryKey1', value1);
      map.set('key1', 'secondaryKey1', value2);
    }).toThrowError(`Current value: ${JSON.stringify(value1)} and given value: ${JSON.stringify(value2)} are different. With hash functions mainKey: key1, secondary key: secondaryKey1!`);
  });

  it('returns undefined when getting non-existing value', () => {
    expect(map.get('key1', 'secondaryKey1')).toBeUndefined();
  });

  it('deletes value when count is greater than 1', () => {
    const value = new TestObject(10);
    map.set('key1', 'secondaryKey1', value);
    map.set('key1', 'secondaryKey1', value);
    expect(map.delete('key1', 'secondaryKey1')).toBe(true);
    expect(map.get('key1', 'secondaryKey1')).toEqual({value, count: 1});
  });

  it('deletes value when count is equal to 1', () => {
    const value = new TestObject(10);
    map.set('key1', 'secondaryKey1', value);
    expect(map.delete('key1', 'secondaryKey1')).toBe(true);
    expect(map.get('key1', 'secondaryKey1')).toBeUndefined();
  });

  it('returns false when deleting non-existing value', () => {
    expect(map.delete('key1', 'secondaryKey1')).toBe(false);
  });

  it('returns an iterator with all values of a secondary key', () => {
    const value1 = new TestObject(10);
    const value2 = new TestObject(20);
    map.set('key1', 'secondaryKey1', value1);
    map.set('key2', 'secondaryKey1', value2);
    expect([...map.getAll('secondaryKey1')]).toEqual([{value: value1, count: 1}, {value: value2, count: 1}]);
  });

  it('returns true when the secondary key exists', () => {
    map.set('key1', 'secondaryKey1', new TestObject(10));
    expect(map.has('secondaryKey1')).toBe(true);
  });

  it('returns false when the secondary key does not exist', () => {
    expect(map.has('secondaryKey1')).toBe(false);
  });

  it('returns an empty iterator;', () => {
    expect([...map.getAll('secondaryKey1')]).toEqual([]);
  });

  it('should clear', () => {
    const value1 = new TestObject(10);
    const value2 = new TestObject(20);
    map.set('key1', 'secondaryKey1', value1);
    map.set('key2', 'secondaryKey1', value2);
    map.clear()
    expect([...map.getAll('secondaryKey1')]).toEqual([]);
  });
});
