import { IndexedSortTree } from '../lib/IndexedSortTree';

function treeToString(tree: any): string {
  if (!tree.root) {
    return 'Tree is empty';
  }
  const out: string[] = [];
  printRow(tree, tree.root, '', true, (v: string) => out.push(v));
  return out.join('');
}

function printRow(tree: any, node: any, prefix: string, isTail: boolean, out: (v: string) => void): void {
  out(`${prefix}${isTail ? '└── ' : '├── '}${printNode(node)}\n`);
  const indent = prefix + (isTail ? '    ' : '│   ');
  if (node.left) {
    printRow(tree, node.left, indent, false, out);
  }
  if (node.right) {
    printRow(tree, node.right, indent, true, out);
  }
}

function printNode(node: any): string {
  return `${node.data.result.map((el: any) => el ?? 'undefined')} => ${node.data.hash}(i:${node.data.index})`;
}

describe('IndexedSortTree', () => {
  let tree: IndexedSortTree;

  beforeEach(() => {
    tree = new IndexedSortTree(
      <any>{ orderTypes: (a: number, b: number) => {
        if (a === undefined && b === undefined) {
          return 0;
        }
        if (a === undefined) {
          return -1;
        }
        if (b === undefined) {
          return 1;
        }
        return a - b;
      } },
      [ true ],
    );
  });

  it('should insert and index a single element', () => {
    const node = tree.insert({ hash: 'a', result: <any>[ 1 ]});
    expect(node.data.index).toBe(0);
  });

  it('should insert and index multiple elements in order', () => {
    tree.insert({ hash: 'a', result: <any>[ 1 ]});
    tree.insert({ hash: 'b', result: <any>[ 2 ]});
    tree.insert({ hash: 'c', result: <any>[ 3 ]});
    expect(treeToString(tree)).toContain('1 => a(i:0)');
    expect(treeToString(tree)).toContain('2 => b(i:1)');
    expect(treeToString(tree)).toContain('3 => c(i:2)');
  });

  it('should insert and index multiple elements out of order', () => {
    tree.insert({ hash: 'c', result: <any>[ 3 ]});
    tree.insert({ hash: 'a', result: <any>[ 1 ]});
    tree.insert({ hash: 'b', result: <any>[ 2 ]});
    expect(treeToString(tree)).toContain('1 => a(i:0)');
    expect(treeToString(tree)).toContain('2 => b(i:1)');
    expect(treeToString(tree)).toContain('3 => c(i:2)');
  });

  it('should handle duplicate hashes with different results', () => {
    tree.insert({ hash: 'a', result: <any>[ 1 ]});
    tree.insert({ hash: 'a', result: <any>[ 2 ]});
    expect(treeToString(tree)).toContain('1 => a(i:0)');
    expect(treeToString(tree)).toContain('2 => a(i:1)');
  });

  it('should handle duplicate results with different hashes', () => {
    tree.insert({ hash: 'a', result: <any>[ 1 ]});
    tree.insert({ hash: 'b', result: <any>[ 1 ]});
    expect(treeToString(tree)).toContain('1 => a(i:0)');
    expect(treeToString(tree)).toContain('1 => b(i:1)');
  });

  it('should handle insertion and deletion of the root node', () => {
    const node = tree.insert({ hash: 'a', result: <any>[ 1 ]});
    expect(tree.remove({ hash: 'a', result: <any>[ 1 ]})).toEqual(node);
    expect(treeToString(tree)).toBe('Tree is empty');
  });

  it('should handle deletion of a leaf node', () => {
    tree.insert({ hash: 'a', result: <any>[ 1 ]});
    tree.insert({ hash: 'b', result: <any>[ 2 ]});
    tree.remove({ hash: 'b', result: <any>[ 2 ]});
    expect(treeToString(tree)).toContain('1 => a(i:0)');
    expect(treeToString(tree)).not.toContain('2 => b(i:1)');
  });

  it('should handle deletion of a node with one child', () => {
    tree.insert({ hash: 'b', result: <any>[ 2 ]});
    tree.insert({ hash: 'a', result: <any>[ 1 ]});
    tree.remove({ hash: 'b', result: <any>[ 2 ]});
    expect(treeToString(tree)).toContain('1 => a(i:0)');
    expect(treeToString(tree)).not.toContain('2 => b(i:1)');
  });

  it('should handle deletion of a node with two children', () => {
    tree.insert({ hash: 'b', result: <any>[ 2 ]});
    tree.insert({ hash: 'a', result: <any>[ 1 ]});
    tree.insert({ hash: 'c', result: <any>[ 3 ]});
    tree.remove({ hash: 'b', result: <any>[ 2 ]});
    expect(treeToString(tree)).toContain('1 => a(i:0)');
    expect(treeToString(tree)).toContain('3 => c(i:1)');
    expect(treeToString(tree)).not.toContain('2 => b(i:1)');
  });

  it('should re-index after deletion', () => {
    tree.insert({ hash: 'a', result: <any>[ 1 ]});
    tree.insert({ hash: 'b', result: <any>[ 2 ]});
    tree.insert({ hash: 'c', result: <any>[ 3 ]});
    tree.remove({ hash: 'a', result: <any>[ 1 ]});
    expect(treeToString(tree)).toContain('2 => b(i:0)');
    expect(treeToString(tree)).toContain('3 => c(i:1)');
  });

  it('should insert and remove multiple items', () => {
    const items = [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' ];
    for (const [ index, item ] of items.entries()) {
      tree.insert({ hash: item, result: <any>[ index ]});
    }
    expect(treeToString(tree)).toContain('5 => f(i:5)');
    tree.remove({ hash: 'f', result: <any>[ 5 ]});
    expect(treeToString(tree)).not.toContain('5 => f(i:5)');
    expect(treeToString(tree)).toContain('6 => g(i:5)');
  });

  it('should insert and remove multiple items 2', () => {
    const items = [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' ];
    for (const [ index, item ] of items.entries()) {
      tree.insert({ hash: item, result: <any>[ index ]});
    }
    expect(treeToString(tree)).toContain('0 => a(i:0)');
    tree.remove({ hash: 'a', result: <any>[ 0 ]});
    expect(treeToString(tree)).not.toContain('0 => a(i:0)');
    expect(treeToString(tree)).toContain('1 => b(i:0)');
  });

  it('should insert and remove multiple items 3', () => {
    const items = [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' ];
    for (const [ index, item ] of items.entries()) {
      tree.insert({ hash: item, result: <any>[ index ]});
    }
    expect(treeToString(tree)).toContain('9 => j(i:9)');
    tree.remove({ hash: 'j', result: <any>[ 9 ]});
    expect(treeToString(tree)).not.toContain('9 => j(i:9)');
    expect(treeToString(tree)).toContain('8 => i(i:8)');
  });

  it('should insert and remove multiple items 4', () => {
    const items = [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' ];
    for (const [ index, item ] of items.entries()) {
      tree.insert({ hash: item, result: <any>[ index ]});
    }
    tree.remove({ hash: 'a', result: <any>[ 0 ]});
    tree.remove({ hash: 'j', result: <any>[ 9 ]});
    tree.remove({ hash: 'f', result: <any>[ 5 ]});
    expect(treeToString(tree)).not.toContain('0 => a(i:0)');
    expect(treeToString(tree)).not.toContain('9 => j(i:9)');
    expect(treeToString(tree)).not.toContain('5 => f(i:5)');
    expect(treeToString(tree)).toContain('1 => b(i:0)');
    expect(treeToString(tree)).toContain('2 => c(i:1)');
    expect(treeToString(tree)).toContain('3 => d(i:2)');
    expect(treeToString(tree)).toContain('4 => e(i:3)');
    expect(treeToString(tree)).toContain('6 => g(i:4)');
    expect(treeToString(tree)).toContain('7 => h(i:5)');
    expect(treeToString(tree)).toContain('8 => i(i:6)');
  });

  it('should insert and remove multiple items out of order', () => {
    const items = [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' ];
    for (const [ index, item ] of items.entries()) {
      tree.insert({ hash: item, result: <any>[ index ]});
    }
    tree.remove({ hash: 'g', result: <any>[ 6 ]});
    tree.remove({ hash: 'c', result: <any>[ 2 ]});
    tree.remove({ hash: 'a', result: <any>[ 0 ]});
    expect(treeToString(tree)).not.toContain('6 => g(i:');
    expect(treeToString(tree)).not.toContain('2 => c(i:');
    expect(treeToString(tree)).not.toContain('0 => a(i:');
    expect(treeToString(tree)).toContain('1 => b(i:0)');
    expect(treeToString(tree)).toContain('3 => d(i:1)');
    expect(treeToString(tree)).toContain('4 => e(i:2)');
    expect(treeToString(tree)).toContain('5 => f(i:3)');
    expect(treeToString(tree)).toContain('7 => h(i:4)');
    expect(treeToString(tree)).toContain('8 => i(i:5)');
    expect(treeToString(tree)).toContain('9 => j(i:6)');
  });

  it('should insert and remove all items', () => {
    const items = [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' ];
    for (const [ index, item ] of items.entries()) {
      tree.insert({ hash: item, result: <any>[ index ]});
    }
    for (const [ index, item ] of items.entries()) {
      tree.remove({ hash: item, result: <any>[ index ]});
    }
    expect(treeToString(tree)).toBe('Tree is empty');
  });

  it('should handle complex insertion and deletion sequence', () => {
    const items = [ 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j' ];
    for (const [ index, item ] of items.entries()) {
      tree.insert({ hash: item, result: <any>[ index ]});
    }

    tree.remove({ hash: 'c', result: <any>[ 2 ]});
    tree.remove({ hash: 'i', result: <any>[ 8 ]});
    tree.insert({ hash: 'k', result: <any>[ 10 ]});
    tree.remove({ hash: 'b', result: <any>[ 1 ]});
    tree.insert({ hash: 'l', result: <any>[ 11 ]});
    tree.remove({ hash: 'f', result: <any>[ 5 ]});

    expect(treeToString(tree)).not.toContain('2 => c(i:');
    expect(treeToString(tree)).not.toContain('8 => i(i:');
    expect(treeToString(tree)).not.toContain('1 => b(i:');
    expect(treeToString(tree)).not.toContain('5 => f(i:');

    expect(treeToString(tree)).toContain('0 => a(i:0)');
    expect(treeToString(tree)).toContain('3 => d(i:1)');
    expect(treeToString(tree)).toContain('4 => e(i:2)');
    expect(treeToString(tree)).toContain('6 => g(i:3)');
    expect(treeToString(tree)).toContain('7 => h(i:4)');
    expect(treeToString(tree)).toContain('9 => j(i:5)');
    expect(treeToString(tree)).toContain('10 => k(i:6)');
    expect(treeToString(tree)).toContain('11 => l(i:7)');
  });

  describe('Large Number Inserts and Deletes', () => {
    it('should handle large number of inserts and deletes', () => {
      const numItems = 1000;
      for (let i = 0; i < numItems; i++) {
        tree.insert({ hash: `item${i}`, result: <any>[ i ]});
      }

      for (let i = 0; i < numItems; i += 2) {
        tree.remove({ hash: `item${i}`, result: <any>[ i ]});
      }

      for (let i = 1; i < numItems; i += 2) {
        expect(treeToString(tree)).toContain(`${i} => item${i}(i:${(i - 1) / 2}`);
      }

      for (let i = 1; i < numItems; i += 2) {
        tree.remove({ hash: `item${i}`, result: <any>[ i ]});
      }
      expect(treeToString(tree)).toBe('Tree is empty');
    });

    it('should handle a large number of out-of-order inserts and deletes efficiently', () => {
      let seed = 42;
      function deterministicRandom() {
        const x = Math.sin(seed++) * 10000;
        return x - Math.floor(x);
      }

      const numItems = 2000;
      const items = Array.from({ length: numItems }, (_, i) => ({
        hash: `item${i}`,
        result: <any>[ i ],
      }));

      // Deterministic shuffling using Fisher-Yates algorithm
      let arrayToShuffle = [ ...items ]; // Create a copy to shuffle
      let currentIndex = arrayToShuffle.length;
      let randomIndex;

      // While there remain elements to shuffle.
      while (currentIndex !== 0) {
        // Pick a remaining element.
        randomIndex = Math.floor(deterministicRandom() * currentIndex);
        currentIndex--;

        // And swap it with the current element.
        [ arrayToShuffle[currentIndex], arrayToShuffle[randomIndex] ] = [
          arrayToShuffle[randomIndex],
          arrayToShuffle[currentIndex],
        ];
      }

      const shuffledItems = arrayToShuffle;
      for (const item of shuffledItems) {
        tree.insert(item);
      }

      // Deterministic deletion (using same shuffling logic)
      arrayToShuffle = [ ...items ]; // Create a copy to shuffle
      currentIndex = arrayToShuffle.length;
      const itemsToDelete = new Set<number>();
      const numToDelete = numItems / 2;
      while (itemsToDelete.size < numToDelete && currentIndex !== 0) {
        randomIndex = Math.floor(deterministicRandom() * currentIndex);
        currentIndex--;
        itemsToDelete.add(arrayToShuffle[randomIndex].result);
      }
      const itemsNotDeleted = [];
      for (const item of shuffledItems) {
        if (!itemsToDelete.has(item.result)) {
          itemsNotDeleted.push(item);
        }
      }

      for (const index of itemsToDelete) {
        tree.remove(items[index]);
      }

      for (const item of itemsNotDeleted) {
        expect(treeToString(tree)).toContain(`${item.result} => ${item.hash}(i:`);
      }
      for (const index of itemsToDelete) {
        expect(treeToString(tree)).not.toContain(`${index} => item${index}(i:`);
      }

      // Delete all remaining items
      for (const item of itemsNotDeleted) {
        tree.remove(item);
      }

      expect(treeToString(tree)).toBe('Tree is empty');
    });
  });

  describe('isAscending option', () => {
    it('should correctly order in ascending order', () => {
      tree = new IndexedSortTree(
        <any>{ orderTypes: (a: number, b: number) => a - b },
        [ true ],
      );
      tree.insert({ hash: 'c', result: <any>[ 3 ]});
      tree.insert({ hash: 'a', result: <any>[ 1 ]});
      tree.insert({ hash: 'b', result: <any>[ 2 ]});
      expect(treeToString(tree)).toContain('1 => a(i:0)');
      expect(treeToString(tree)).toContain('2 => b(i:1)');
      expect(treeToString(tree)).toContain('3 => c(i:2)');
    });

    it('should correctly order in descending order', () => {
      tree = new IndexedSortTree(
        <any>{ orderTypes: (a: number, b: number) => a - b },
        [ false ],
      );
      tree.insert({ hash: 'c', result: <any>[ 3 ]});
      tree.insert({ hash: 'a', result: <any>[ 1 ]});
      tree.insert({ hash: 'b', result: <any>[ 2 ]});
      expect(treeToString(tree)).toContain('3 => c(i:0)');
      expect(treeToString(tree)).toContain('2 => b(i:1)');
      expect(treeToString(tree)).toContain('1 => a(i:2)');
    });
  });

  describe('Duplicate Handling', () => {
    it('should handle insertion of duplicate results with different hashes and maintain correct indexing', () => {
      tree.insert({ hash: 'a1', result: <any>[ 1 ]});
      tree.insert({ hash: 'b1', result: <any>[ 1 ]});
      tree.insert({ hash: 'a2', result: <any>[ 1 ]});
      tree.insert({ hash: 'b2', result: <any>[ 1 ]});
      expect(treeToString(tree)).toContain('1 => a1(i:0)');
      expect(treeToString(tree)).toContain('1 => b1(i:2)');
      expect(treeToString(tree)).toContain('1 => a2(i:1)');
      expect(treeToString(tree)).toContain('1 => b2(i:3)');
    });

    it('should handle deletion of one duplicate result correctly', () => {
      tree.insert({ hash: 'a1', result: <any>[ 1 ]});
      tree.insert({ hash: 'b1', result: <any>[ 1 ]});
      tree.insert({ hash: 'a2', result: <any>[ 1 ]});
      tree.remove({ hash: 'b1', result: <any>[ 1 ]});
      expect(treeToString(tree)).toContain('1 => a1(i:0)');
      expect(treeToString(tree)).toContain('1 => a2(i:1)');
    });

    it('should handle deletion of all duplicate results correctly', () => {
      tree.insert({ hash: 'a1', result: <any>[ 1 ]});
      tree.insert({ hash: 'b1', result: <any>[ 1 ]});
      tree.insert({ hash: 'a2', result: <any>[ 1 ]});
      tree.remove({ hash: 'a1', result: <any>[ 1 ]});
      tree.remove({ hash: 'b1', result: <any>[ 1 ]});
      tree.remove({ hash: 'a2', result: <any>[ 1 ]});
      expect(treeToString(tree)).toBe('Tree is empty');
    });
  });

  describe('Error Handling', () => {
    it('should throw an error when deleting from an empty tree', () => {
      expect(() => tree.remove({ hash: 'a', result: <any>[ 1 ]})).toThrow('Deletion does not exist');
    });

    it('should throw an error when deleting a non-existent element', () => {
      tree.insert({ hash: 'b', result: <any>[ 2 ]});
      expect(() => tree.remove({ hash: 'a', result: <any>[ 1 ]})).toThrow('Deletion does not exist');
    });
  });

  describe('Edge cases', () => {
    it('should handle inserting the same element multiple times', () => {
      const item = { hash: 'a', result: <any>[ 1 ]};
      tree.insert(item);
      tree.insert(item);
      tree.insert(item);
      expect(treeToString(tree)).toContain('1 => a(i:0)');
    });

    it('should handle inserting and deleting undefined result', () => {
      const item = { hash: 'a', result: [ undefined ]};
      tree.insert(item);
      expect(treeToString(tree)).toContain('undefined => a(i:0)');
      tree.remove(item);
      expect(treeToString(tree)).toBe('Tree is empty');
    });
  });

  describe('Insert and Remove Return Values with Correct Indexing', () => {
    it('should return the inserted node with the correct index', () => {
      const node1 = tree.insert({ hash: 'a', result: <any>[ 1 ]});
      expect(node1.data.index).toBe(0);

      const node2 = tree.insert({ hash: 'c', result: <any>[ 3 ]});
      expect(node2.data.index).toBe(1);

      const node3 = tree.insert({ hash: 'b', result: <any>[ 2 ]});
      expect(node3.data.index).toBe(1);

      const node4 = tree.insert({ hash: 'd', result: <any>[ 4 ]});
      expect(node4.data.index).toBe(3);

      expect(treeToString(tree)).toContain('1 => a(i:0)');
      expect(treeToString(tree)).toContain('2 => b(i:1)');
      expect(treeToString(tree)).toContain('3 => c(i:2)');
      expect(treeToString(tree)).toContain('4 => d(i:3)');
    });

    it('should return the removed node with the correct index (before removal)', () => {
      tree.insert({ hash: 'a', result: <any>[ 1 ]});
      tree.insert({ hash: 'c', result: <any>[ 3 ]});
      tree.insert({ hash: 'b', result: <any>[ 2 ]});
      tree.insert({ hash: 'd', result: <any>[ 4 ]});

      const removedNode = tree.remove({ hash: 'b', result: <any>[ 2 ]});
      expect(removedNode.data.index).toBe(1); // Index before removal

      expect(treeToString(tree)).toContain('1 => a(i:0)');
      expect(treeToString(tree)).toContain('3 => c(i:1)');
      expect(treeToString(tree)).toContain('4 => d(i:2)');
    });

    it('should return the removed node with the correct index (before removal) 2', () => {
      tree.insert({ hash: 'a', result: <any>[ 1 ]});
      tree.insert({ hash: 'b', result: <any>[ 2 ]});
      tree.insert({ hash: 'c', result: <any>[ 3 ]});
      tree.insert({ hash: 'd', result: <any>[ 4 ]});
      tree.insert({ hash: 'e', result: <any>[ 5 ]});
      tree.insert({ hash: 'f', result: <any>[ 6 ]});
      tree.insert({ hash: 'g', result: <any>[ 7 ]});
      tree.insert({ hash: 'h', result: <any>[ 8 ]});
      tree.insert({ hash: 'i', result: <any>[ 9 ]});
      tree.insert({ hash: 'j', result: <any>[ 10 ]});

      const removedNode = tree.remove({ hash: 'f', result: <any>[ 6 ]});
      expect(removedNode.data.index).toBe(5); // Index before removal

      expect(treeToString(tree)).toContain('1 => a(i:0)');
      expect(treeToString(tree)).toContain('2 => b(i:1)');
      expect(treeToString(tree)).toContain('3 => c(i:2)');
      expect(treeToString(tree)).toContain('4 => d(i:3)');
      expect(treeToString(tree)).toContain('5 => e(i:4)');
      expect(treeToString(tree)).toContain('7 => g(i:5)');
      expect(treeToString(tree)).toContain('8 => h(i:6)');
      expect(treeToString(tree)).toContain('9 => i(i:7)');
      expect(treeToString(tree)).toContain('10 => j(i:8)');
    });
  });

  describe('Multiple Results per Node', () => {
    const orderTypesObject = <any>{
      orderTypes: (a: number, b: number) => {
        if (a === undefined && b === undefined) {
          return 0;
        }
        if (a === undefined) {
          return -1;
        }
        if (b === undefined) {
          return 1;
        }
        return a - b;
      },
    };

    beforeEach(() => {
      tree = new IndexedSortTree(
        orderTypesObject,
        [ true, true ],
      );
    });

    it('should handle multiple results per node and order correctly', () => {
      tree.insert({ hash: 'a', result: <any>[ 1, 2 ]});
      tree.insert({ hash: 'b', result: <any>[ 1, 3 ]});
      tree.insert({ hash: 'c', result: <any>[ 2, 1 ]});
      expect(treeToString(tree)).toContain('1,2 => a(i:0)');
      expect(treeToString(tree)).toContain('1,3 => b(i:1)');
      expect(treeToString(tree)).toContain('2,1 => c(i:2)');
    });

    it('should handle multiple results per node and order correctly with out-of-order inserts', () => {
      tree.insert({ hash: 'c', result: <any>[ 2, 1 ]});
      tree.insert({ hash: 'a', result: <any>[ 1, 2 ]});
      tree.insert({ hash: 'b', result: <any>[ 1, 3 ]});
      expect(treeToString(tree)).toContain('1,2 => a(i:0)');
      expect(treeToString(tree)).toContain('1,3 => b(i:1)');
      expect(treeToString(tree)).toContain('2,1 => c(i:2)');
    });

    it('should handle multiple results per node and order correctly with descending on second result', () => {
      tree = new IndexedSortTree(
        orderTypesObject,
        [ true, false ],
      );
      tree.insert({ hash: 'a', result: <any>[ 1, 3 ]});
      tree.insert({ hash: 'b', result: <any>[ 1, 2 ]});
      tree.insert({ hash: 'c', result: <any>[ 2, 1 ]});
      expect(treeToString(tree)).toContain('1,3 => a(i:0)');
      expect(treeToString(tree)).toContain('1,2 => b(i:1)');
      expect(treeToString(tree)).toContain('2,1 => c(i:2)');
    });

    it('should handle multiple results per node and order correctly with descending on first result', () => {
      tree = new IndexedSortTree(
        orderTypesObject,
        [ false, true ],
      );
      tree.insert({ hash: 'a', result: <any>[ 3, 1 ]});
      tree.insert({ hash: 'b', result: <any>[ 2, 2 ]});
      tree.insert({ hash: 'c', result: <any>[ 1, 3 ]});
      expect(treeToString(tree)).toContain('3,1 => a(i:0)');
      expect(treeToString(tree)).toContain('2,2 => b(i:1)');
      expect(treeToString(tree)).toContain('1,3 => c(i:2)');
    });

    it('should handle multiple results per node and order correctly with descending on both results', () => {
      tree = new IndexedSortTree(
        orderTypesObject,
        [ false, false ],
      );
      tree.insert({ hash: 'a', result: <any>[ 3, 3 ]});
      tree.insert({ hash: 'b', result: <any>[ 2, 2 ]});
      tree.insert({ hash: 'c', result: <any>[ 1, 1 ]});
      expect(treeToString(tree)).toContain('3,3 => a(i:0)');
      expect(treeToString(tree)).toContain('2,2 => b(i:1)');
      expect(treeToString(tree)).toContain('1,1 => c(i:2)');
    });

    it('should handle multiple results and different hashes', () => {
      tree = new IndexedSortTree(
        orderTypesObject,
        [ true, true ],
      );
      tree.insert({ hash: 'a1', result: <any>[ 1, 2 ]});
      tree.insert({ hash: 'b2', result: <any>[ 1, 3 ]});
      tree.insert({ hash: 'c3', result: <any>[ 2, 1 ]});
      tree.insert({ hash: 'd4', result: <any>[ 2, 1 ]});
      expect(treeToString(tree)).toContain('1,2 => a1(i:0)');
      expect(treeToString(tree)).toContain('1,3 => b2(i:1)');
      expect(treeToString(tree)).toContain('2,1 => c3(i:2)');
      expect(treeToString(tree)).toContain('2,1 => d4(i:3)');
    });

    it('should handle multiple results, different hashes and deletions', () => {
      tree = new IndexedSortTree(
        orderTypesObject,
        [ true, true ],
      );
      tree.insert({ hash: 'a1', result: <any>[ 1, 2 ]});
      tree.insert({ hash: 'b2', result: <any>[ 1, 3 ]});
      tree.insert({ hash: 'c3', result: <any>[ 2, 1 ]});
      tree.insert({ hash: 'd4', result: <any>[ 2, 1 ]});
      tree.remove({ hash: 'b2', result: <any>[ 1, 3 ]});
      expect(treeToString(tree)).toContain('1,2 => a1(i:0)');
      expect(treeToString(tree)).toContain('2,1 => c3(i:1)');
      expect(treeToString(tree)).toContain('2,1 => d4(i:2)');
      expect(treeToString(tree)).not.toContain('1,3 => b2(i:1)');
    });

    it('should handle multiple results, different hashes, deletions and insertions', () => {
      tree = new IndexedSortTree(
        orderTypesObject,
        [ true, true ],
      );
      tree.insert({ hash: 'a1', result: <any>[ 1, 2 ]});
      tree.insert({ hash: 'b2', result: <any>[ 1, 3 ]});
      tree.insert({ hash: 'c3', result: <any>[ 2, 1 ]});
      tree.insert({ hash: 'd4', result: <any>[ 2, 1 ]});
      tree.remove({ hash: 'b2', result: <any>[ 1, 3 ]});
      tree.insert({ hash: 'e5', result: <any>[ 1, 4 ]});
      expect(treeToString(tree)).toContain('1,2 => a1(i:0)');
      expect(treeToString(tree)).toContain('1,4 => e5(i:1)');
      expect(treeToString(tree)).toContain('2,1 => c3(i:2)');
      expect(treeToString(tree)).toContain('2,1 => d4(i:3)');
      expect(treeToString(tree)).not.toContain('1,3 => b2(i:1)');
    });

    it('should handle multiple results, different hashes, deletions, insertions and different ascending order', () => {
      tree = new IndexedSortTree(
        orderTypesObject,
        [ false, true ],
      );
      tree.insert({ hash: 'a1', result: <any>[ 3, 2 ]});
      tree.insert({ hash: 'b2', result: <any>[ 3, 3 ]});
      tree.insert({ hash: 'c3', result: <any>[ 2, 1 ]});
      tree.insert({ hash: 'd4', result: <any>[ 2, 1 ]});
      tree.insert({ hash: 'e5', result: <any>[ 3, 4 ]});
      expect(treeToString(tree)).toContain('3,2 => a1(i:0)');
      expect(treeToString(tree)).toContain('3,3 => b2(i:1)');
      expect(treeToString(tree)).toContain('3,4 => e5(i:2)');
      expect(treeToString(tree)).toContain('2,1 => c3(i:3)');
      expect(treeToString(tree)).toContain('2,1 => d4(i:4)');
      tree.remove({ hash: 'b2', result: <any>[ 3, 3 ]});
      expect(treeToString(tree)).not.toContain('3,3 => b2(i:1)');
    });
  });
});
