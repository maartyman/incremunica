import type { IActorTermComparatorFactoryOutput } from '@comunica/bus-term-comparator-factory';
import type * as RDF from '@rdfjs/types';

interface Node {
  data: {
    hash: string;
    result: (RDF.Term | undefined)[];
    index: number;
  };
  left: null | Node;
  right: null | Node;
  red: boolean;
}

export class IndexedSortTree {
  private readonly comparator: (a: any, b: any) => number;
  private root: null | Node;
  private readonly collator: Intl.Collator;

  public constructor(orderByEvaluator: IActorTermComparatorFactoryOutput, isAscending: boolean[]) {
    this.collator = new Intl.Collator(undefined, { usage: 'sort' });
    this.comparator = (
      left: Node['data'],
      right: Node['data'],
    ) => {
      for (let i = 0; i < left.result.length; i++) {
        const compare: number = orderByEvaluator.orderTypes(left.result[i], right.result[i]);
        if (compare !== 0) {
          if (!isAscending[i]) {
            return compare * -1;
          }
          return compare;
        }
      }
      return this.collator.compare(left.hash, right.hash);
    };
    this.root = null;
  }

  public insert(data: { hash: string; result: (RDF.Term | undefined)[] }): Node {
    if (this.root === null) {
      // Empty tree
      const node = {
        data: { hash: data.hash, result: data.result, index: 0 },
        left: null,
        right: null,
        red: false,
      };
      this.root = node;
      return node;
    }

    // Fake tree root
    const head: Node = {
      left: null,
      right: null,
      red: true,
      data: { hash: '', result: [], index: -1 },
    };

    // False if left, true if right
    let dir = false;
    // False if left, true if right
    let last = false;

    // Setup
    // Grandparent
    let gp = null;
    // Grand-grand-parent
    let ggp = head;
    // Parent
    let p = null;
    let node: Node | null = this.root;
    ggp.right = this.root;

    // Search down
    let inserted = false;
    while (true) {
      if (node === null) {
        // Insert new node at the bottom
        /* eslint-disable-next-line ts/no-unnecessary-type-assertion */
        let index: number = p!.data.index;
        if (dir) {
          index += 1;
        } else {
          index -= 1;
        }
        node = {
          left: null,
          right: null,
          red: true,
          data: { hash: data.hash, result: data.result, index },
        };
        /* eslint-disable-next-line ts/no-unnecessary-type-assertion */
        dir ? p!.right = node : p!.left = node;
        inserted = true;
      } else if (node.left?.red && node.right?.red) {
        // Color flip
        node.red = true;
        node.left.red = false;
        node.right.red = false;
      }

      // Fix red violation
      if (node.red && p?.red) {
        const dir2 = (ggp.right === gp);

        if (node === (last ? p.right : p.left)) {
          /* eslint-disable-next-line ts/no-unnecessary-type-assertion */
          dir2 ? ggp.right = this.single_rotate(gp!, !last) : ggp.left = this.single_rotate(gp!, !last);
        } else {
          /* eslint-disable-next-line ts/no-unnecessary-type-assertion */
          dir2 ? ggp.right = this.double_rotate(gp!, !last) : ggp.left = this.double_rotate(gp!, !last);
        }
      }

      if (inserted) {
        break;
      }

      const cmp = this.comparator(node.data, data);

      last = dir;
      dir = cmp <= 0;

      // Update helpers
      if (gp !== null) {
        ggp = gp;
      }
      gp = p;
      p = node;
      node = (dir ? node.right : node.left);
      if (!dir) {
        // Update the index of the parent and all indexes on the right
        p.data.index++;
        if (p.right) {
          this.incrementIndex(p.right);
        }
      }
    }

    // Update root
    this.root = head.right;
    this.root!.red = false;
    return node;
  }

  public remove(data: { hash: string; result: (RDF.Term | undefined)[] }): Node {
    if (this.root === null) {
      throw new Error('Deletion does not exist');
    }

    // Fake tree root
    const head: Node = {
      left: null,
      right: null,
      red: true,
      data: { hash: '', result: [], index: -1 },
    };
    let node: Node = head;
    node.right = this.root;
    // Parent
    let p = null;
    // Grand parent
    let gp = null;
    // Found item
    let found = null;
    let dir = true;

    while ((dir ? node.right : node.left) !== null) {
      const last = dir;

      // Update helpers
      gp = p;
      p = node;
      node = (dir ? node.right : node.left)!;

      const cmp = this.comparator(data, node.data);

      dir = cmp > 0;

      // Save found node
      if (cmp === 0) {
        found = node;
        if (!dir && node.right) {
          this.decrementIndex(node.right);
        }
      }

      if (!found && !dir) {
        // Update the index of the parent and all indexes on the right
        node.data.index--;
        if (node.right) {
          this.decrementIndex(node.right);
        }
      }

      // Push the red node down
      if (!node.red && !((dir ? node.right : node.left)?.red)) {
        if ((dir ? node.left : node.right)?.red) {
          const sr = this.single_rotate(node, dir);
          if (last) {
            p.right = sr;
          } else {
            p.left = sr;
          }
          p = sr;
        } else if (!(dir ? node.left : node.right)?.red) {
          const sibling = last ? p.left : p.right;
          if (sibling !== null) {
            if (!((last ? sibling.left : sibling.right)?.red) && !((last ? sibling.right : sibling.left)?.red)) {
              // Color flip
              p.red = false;
              sibling.red = true;
              node.red = true;
            } else {
              /* eslint-disable-next-line ts/no-unnecessary-type-assertion */
              const dir2 = (gp!.right === p);
              if ((last ? sibling.right : sibling.left)?.red) {
                if (dir2) {
                  /* eslint-disable-next-line ts/no-unnecessary-type-assertion */
                  gp!.right = this.double_rotate(p, last);
                } else {
                  /* eslint-disable-next-line ts/no-unnecessary-type-assertion */
                  gp!.left = this.double_rotate(p, last);
                }
              } else if ((last ? sibling.left : sibling.right)?.red) {
                if (dir2) {
                  /* eslint-disable-next-line ts/no-unnecessary-type-assertion */
                  gp!.right = this.single_rotate(p, last);
                } else {
                  /* eslint-disable-next-line ts/no-unnecessary-type-assertion */
                  gp!.left = this.single_rotate(p, last);
                }
              }

              // Ensure correct coloring
              /* eslint-disable-next-line ts/no-unnecessary-type-assertion */
              const gpc = (dir2 ? gp!.right : gp!.left)!;
              gpc.red = true;
              node.red = true;
              gpc.left!.red = false;
              gpc.right!.red = false;
            }
          }
        }
      }
    }

    const returnNode: Node = {
      right: null,
      left: null,
      red: false,
      data: { hash: '', result: [], index: -1 },
    };
    // Replace and remove
    if (found === null) {
      throw new Error('Deletion does not exist');
    } else {
      returnNode.data = found.data;
      found.data = node.data;
      /* eslint-disable-next-line ts/no-unnecessary-type-assertion */
      if (p!.right === node) {
        /* eslint-disable-next-line ts/no-unnecessary-type-assertion */
        p!.right = node.left ?? node.right;
      } else {
        /* eslint-disable-next-line ts/no-unnecessary-type-assertion */
        p!.left = node.left ?? node.right;
      }
    }

    // Update root and make it black
    this.root = head.right;
    if (this.root !== null) {
      this.root.red = false;
    }

    return returnNode;
  }

  private incrementIndex(node: Node): void {
    node.data.index++;
    if (node.right) {
      this.incrementIndex(node.right);
    }
    if (node.left) {
      this.incrementIndex(node.left);
    }
  }

  private decrementIndex(node: Node): void {
    node.data.index--;
    if (node.right) {
      this.decrementIndex(node.right);
    }
    if (node.left) {
      this.decrementIndex(node.left);
    }
  }

  private single_rotate(root: Node, dir: boolean): Node {
    if (dir) {
      const save = root.left!;

      root.left = save.right;
      save.right = root;

      root.red = true;
      save.red = false;
      return save;
    }
    const save = root.right!;

    root.right = save.left;
    save.left = root;

    root.red = true;
    save.red = false;
    return save;
  }

  private double_rotate(root: Node, dir: boolean): Node {
    if (dir) {
      root.left = this.single_rotate(root.left!, !dir);
      return this.single_rotate(root, dir);
    }
    root.right = this.single_rotate(root.right!, !dir);
    return this.single_rotate(root, dir);
  }
}
