import { BindingsFactory } from '@comunica/incremental-bindings-factory';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import '../../lib';

const DF = new DataFactory();
const BF = new BindingsFactory();

describe('toEqualBindingsStream', () => {
  it('should succeed for equal empty bindings', async() => {
    await expect(new ArrayIterator([], { autoStart: false })).toEqualBindingsStream([]);
  });

  it('should succeed for equal non-empty bindings', async() => {
    await expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ], { autoStart: false })).toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]);
  });

  it('should not succeed for non-equal bindings', async() => {
    await expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ], { autoStart: false })).not.toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b2') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]);
  });

  it('should not succeed for non-equal bindings due to different length', async() => {
    await expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ], { autoStart: false })).not.toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
    ]);
  });

  it('should not fail for equal empty bindings', async() => {
    await expect(() => expect(new ArrayIterator([], { autoStart: false })).not.toEqualBindingsStream([]))
      .rejects.toThrowError(`expected [  ] not to equal [  ]`);
  });

  it('should not fail for equal non-empty bindings', async() => {
    await expect(() => expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ], { autoStart: false })).not.toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]))
      .rejects.toThrowError(`expected [ {
  "bindings": {
    "a": "a1",
    "b": "b1"
  },
  "diff": true
}, {
  "bindings": {
    "b": "b1",
    "c": "c1"
  },
  "diff": true
} ] not to equal [ {
  "bindings": {
    "a": "a1",
    "b": "b1"
  },
  "diff": true
}, {
  "bindings": {
    "b": "b1",
    "c": "c1"
  },
  "diff": true
} ]`);
  });

  it('should fail for non-equal non-empty bindings', async() => {
    await expect(() => expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ], { autoStart: false })).toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b2') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]))
      .rejects.toThrowError(`expected [ {
  "bindings": {
    "a": "a1",
    "b": "b1"
  },
  "diff": true
}, {
  "bindings": {
    "b": "b1",
    "c": "c1"
  },
  "diff": true
} ] to equal [ {
  "bindings": {
    "a": "a2",
    "b": "b2"
  },
  "diff": true
}, {
  "bindings": {
    "b": "b2",
    "c": "c1"
  },
  "diff": true
} ]
Index 0 is different.`);
  });

  it('should fail for non-equal non-empty bindings due to different length', async() => {
    await expect(() => expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ], { autoStart: false })).toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]),
    ]))
      .rejects.toThrowError(`expected [ {
  "bindings": {
    "a": "a1",
    "b": "b1"
  },
  "diff": true
}, {
  "bindings": {
    "b": "b1",
    "c": "c1"
  },
  "diff": true
} ] to equal [ {
  "bindings": {
    "a": "a2",
    "b": "b2"
  },
  "diff": true
} ]`);
  });

  it('should succeed for equal non-empty false bindings', async() => {
    await expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ],false),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ], { autoStart: false })).toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ], false),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]);
  });

  it('should not succeed for equal non-empty bindings with different diffs', async() => {
    await expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ],false),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ], { autoStart: false })).not.toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]);
  });
});
