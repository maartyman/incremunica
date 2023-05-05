import { BindingsFactory } from '@comunica/incremental-bindings-factory';
import { DataFactory } from 'rdf-data-factory';
import '../../lib';

const DF = new DataFactory();
const BF = new BindingsFactory();

describe('toEqualBindingsArray', () => {
  it('should succeed for equal empty bindings', () => {
    return expect([]).toEqualBindingsArray([]);
  });

  it('should succeed for equal non-empty bindings', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).toEqualBindingsArray([
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

  it('should not succeed for non-equal bindings', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).not.toEqualBindingsArray([
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

  it('should not succeed for non-equal bindings due to different length', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).not.toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
    ]);
  });

  it('should not fail for equal empty bindings', () => {
    return expect(() => expect([]).not.toEqualBindingsArray([]))
      .toThrowError(`expected [  ] not to equal [  ]`);
  });

  it('should not fail for equal non-empty bindings', () => {
    return expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).not.toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]))
      .toThrowError(`expected [ {
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

  it('should fail for non-equal non-empty bindings', () => {
    return expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b2') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]))
      .toThrowError(`expected [ {
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

  it('should fail for non-equal non-empty bindings due to different length', () => {
    return expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]),
    ]))
      .toThrowError(`expected [ {
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

  it('should succeed for equal non-empty false bindings', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ], false),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).toEqualBindingsArray([
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

  it('should not succeed for equal non-empty bindings with different diffs', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).not.toEqualBindingsArray([
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
});

describe('toBeIsomorphicBindingsArray', () => {
  it('should succeed for equal empty bindings', () => {
    return expect([]).toBeIsomorphicBindingsArray([]);
  });

  it('should succeed for equal non-empty bindings', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
    ]);
  });

  it('should not succeed for non-equal bindings', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).not.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b2') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
    ]);
  });

  it('should not succeed for non-equal bindings due to different length', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).not.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
    ]);
  });

  it('should not fail for equal empty bindings', () => {
    return expect(() => expect([]).not.toBeIsomorphicBindingsArray([]))
      .toThrowError(`expected [  ] not to equal [  ]`);
  });

  it('should not fail for equal non-empty bindings', () => {
    return expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).not.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
    ]))
      .toThrowError(`expected [ {
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
    "b": "b1",
    "c": "c1"
  },
  "diff": true
}, {
  "bindings": {
    "a": "a1",
    "b": "b1"
  },
  "diff": true
} ]`);
  });

  it('should fail for non-equal non-empty bindings', () => {
    return expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b2') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]),
    ]))
      .toThrowError(`expected [ {
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
    "b": "b2",
    "c": "c1"
  },
  "diff": true
}, {
  "bindings": {
    "a": "a2",
    "b": "b2"
  },
  "diff": true
} ].`);
  });

  it('should fail for non-equal non-empty bindings due to different length', () => {
    return expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]),
    ]))
      .toThrowError(`expected [ {
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

  it('should succeed for equal non-empty false bindings', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ], false),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ], false),
    ]);
  });

  it('should not succeed for equal non-empty bindings with different diffs', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
    ]).not.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ], false),
    ]);
  });
});
