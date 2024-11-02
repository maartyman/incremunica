import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import '../../lib';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import { DevTools } from '@incremunica/dev-tools';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';

const DF = new DataFactory();

describe('toEqualBindingsStream', () => {
  let BF: BindingsFactory;

  beforeEach(async() => {
    BF = await DevTools.createTestBindingsFactory(DF);
  });

  it('should succeed for equal empty bindings', async() => {
    await expect(new ArrayIterator([], { autoStart: false })).toEqualBindingsStream([]);
  });

  it('should succeed for equal non-empty bindings', async() => {
    await expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ], { autoStart: false })).toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]);
  });

  it('should not succeed for non-equal bindings', async() => {
    await expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ], { autoStart: false })).not.toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b2') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]);
  });

  it('should not succeed for non-equal bindings due to different length', async() => {
    await expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ], { autoStart: false })).not.toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]);
  });

  it('should not fail for equal empty bindings', async() => {
    await expect(() => expect(new ArrayIterator([], { autoStart: false })).not.toEqualBindingsStream([]))
      .rejects.toThrow(`
Expected:
[ ]
Received:
[ ]`);
  });

  it('should not fail for equal non-empty bindings', async() => {
    await expect(() => expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ], { autoStart: false })).not.toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]))
      .rejects.toThrow(`
Expected:
[
\t{
\t  "a": "a1",
\t  "b": "b1"
\t}, isAddition: true
\t{
\t  "b": "b1",
\t  "c": "c1"
\t}, isAddition: true
]
Received:
[
\t{
\t  "a": "a1",
\t  "b": "b1"
\t}, isAddition: true
\t{
\t  "b": "b1",
\t  "c": "c1"
\t}, isAddition: true
]`);
  });

  it('should fail for non-equal non-empty bindings', async() => {
    await expect(() => expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ], { autoStart: false })).toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b2') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]))
      .rejects.toThrow(`
Expected:
[
\t{
\t  "a": "a2",
\t  "b": "b2"
\t}, isAddition: true
\t{
\t  "b": "b2",
\t  "c": "c1"
\t}, isAddition: true
]
Received:
[
\t{
\t  "a": "a1",
\t  "b": "b1"
\t}, isAddition: true
\t{
\t  "b": "b1",
\t  "c": "c1"
\t}, isAddition: true
]
Index 0 is different.`);
  });

  it('should fail for non-equal non-empty bindings due to different length', async() => {
    await expect(() => expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ], { autoStart: false })).toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]))
      .rejects.toThrow(`
Expected:
[
\t{
\t  "a": "a2",
\t  "b": "b2"
\t}, isAddition: true
]
Received:
[
\t{
\t  "a": "a1",
\t  "b": "b1"
\t}, isAddition: true
\t{
\t  "b": "b1",
\t  "c": "c1"
\t}, isAddition: true
]`);
  });

  it('should succeed for equal non-empty false bindings', async() => {
    await expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ], { autoStart: false })).toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]);
  });

  it('should not succeed for equal non-empty bindings with different diffs', async() => {
    await expect(new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ], { autoStart: false })).not.toEqualBindingsStream([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]);
  });
});
