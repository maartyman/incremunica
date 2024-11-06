import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import '../../lib';
import { KeysBindings } from '@incremunica/context-entries';
import { DevTools } from '@incremunica/dev-tools';
import { DataFactory } from 'rdf-data-factory';

const DF = new DataFactory();

// TODO check if all test still do what they are supposed to do
describe('toEqualBindingsArray', () => {
  let BF: BindingsFactory;

  beforeEach(async() => {
    BF = await DevTools.createTestBindingsFactory(DF);
  });

  it('should succeed for equal empty bindings', () => {
    expect([]).toEqualBindingsArray([]);
  });

  it('should succeed for equal non-empty bindings', () => {
    expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]).toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]);
  });

  it('should not succeed for non-equal bindings', () => {
    expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]).not.toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b2') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]);
  });

  it('should not succeed for non-equal bindings due to different length', () => {
    expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]).not.toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]);
  });

  it('should not fail for equal empty bindings', () => {
    expect(() => expect([]).not.toEqualBindingsArray([]))
      .toThrow(`
Expected:
[ ]
Received:
[ ]`);
  });

  it('should not fail for equal non-empty bindings', () => {
    expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]).not.toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]))
      .toThrow(`
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

  it('should fail for non-equal non-empty bindings', () => {
    expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]).toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b2') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]))
      .toThrow(`
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

  it('should fail for non-equal non-empty bindings due to different length', () => {
    expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]).toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]))
      .toThrow(`
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

  it('should succeed for equal non-empty false bindings', () => {
    expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, false),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]).toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, false),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]);
  });

  it('should not succeed for equal non-empty bindings with different diffs', () => {
    expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]).not.toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(KeysBindings.isAddition, false),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]);
  });
});
