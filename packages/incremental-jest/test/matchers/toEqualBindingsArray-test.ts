import { BindingsFactory } from '@comunica/bindings-factory';
import { DataFactory } from 'rdf-data-factory';
import '../../lib';
import {ActionContextKeyIsAddition} from "@incremunica/actor-merge-bindings-context-is-addition";
import {DevTools} from "@incremunica/dev-tools";

const DF = new DataFactory();

describe('toEqualBindingsArray', () => {
  let BF: BindingsFactory;

  beforeEach(async () => {
    BF = await DevTools.createBindingsFactory(DF);
  });

  it('should succeed for equal empty bindings', () => {
    return expect([]).toEqualBindingsArray([]);
  });

  it('should succeed for equal non-empty bindings', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).toEqualBindingsArray([
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

  it('should not succeed for non-equal bindings', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).not.toEqualBindingsArray([
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

  it('should not succeed for non-equal bindings due to different length', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).not.toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]);
  });

  it('should not fail for equal empty bindings', () => {
    return expect(() => expect([]).not.toEqualBindingsArray([]))
      .toThrowError(`
Expected:
[ ]
Received:
[ ]`);
  });

  it('should not fail for equal non-empty bindings', () => {
    return expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).not.toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]))
      .toThrowError(`
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
    return expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b2') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]))
      .toThrowError(`
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
    return expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).toEqualBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]))
      .toThrowError(`
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
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).toEqualBindingsArray([
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

  it('should not succeed for equal non-empty bindings with different diffs', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).not.toEqualBindingsArray([
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
});

describe('toBeIsomorphicBindingsArray', () => {
  let BF: BindingsFactory;

  beforeEach(async () => {
    BF = await DevTools.createBindingsFactory(DF);
  });

  it('should succeed for equal empty bindings', () => {
    return expect([]).toBeIsomorphicBindingsArray([]);
  });

  it('should succeed for equal non-empty bindings', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]);
  });

  it('should not succeed for non-equal bindings', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).not.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b2') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]);
  });

  it('should not succeed for non-equal bindings due to different length', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).not.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]);
  });

  it('should not fail for equal empty bindings', () => {
    return expect(() => expect([]).not.toBeIsomorphicBindingsArray([]))
      .toThrowError(`
Expected:
[ ]
Received:
[ ]`);
  });

  it('should not fail for equal non-empty bindings', () => {
    return expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).not.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]))
      .toThrowError(`
Expected:
[
\t{
\t  "b": "b1",
\t  "c": "c1"
\t}, isAddition: true
\t{
\t  "a": "a1",
\t  "b": "b1"
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
    return expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b2') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]))
      .toThrowError(`
Expected:
[
\t{
\t  "b": "b2",
\t  "c": "c1"
\t}, isAddition: true
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

  it('should fail for non-equal non-empty bindings due to different length', () => {
    return expect(() => expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a2') ],
        [ DF.variable('b'), DF.namedNode('b2') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]))
      .toThrowError(`
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
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false),
    ]);
  });

  it('should not succeed for equal non-empty bindings with different diffs', () => {
    return expect([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
    ]).not.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('b'), DF.namedNode('b1') ],
        [ DF.variable('c'), DF.namedNode('c1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('a1') ],
        [ DF.variable('b'), DF.namedNode('b1') ],
      ]).setContextEntry(new ActionContextKeyIsAddition(), false),
    ]);
  });
});
