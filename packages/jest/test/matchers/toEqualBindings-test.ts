import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import '../../lib';
import { KeysBindings } from '@incremunica/context-entries';
import { createTestBindingsFactory } from '@incremunica/dev-tools';
import { DataFactory } from 'rdf-data-factory';

const DF = new DataFactory();

describe('toEqualBindings', () => {
  let BF: BindingsFactory;

  beforeEach(async() => {
    BF = await createTestBindingsFactory(DF);
  });

  it('should succeed for equal empty bindings', () => {
    expect(BF.bindings()).toEqualBindings(BF.bindings());
  });

  it('should succeed for equal non-empty bindings', () => {
    expect(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(KeysBindings.isAddition, true)).toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(KeysBindings.isAddition, true));
  });

  it('should succeed for equal non-empty bindings even is the context is not set', () => {
    expect(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ])).toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(KeysBindings.isAddition, true));
  });

  it('should add the isAddition even is it isn\'t added', () => {
    expect(() => expect(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ])).toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a2') ],
      [ DF.variable('b'), DF.namedNode('b2') ],
    ])))
      .toThrow(`
Expected:
{
  "a": "a2",
  "b": "b2"
}, isAddition: true
Received:
{
  "a": "a1",
  "b": "b1"
}, isAddition: true`);
  });

  it('should not succeed for non-equal bindings', () => {
    expect(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(KeysBindings.isAddition, true)).not.toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a2') ],
      [ DF.variable('b'), DF.namedNode('b2') ],
    ]).setContextEntry(KeysBindings.isAddition, true));
  });

  it('should not fail for equal empty bindings', () => {
    expect(() => expect(BF.bindings()).not.toEqualBindings(BF.bindings()))
      .toThrow(`
Expected:
{}, isAddition: true
Received:
{}, isAddition: true`);
  });

  it('should not fail for equal non-empty bindings', () => {
    expect(() => expect(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(KeysBindings.isAddition, true)).not.toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(KeysBindings.isAddition, true)))
      .toThrow(`
Expected:
{
  "a": "a1",
  "b": "b1"
}, isAddition: true
Received:
{
  "a": "a1",
  "b": "b1"
}, isAddition: true`);
  });

  it('should fail for non-equal non-empty bindings', () => {
    expect(() => expect(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(KeysBindings.isAddition, true)).toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a2') ],
      [ DF.variable('b'), DF.namedNode('b2') ],
    ]).setContextEntry(KeysBindings.isAddition, true)))
      .toThrow(`
{
  "a": "a2",
  "b": "b2"
}, isAddition: true
Received:
{
  "a": "a1",
  "b": "b1"
}, isAddition: true`);
  });

  it('should succeed for equal non-empty false bindings', () => {
    expect(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(KeysBindings.isAddition, false)).toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(KeysBindings.isAddition, false));
  });

  it('should not succeed for equal bindings with different diffs', () => {
    expect(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(KeysBindings.isAddition, false)).not.toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(KeysBindings.isAddition, true));
  });
});
