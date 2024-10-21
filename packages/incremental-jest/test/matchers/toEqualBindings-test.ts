import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import '../../lib';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import { DevTools } from '@incremunica/dev-tools';
import { DataFactory } from 'rdf-data-factory';

const DF = new DataFactory();

// TODO check if all test still do what they are supposed to do
describe('toEqualBindings', () => {
  let BF: BindingsFactory;

  beforeEach(async() => {
    BF = await DevTools.createBindingsFactory(DF);
  });

  it('should succeed for equal empty bindings', () => {
    expect(BF.bindings()).toEqualBindings(BF.bindings());
  });

  it('should succeed for equal non-empty bindings', () => {
    expect(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(new ActionContextKeyIsAddition(), true)).toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(new ActionContextKeyIsAddition(), true));
  });

  it('should not succeed for non-equal bindings', () => {
    expect(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(new ActionContextKeyIsAddition(), true)).not.toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a2') ],
      [ DF.variable('b'), DF.namedNode('b2') ],
    ]).setContextEntry(new ActionContextKeyIsAddition(), true));
  });

  it('should not fail for equal empty bindings', () => {
    expect(() => expect(BF.bindings()).not.toEqualBindings(BF.bindings()))
      .toThrow(`
Expected:
{}, isAddition: undefined
Received:
{}, isAddition: undefined`);
  });

  it('should not fail for equal non-empty bindings', () => {
    expect(() => expect(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(new ActionContextKeyIsAddition(), true)).not.toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(new ActionContextKeyIsAddition(), true)))
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
    ]).setContextEntry(new ActionContextKeyIsAddition(), true)).toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a2') ],
      [ DF.variable('b'), DF.namedNode('b2') ],
    ]).setContextEntry(new ActionContextKeyIsAddition(), true)))
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
    ]).setContextEntry(new ActionContextKeyIsAddition(), false)).toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(new ActionContextKeyIsAddition(), false));
  });

  it('should not succeed for equal bindings with different diffs', () => {
    expect(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(new ActionContextKeyIsAddition(), false)).not.toEqualBindings(BF.bindings([
      [ DF.variable('a'), DF.namedNode('a1') ],
      [ DF.variable('b'), DF.namedNode('b1') ],
    ]).setContextEntry(new ActionContextKeyIsAddition(), true));
  });
});
