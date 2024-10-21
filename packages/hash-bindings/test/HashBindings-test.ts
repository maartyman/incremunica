import '@incremunica/incremental-jest';
import 'jest-rdf';
import type { Bindings, BindingsFactory } from '@comunica/utils-bindings-factory';
import { DevTools } from '@incremunica/dev-tools';
import { DataFactory } from 'rdf-data-factory';
import { HashBindings } from '../lib';

const DF = new DataFactory();

describe('HashBindings', () => {
  let hashBindings: HashBindings;
  let BF: BindingsFactory;

  beforeEach(async() => {
    hashBindings = new HashBindings();
    BF = await DevTools.createBindingsFactory(DF);
  });

  it('should hash bindings', () => {
    const bindings: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
    ]);

    const hash = hashBindings.hash(bindings);

    expect(hash).toBe('a:<ex:a1>\n');
  });

  it('should hash deterministically', () => {
    const bindings1: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('b'), DF.namedNode('ex:b1') ],
    ]);

    const bindings2: Bindings = BF.bindings([
      [ DF.variable('b'), DF.namedNode('ex:b1') ],
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
    ]);

    const hash1 = hashBindings.hash(bindings1);
    const hash2 = hashBindings.hash(bindings2);

    expect(hash1).toBe('a:<ex:a1>\nb:<ex:b1>\n');
    expect(hash2).toBe('a:<ex:a1>\nb:<ex:b1>\n');
  });

  it('should hash if value is undefined', () => {
    const bindings1: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('b'), DF.namedNode('ex:b1') ],
      [ DF.variable('c'), DF.namedNode('ex:c1') ],
    ]);

    const bindings2: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('c'), DF.namedNode('ex:c1') ],
    ]);

    const hash1 = hashBindings.hash(bindings1);
    const hash2 = hashBindings.hash(bindings2);

    expect(hash1).toBe('a:<ex:a1>\nb:<ex:b1>\nc:<ex:c1>\n');
    expect(hash2).toBe('a:<ex:a1>\nc:<ex:c1>\n');
  });

  it('should hash if value is undefined and should be the same before and after', () => {
    const bindings1: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('c'), DF.namedNode('ex:c1') ],
    ]);

    const bindings2: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('b'), DF.namedNode('ex:b1') ],
      [ DF.variable('c'), DF.namedNode('ex:c1') ],
    ]);

    const bindings3: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('c'), DF.namedNode('ex:c1') ],
    ]);

    const hash1 = hashBindings.hash(bindings1);
    const hash2 = hashBindings.hash(bindings2);
    const hash3 = hashBindings.hash(bindings3);

    expect(hash1).toBe('a:<ex:a1>\nc:<ex:c1>\n');
    expect(hash2).toBe('a:<ex:a1>\nc:<ex:c1>\nb:<ex:b1>\n');
    expect(hash3).toBe('a:<ex:a1>\nc:<ex:c1>\n');
  });

  it('should hash with the initialization variables', () => {
    hashBindings = new HashBindings([ DF.variable('a'), DF.variable('b') ]);

    const bindings1: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('b'), DF.namedNode('ex:b1') ],
    ]);

    const bindings2: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
    ]);

    const hash1 = hashBindings.hash(bindings1);
    const hash2 = hashBindings.hash(bindings2);

    expect(hash1).toBe('a:<ex:a1>\nb:<ex:b1>\n');
    expect(hash2).toBe('a:<ex:a1>\n');
  });
});
