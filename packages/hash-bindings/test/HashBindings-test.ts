import '@incremunica/incremental-jest';
import 'jest-rdf';
import {HashBindings} from "../lib";
import {Bindings, BindingsFactory} from "@comunica/bindings-factory";
import {DataFactory} from "rdf-data-factory";
import {DevTools} from "@incremunica/dev-tools";

const DF = new DataFactory();

describe('HashBindings', () => {
  let hashBindings: HashBindings;
  let BF: BindingsFactory;

  beforeEach(async () => {
    hashBindings = new HashBindings();
    BF = await DevTools.createBindingsFactory(DF);
  });

  it('should hash bindings', () => {
    let bindings: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
    ]);

    let hash = hashBindings.hash(bindings);

    expect(hash).toEqual('a:<ex:a1>\n');
  });

  it('should hash deterministically', () => {
    let bindings1: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('b'), DF.namedNode('ex:b1') ],
    ]);

    let bindings2: Bindings = BF.bindings([
      [ DF.variable('b'), DF.namedNode('ex:b1') ],
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
    ]);

    let hash1 = hashBindings.hash(bindings1);
    let hash2 = hashBindings.hash(bindings2);

    expect(hash1).toEqual('a:<ex:a1>\nb:<ex:b1>\n');
    expect(hash2).toEqual('a:<ex:a1>\nb:<ex:b1>\n');
  });

  it('should hash if value is undefined', () => {
    let bindings1: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('b'), DF.namedNode('ex:b1') ],
      [ DF.variable('c'), DF.namedNode('ex:c1') ],
    ]);

    let bindings2: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('c'), DF.namedNode('ex:c1') ],
    ]);

    let hash1 = hashBindings.hash(bindings1);
    let hash2 = hashBindings.hash(bindings2);

    expect(hash1).toEqual('a:<ex:a1>\nb:<ex:b1>\nc:<ex:c1>\n');
    expect(hash2).toEqual('a:<ex:a1>\nc:<ex:c1>\n');
  });



  it('should hash if value is undefined and should be the same before and after', () => {
    let bindings1: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('c'), DF.namedNode('ex:c1') ],
    ]);

    let bindings2: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('b'), DF.namedNode('ex:b1') ],
      [ DF.variable('c'), DF.namedNode('ex:c1') ],
    ]);

    let bindings3: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('c'), DF.namedNode('ex:c1') ],
    ]);

    let hash1 = hashBindings.hash(bindings1);
    let hash2 = hashBindings.hash(bindings2);
    let hash3 = hashBindings.hash(bindings3);

    expect(hash1).toEqual('a:<ex:a1>\nc:<ex:c1>\n');
    expect(hash2).toEqual('a:<ex:a1>\nc:<ex:c1>\nb:<ex:b1>\n');
    expect(hash3).toEqual('a:<ex:a1>\nc:<ex:c1>\n');
  });

  it('should hash with the initialization variables', () => {
    hashBindings = new HashBindings([ DF.variable('a'), DF.variable('b') ]);

    let bindings1: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
      [ DF.variable('b'), DF.namedNode('ex:b1') ],
    ]);

    let bindings2: Bindings = BF.bindings([
      [ DF.variable('a'), DF.namedNode('ex:a1') ],
    ]);

    let hash1 = hashBindings.hash(bindings1);
    let hash2 = hashBindings.hash(bindings2);

    expect(hash1).toEqual('a:<ex:a1>\nb:<ex:b1>\n');
    expect(hash2).toEqual('a:<ex:a1>\n');
  });
});


