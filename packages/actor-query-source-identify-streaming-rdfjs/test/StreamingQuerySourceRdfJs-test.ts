import { Readable } from 'node:stream';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { KeysQueryOperation } from '@comunica/context-entries';
import { ActionContext } from '@comunica/core';
import { MetadataValidationState } from '@comunica/metadata';
import type { IActionContext } from '@comunica/types';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import { DevTools } from '@incremunica/dev-tools';
import { StreamingStore } from '@incremunica/incremental-rdf-streaming-store';
import type { Quad } from '@incremunica/incremental-types';
import arrayifyStream from 'arrayify-stream';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { Factory } from 'sparqlalgebrajs';
import { StreamingQuerySourceRdfJs } from '../lib';
import '@incremunica/incremental-jest';
import 'jest-rdf';

const quad = require('rdf-quad');

const DF = new DataFactory();
const AF = new Factory();

describe('StreamingQuerySourceRdfJs', () => {
  let ctx: IActionContext;

  let store: StreamingStore<Quad>;
  let source: StreamingQuerySourceRdfJs;
  let BF: BindingsFactory;
  beforeEach(async() => {
    ctx = new ActionContext({});
    store = new StreamingStore();
    BF = await DevTools.createBindingsFactory(DF);
    source = new StreamingQuerySourceRdfJs(store, BF);
  });

  describe('getSelectorShape', () => {
    it('should return a selector shape', async() => {
      await expect(source.getSelectorShape()).resolves.toEqual({
        type: 'operation',
        operation: {
          operationType: 'pattern',
          pattern: AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o')),
        },
        variablesOptional: [
          DF.variable('s'),
          DF.variable('p'),
          DF.variable('o'),
        ],
      });
    });
  });

  describe('toString', () => {
    it('should return a string representation', async() => {
      expect(source.toString()).toBe('StreamingQuerySourceRdfJs(StreamingStore)');
    });
  });

  describe('queryBindings', () => {
    it('should throw when passing non-pattern', async() => {
      expect(() => source.queryBindings(
        AF.createNop(),
        ctx,
      )).toThrow(`Attempted to pass non-pattern operation 'nop' to StreamingQuerySourceRdfJs`);
    });

    it('should return triples in the default graph', async() => {
      store.addQuad(<Quad>DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1')));
      store.addQuad(<Quad>DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2')));
      store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('px'), DF.namedNode('o3')));
      store.end();

      const data = source.queryBindings(
        AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o')),
        ctx,
      );
      await expect(data).toEqualBindingsStream([
        BF.fromRecord({
          s: DF.namedNode('s2'),
          o: DF.namedNode('o2'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.fromRecord({
          s: DF.namedNode('s1'),
          o: DF.namedNode('o1'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      //
      // await expect(new Promise(resolve => data.getProperty('metadata', resolve))).resolves
      // .toEqual({
      //     cardinality: { type: 'exact', value: 2 },
      //     canContainUndefs: false,
      //     state: expect.any(MetadataValidationState),
      //     variables: [ DF.variable('s'), DF.variable('o') ],
      // });
      //
    });

    it('should return triples in a named graph', async() => {
      store.addQuad(<Quad>DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1'), DF.namedNode('g1')));
      store.addQuad(<Quad>DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2'), DF.namedNode('g2')));
      store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('px'), DF.namedNode('o3'), DF.namedNode('g3')));
      store.end();

      const data = source.queryBindings(
        AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o'), DF.namedNode('g1')),
        ctx,
      );
      await expect(data).toEqualBindingsStream([
        BF.fromRecord({
          s: DF.namedNode('s1'),
          o: DF.namedNode('o1'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      await expect(new Promise(resolve => data.getProperty('metadata', resolve))).resolves
        .toEqual({
          cardinality: { type: 'exact', value: 1 },
          canContainUndefs: false,
          state: expect.any(MetadataValidationState),
          variables: [ DF.variable('s'), DF.variable('o') ],
        });
    });

    it('should return quads in named graphs', async() => {
      store.addQuad(<Quad>DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1'), DF.namedNode('g1')));
      store.addQuad(<Quad>DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2')));
      store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('px'), DF.namedNode('o3')));
      store.end();

      const data = source.queryBindings(
        AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o'), DF.variable('g')),
        ctx,
      );
      await expect(data).toEqualBindingsStream([
        BF.fromRecord({
          s: DF.namedNode('s1'),
          o: DF.namedNode('o1'),
          g: DF.namedNode('g1'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      //
      // await expect(new Promise(resolve => data.getProperty('metadata', resolve))).resolves
      // .toEqual({
      //     cardinality: { type: 'estimate', value: 2 },
      //     canContainUndefs: false,
      //     state: expect.any(MetadataValidationState),
      //     variables: [ DF.variable('s'), DF.variable('o'), DF.variable('g') ],
      // });
      //
    });

    it('should return quads in named graphs and the default graph with union default graph', async() => {
      ctx = ctx.set(KeysQueryOperation.unionDefaultGraph, true);

      store.addQuad(<Quad>DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1'), DF.namedNode('g1')));
      store.addQuad(<Quad>DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2')));
      store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('px'), DF.namedNode('o3')));
      store.end();

      const data = source.queryBindings(
        AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o'), DF.variable('g')),
        ctx,
      );
      await expect(data).toEqualBindingsStream([
        BF.fromRecord({
          s: DF.namedNode('s2'),
          o: DF.namedNode('o2'),
          g: DF.defaultGraph(),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.fromRecord({
          s: DF.namedNode('s1'),
          o: DF.namedNode('o1'),
          g: DF.namedNode('g1'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      //
      // await expect(new Promise(resolve => data.getProperty('metadata', resolve))).resolves
      // .toEqual({
      //     cardinality: { type: 'exact', value: 2 },
      //     canContainUndefs: false,
      //     state: expect.any(MetadataValidationState),
      //     variables: [ DF.variable('s'), DF.variable('o'), DF.variable('g') ],
      // });
      //
    });

    it('should use countQuads if available', async() => {
      (<any> store).countQuads = () => 123;

      store.addQuad(<Quad>DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1')));
      store.addQuad(<Quad>DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2')));
      store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('px'), DF.namedNode('o3')));
      store.end();

      const data = source.queryBindings(
        AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o')),
        ctx,
      );
      await expect(data).toEqualBindingsStream([
        BF.fromRecord({
          s: DF.namedNode('s2'),
          o: DF.namedNode('o2'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.fromRecord({
          s: DF.namedNode('s1'),
          o: DF.namedNode('o1'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      //
      // await expect(new Promise(resolve => data.getProperty('metadata', resolve))).resolves
      // .toEqual({
      //     cardinality: { type: 'exact', value: 123 },
      //     canContainUndefs: false,
      //     state: expect.any(MetadataValidationState),
      //     variables: [ DF.variable('s'), DF.variable('o') ],
      // });
      //
    });

    it('should fallback to match if countQuads is not available', async() => {
      store = new StreamingStore();
      (<any> store).countQuads = undefined;
      source = new StreamingQuerySourceRdfJs(store, BF);

      store.addQuad(<Quad>DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1')));
      store.addQuad(<Quad>DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2')));
      store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('px'), DF.namedNode('o3')));
      store.end();

      const data = source.queryBindings(
        AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o')),
        ctx,
      );
      await expect(data).toEqualBindingsStream([
        BF.fromRecord({
          s: DF.namedNode('s2'),
          o: DF.namedNode('o2'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.fromRecord({
          s: DF.namedNode('s1'),
          o: DF.namedNode('o1'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      //
      // await expect(new Promise(resolve => data.getProperty('metadata', resolve))).resolves
      // .toEqual({
      //     cardinality: { type: 'exact', value: 2 },
      //     canContainUndefs: false,
      //     state: expect.any(MetadataValidationState),
      //     variables: [ DF.variable('s'), DF.variable('o') ],
      // });
      //
    });

    it('should delegate errors', async() => {
      const it = new Readable();
      it._read = () => {
        it.emit('error', new Error('RdfJsSource error'));
      };
      store = <any> { match: () => it };
      source = new StreamingQuerySourceRdfJs(store, BF);

      const data = source.queryBindings(
        AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o')),
        ctx,
      );
      await expect(arrayifyStream(data)).rejects.toThrow(new Error('RdfJsSource error'));
    });

    describe('for quoted triples', () => {
      describe('with a store supporting quoted triple filtering', () => {
        beforeEach(() => {
          store = new StreamingStore();
          source = new StreamingQuerySourceRdfJs(store, BF);
        });

        it('should run when containing quoted triples', async() => {
          store.addQuad(<Quad>DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('px'), DF.namedNode('o3')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('p'), DF.quad(
            DF.namedNode('sa3'),
            DF.namedNode('pax'),
            DF.namedNode('oa3'),
          )));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s4'), DF.namedNode('px'), DF.quad(
            DF.namedNode('sb3'),
            DF.namedNode('pbx'),
            DF.namedNode('ob3'),
          )));
          store.end();

          const data = source.queryBindings(
            AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o')),
            ctx,
          );
          await expect(data).toEqualBindingsStream([
            BF.fromRecord({
              s: DF.namedNode('s3'),
              o: DF.quad(
                DF.namedNode('sa3'),
                DF.namedNode('pax'),
                DF.namedNode('oa3'),
              ),
            }).setContextEntry(new ActionContextKeyIsAddition(), true),
            BF.fromRecord({
              s: DF.namedNode('s2'),
              o: DF.namedNode('o2'),
            }).setContextEntry(new ActionContextKeyIsAddition(), true),
            BF.fromRecord({
              s: DF.namedNode('s1'),
              o: DF.namedNode('o1'),
            }).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);
          //
          // await expect(new Promise(resolve => data.getProperty('metadata', resolve))).resolves
          // .toEqual({
          //     cardinality: { type: 'exact', value: 3 },
          //     canContainUndefs: false,
          //     state: expect.any(MetadataValidationState),
          //     variables: [ DF.variable('s'), DF.variable('o') ],
          // });
          //
        });

        it('should run when containing quoted triples with a quoted pattern (1)', async() => {
          store.addQuad(<Quad>DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('px'), DF.namedNode('o3')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('p'), DF.quad(
            DF.namedNode('sa3'),
            DF.namedNode('pax'),
            DF.namedNode('oa3'),
          )));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s4'), DF.namedNode('px'), DF.quad(
            DF.namedNode('sb3'),
            DF.namedNode('pbx'),
            DF.namedNode('ob3'),
          )));
          store.end();

          const data = source.queryBindings(
            AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.quad(
              DF.variable('s1'),
              DF.variable('p1'),
              DF.variable('o1'),
            )),
            ctx,
          );
          await expect(data).toEqualBindingsStream([
            BF.fromRecord({
              s: DF.namedNode('s3'),
              s1: DF.namedNode('sa3'),
              p1: DF.namedNode('pax'),
              o1: DF.namedNode('oa3'),
            }).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);
          await expect(new Promise(resolve => data.getProperty('metadata', resolve))).resolves
            .toEqual({
              cardinality: { type: 'exact', value: 1 },
              canContainUndefs: false,
              state: expect.any(MetadataValidationState),
              variables: [ DF.variable('s'), DF.variable('s1'), DF.variable('p1'), DF.variable('o1') ],
            });
        });

        it('should run when containing quoted triples with a quoted pattern (2)', async() => {
          store.addQuad(<Quad>DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('px'), DF.namedNode('o3')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('p'), DF.quad(
            DF.namedNode('sa3'),
            DF.namedNode('pax'),
            DF.namedNode('oa3'),
          )));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s4'), DF.namedNode('px'), DF.quad(
            DF.namedNode('sb3'),
            DF.namedNode('pbx'),
            DF.namedNode('ob3'),
          )));
          store.end();

          const data = source.queryBindings(
            AF.createPattern(DF.variable('s'), DF.variable('p'), DF.quad(
              DF.variable('s1'),
              DF.namedNode('pbx'),
              DF.variable('o1'),
            )),
            ctx,
          );
          await expect(data).toEqualBindingsStream([
            BF.fromRecord({
              s: DF.namedNode('s4'),
              p: DF.namedNode('px'),
              s1: DF.namedNode('sb3'),
              o1: DF.namedNode('ob3'),
            }).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);
          await expect(new Promise(resolve => data.getProperty('metadata', resolve))).resolves
            .toEqual({
              cardinality: { type: 'exact', value: 1 },
              canContainUndefs: false,
              state: expect.any(MetadataValidationState),
              variables: [ DF.variable('s'), DF.variable('p'), DF.variable('s1'), DF.variable('o1') ],
            });
        });

        it('should run when containing quoted triples with a nested quoted pattern', async() => {
          store.addQuad(<Quad>DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('px'), DF.namedNode('o3')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('p'), DF.quad(
            DF.namedNode('sa3'),
            DF.namedNode('pax'),
            DF.namedNode('oa3'),
          )));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s4'), DF.namedNode('px'), DF.quad(
            DF.namedNode('sb3'),
            DF.namedNode('pbx'),
            DF.namedNode('ob3'),
          )));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s4'), DF.namedNode('px'), DF.quad(
            DF.namedNode('sb3'),
            DF.namedNode('pbx'),
            DF.quad(
              DF.namedNode('sb3'),
              DF.namedNode('pbx'),
              DF.namedNode('ob3'),
            ),
          )));
          store.end();

          const data = source.queryBindings(
            AF.createPattern(DF.variable('s'), DF.variable('p'), DF.quad(
              DF.variable('s1'),
              DF.namedNode('pbx'),
              DF.quad(
                DF.variable('s2'),
                DF.variable('pcx'),
                DF.variable('o2'),
              ),
            )),
            ctx,
          );
          await expect(data).toEqualBindingsStream([
            BF.fromRecord({
              s: DF.namedNode('s4'),
              p: DF.namedNode('px'),
              s1: DF.namedNode('sb3'),
              s2: DF.namedNode('sb3'),
              pcx: DF.namedNode('pbx'),
              o2: DF.namedNode('ob3'),
            }).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);
          await expect(new Promise(resolve => data.getProperty('metadata', resolve))).resolves
            .toEqual({
              cardinality: { type: 'exact', value: 1 },
              canContainUndefs: false,
              state: expect.any(MetadataValidationState),
              variables: [
                DF.variable('s'),
                DF.variable('p'),
                DF.variable('s1'),
                DF.variable('s2'),
                DF.variable('pcx'),
                DF.variable('o2'),
              ],
            });
        });
      });

      describe('with a store not supporting quoted triple filtering', () => {
        beforeEach(() => {
          store = new StreamingStore();
          source = new StreamingQuerySourceRdfJs(store, BF);
        });

        it('should run when containing quoted triples', async() => {
          store.addQuad(<Quad>DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('px'), DF.namedNode('o3')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('p'), DF.quad(
            DF.namedNode('sa3'),
            DF.namedNode('pax'),
            DF.namedNode('oa3'),
          )));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s4'), DF.namedNode('px'), DF.quad(
            DF.namedNode('sb3'),
            DF.namedNode('pbx'),
            DF.namedNode('ob3'),
          )));
          store.end();

          const data = source.queryBindings(
            AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o')),
            ctx,
          );
          await expect(data).toEqualBindingsStream([
            BF.fromRecord({
              s: DF.namedNode('s3'),
              o: DF.quad(
                DF.namedNode('sa3'),
                DF.namedNode('pax'),
                DF.namedNode('oa3'),
              ),
            }).setContextEntry(new ActionContextKeyIsAddition(), true),
            BF.fromRecord({
              s: DF.namedNode('s2'),
              o: DF.namedNode('o2'),
            }).setContextEntry(new ActionContextKeyIsAddition(), true),
            BF.fromRecord({
              s: DF.namedNode('s1'),
              o: DF.namedNode('o1'),
            }).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);
          //
          // await expect(new Promise(resolve => data.getProperty('metadata', resolve))).resolves
          // .toEqual({
          //     cardinality: { type: 'exact', value: 3 },
          //     canContainUndefs: false,
          //     state: expect.any(MetadataValidationState),
          //     variables: [ DF.variable('s'), DF.variable('o') ],
          // });
          //
        });

        it('should run when containing quoted triples with a quoted pattern (1)', async() => {
          store.addQuad(<Quad>DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('px'), DF.namedNode('o3')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('p'), DF.quad(
            DF.namedNode('sa3'),
            DF.namedNode('pax'),
            DF.namedNode('oa3'),
          )));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s4'), DF.namedNode('px'), DF.quad(
            DF.namedNode('sb3'),
            DF.namedNode('pbx'),
            DF.namedNode('ob3'),
          )));
          store.end();

          const data = source.queryBindings(
            AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.quad(
              DF.variable('s1'),
              DF.variable('p1'),
              DF.variable('o1'),
            )),
            ctx,
          );
          await expect(data).toEqualBindingsStream([
            BF.fromRecord({
              s: DF.namedNode('s3'),
              s1: DF.namedNode('sa3'),
              p1: DF.namedNode('pax'),
              o1: DF.namedNode('oa3'),
            }).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);
          //
          // await expect(new Promise(resolve => data.getProperty('metadata', resolve))).resolves
          // .toEqual({
          //     cardinality: { type: 'estimate', value: 3 },
          //     canContainUndefs: false,
          //     state: expect.any(MetadataValidationState),
          //     variables: [ DF.variable('s'), DF.variable('s1'), DF.variable('p1'), DF.variable('o1') ],
          // });
          //
          //
        });

        it('should run when containing quoted triples with a quoted pattern (2)', async() => {
          store.addQuad(<Quad>DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('px'), DF.namedNode('o3')));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s3'), DF.namedNode('p'), DF.quad(
            DF.namedNode('sa3'),
            DF.namedNode('pax'),
            DF.namedNode('oa3'),
          )));
          store.addQuad(<Quad>DF.quad(DF.namedNode('s4'), DF.namedNode('px'), DF.quad(
            DF.namedNode('sb3'),
            DF.namedNode('pbx'),
            DF.namedNode('ob3'),
          )));
          store.end();

          const data = source.queryBindings(
            AF.createPattern(DF.variable('s'), DF.variable('p'), DF.quad(
              DF.variable('s1'),
              DF.namedNode('pbx'),
              DF.variable('o1'),
            )),
            ctx,
          );
          await expect(data).toEqualBindingsStream([
            BF.fromRecord({
              s: DF.namedNode('s4'),
              p: DF.namedNode('px'),
              s1: DF.namedNode('sb3'),
              o1: DF.namedNode('ob3'),
            }).setContextEntry(new ActionContextKeyIsAddition(), true),
          ]);
          //
          // await expect(new Promise(resolve => data.getProperty('metadata', resolve))).resolves
          // .toEqual({
          //     cardinality: { type: 'estimate', value: 5 },
          //     canContainUndefs: false,
          //     state: expect.any(MetadataValidationState),
          //     variables: [ DF.variable('s'), DF.variable('p'), DF.variable('s1'), DF.variable('o1') ],
          // });
          //
        });
      });
    });
  });

  describe('queryQuads', () => {
    it('should throw', () => {
      expect(() => source.queryQuads(<any> undefined, ctx))
        .toThrow(`queryQuads is not implemented in StreamingQuerySourceRdfJs`);
    });
  });

  describe('queryBoolean', () => {
    it('should throw', () => {
      expect(() => source.queryBoolean(<any> undefined, ctx))
        .toThrow(`queryBoolean is not implemented in StreamingQuerySourceRdfJs`);
    });
  });

  describe('queryVoid', () => {
    it('should throw', () => {
      expect(() => source.queryVoid(<any> undefined, ctx))
        .toThrow(`queryVoid is not implemented in StreamingQuerySourceRdfJs`);
    });
  });

  describe('quadsToBindings', () => {
    it('converts triples', async() => {
      // Prepare data
      const quadStream = new ArrayIterator([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
      ], { autoStart: false });
      quadStream.setProperty('metadata', {
        cardinality: { type: 'exact', value: 3 },
        order: [
          { term: 'subject', direction: 'asc' },
          { term: 'predicate', direction: 'asc' },
          { term: 'object', direction: 'asc' },
          { term: 'graph', direction: 'asc' },
        ],
      });
      const pattern = AF.createPattern(
        DF.namedNode('s'),
        DF.variable('p'),
        DF.namedNode('o'),
      );
      const bindingsStream = StreamingQuerySourceRdfJs.quadsToBindings(quadStream, pattern, BF, false);

      // Check results
      await expect(bindingsStream).toEqualBindingsStream([
        BF.fromRecord({
          p: DF.namedNode('p1'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.fromRecord({
          p: DF.namedNode('p2'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.fromRecord({
          p: DF.namedNode('p3'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);

      // Check metadata
      const metadata = await new Promise(resolve => bindingsStream.getProperty('metadata', resolve));
      expect(metadata).toEqual({
        canContainUndefs: false,
        cardinality: { type: 'exact', value: 3 },
        order: [
          { term: DF.variable('p'), direction: 'asc' },
        ],
        variables: [ DF.variable('p') ],
      });
    });

    it('converts triples without order metadata', async() => {
      // Prepare data
      const quadStream = new ArrayIterator([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
      ], { autoStart: false });
      quadStream.setProperty('metadata', {
        cardinality: { type: 'exact', value: 3 },
      });
      const pattern = AF.createPattern(
        DF.namedNode('s'),
        DF.variable('p'),
        DF.namedNode('o'),
      );
      const bindingsStream = StreamingQuerySourceRdfJs.quadsToBindings(quadStream, pattern, BF, false);

      // Check results
      await expect(bindingsStream).toEqualBindingsStream([
        BF.fromRecord({
          p: DF.namedNode('p1'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.fromRecord({
          p: DF.namedNode('p2'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.fromRecord({
          p: DF.namedNode('p3'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);

      // Check metadata
      const metadata = await new Promise(resolve => bindingsStream.getProperty('metadata', resolve));
      expect(metadata).toEqual({
        canContainUndefs: false,
        cardinality: { type: 'exact', value: 3 },
        variables: [ DF.variable('p') ],
      });
    });

    it('converts triples with available orders metadata', async() => {
      // Prepare data
      const quadStream = new ArrayIterator([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
      ], { autoStart: false });
      quadStream.setProperty('metadata', {
        cardinality: { type: 'exact', value: 3 },
        availableOrders: [
          {
            cost: {
              cardinality: { type: 'exact', value: 123 },
              iterations: 1,
              persistedItems: 2,
              blockingItems: 3,
              requestTime: 4,
            },
            terms: [
              { term: 'subject', direction: 'asc' },
              { term: 'predicate', direction: 'asc' },
            ],
          },
          {
            cost: {
              cardinality: { type: 'exact', value: 456 },
              iterations: 1,
              persistedItems: 2,
              blockingItems: 3,
              requestTime: 4,
            },
            terms: [
              { term: 'object', direction: 'desc' },
              { term: 'graph', direction: 'desc' },
            ],
          },
        ],
      });
      const pattern = AF.createPattern(
        DF.namedNode('s'),
        DF.variable('p'),
        DF.namedNode('o'),
      );
      const bindingsStream = StreamingQuerySourceRdfJs.quadsToBindings(quadStream, pattern, BF, false);

      // Check results
      await expect(bindingsStream).toEqualBindingsStream([
        BF.fromRecord({
          p: DF.namedNode('p1'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.fromRecord({
          p: DF.namedNode('p2'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.fromRecord({
          p: DF.namedNode('p3'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);

      // Check metadata
      const metadata = await new Promise(resolve => bindingsStream.getProperty('metadata', resolve));
      expect(metadata).toEqual({
        canContainUndefs: false,
        cardinality: { type: 'exact', value: 3 },
        variables: [ DF.variable('p') ],
        availableOrders: [
          {
            cost: {
              blockingItems: 3,
              cardinality: {
                type: 'exact',
                value: 123,
              },
              iterations: 1,
              persistedItems: 2,
              requestTime: 4,
            },
            terms: [
              {
                direction: 'asc',
                term: {
                  termType: 'Variable',
                  value: 'p',
                },
              },
            ],
          },
          {
            cost: {
              blockingItems: 3,
              cardinality: {
                type: 'exact',
                value: 456,
              },
              iterations: 1,
              persistedItems: 2,
              requestTime: 4,
            },
            terms: [],
          },
        ],
      });
    });

    it('converts quads', async() => {
      // Prepare data
      const quadStream = new ArrayIterator([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2', 'g1'),
        quad('s3', 'p3', 'o3', 'g2'),
      ], { autoStart: false });
      quadStream.setProperty('metadata', {
        cardinality: { type: 'exact', value: 3 },
        order: [
          { term: 'subject', direction: 'asc' },
          { term: 'predicate', direction: 'asc' },
          { term: 'object', direction: 'asc' },
          { term: 'graph', direction: 'asc' },
        ],
      });
      const pattern = AF.createPattern(
        DF.namedNode('s'),
        DF.variable('p'),
        DF.namedNode('o'),
        DF.variable('g'),
      );
      const bindingsStream = StreamingQuerySourceRdfJs.quadsToBindings(quadStream, pattern, BF, false);

      // Check results
      await expect(bindingsStream).toEqualBindingsStream([
        BF.fromRecord({
          p: DF.namedNode('p2'),
          g: DF.namedNode('g1'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.fromRecord({
          p: DF.namedNode('p3'),
          g: DF.namedNode('g2'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);

      // Check metadata
      const metadata = await new Promise(resolve => bindingsStream.getProperty('metadata', resolve));
      expect(metadata).toEqual({
        canContainUndefs: false,
        cardinality: { type: 'estimate', value: 3 },
        order: [
          { term: DF.variable('p'), direction: 'asc' },
          { term: DF.variable('g'), direction: 'asc' },
        ],
        variables: [ DF.variable('p'), DF.variable('g') ],
      });
    });

    it('converts quads with union default graph', async() => {
      // Prepare data
      const quadStream = new ArrayIterator([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2', 'g1'),
        quad('s3', 'p3', 'o3', 'g2'),
      ], { autoStart: false });
      quadStream.setProperty('metadata', {
        cardinality: { type: 'exact', value: 3 },
        order: [
          { term: 'subject', direction: 'asc' },
          { term: 'predicate', direction: 'asc' },
          { term: 'object', direction: 'asc' },
          { term: 'graph', direction: 'asc' },
        ],
      });
      const pattern = AF.createPattern(
        DF.namedNode('s'),
        DF.variable('p'),
        DF.namedNode('o'),
        DF.variable('g'),
      );
      const bindingsStream = StreamingQuerySourceRdfJs.quadsToBindings(quadStream, pattern, BF, true);

      // Check results
      await expect(bindingsStream).toEqualBindingsStream([
        BF.fromRecord({
          p: DF.namedNode('p1'),
          g: DF.defaultGraph(),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.fromRecord({
          p: DF.namedNode('p2'),
          g: DF.namedNode('g1'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.fromRecord({
          p: DF.namedNode('p3'),
          g: DF.namedNode('g2'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);

      // Check metadata
      const metadata = await new Promise(resolve => bindingsStream.getProperty('metadata', resolve));
      expect(metadata).toEqual({
        canContainUndefs: false,
        cardinality: { type: 'exact', value: 3 },
        order: [
          { term: DF.variable('p'), direction: 'asc' },
          { term: DF.variable('g'), direction: 'asc' },
        ],
        variables: [ DF.variable('p'), DF.variable('g') ],
      });
    });

    it('converts triples with duplicate variables', async() => {
      // Prepare data
      const quadStream = new ArrayIterator([
        quad('s1', 's1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 's1', 'o3'),
      ], { autoStart: false });
      quadStream.setProperty('metadata', {
        cardinality: { type: 'exact', value: 3 },
        order: [
          { term: 'subject', direction: 'asc' },
          { term: 'predicate', direction: 'asc' },
          { term: 'object', direction: 'asc' },
          { term: 'graph', direction: 'asc' },
        ],
      });
      const pattern = AF.createPattern(
        DF.variable('x'),
        DF.variable('x'),
        DF.namedNode('o'),
      );
      const bindingsStream = StreamingQuerySourceRdfJs.quadsToBindings(quadStream, pattern, BF, false);

      // Check results
      await expect(bindingsStream).toEqualBindingsStream([
        BF.fromRecord({
          x: DF.namedNode('s1'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);

      // Check metadata
      const metadata = await new Promise(resolve => bindingsStream.getProperty('metadata', resolve));
      expect(metadata).toEqual({
        canContainUndefs: false,
        cardinality: { type: 'estimate', value: 3 },
        order: [
          { term: DF.variable('x'), direction: 'asc' },
        ],
        variables: [ DF.variable('x') ],
      });
    });

    it('converts quoted triples with duplicate variables', async() => {
      // Prepare data
      const quadStream = new ArrayIterator([
        quad('s1', 'p1', '<<<o1s> <o1p> <o1o>>>'),
        quad('s2', 'p2', '<<<o2s> <o2p> <s2>>>'),
        quad('s3', 'p3', '<<<o3s> <o3p> <o3o>>>'),
      ], { autoStart: false });
      quadStream.setProperty('metadata', {
        cardinality: { type: 'exact', value: 3 },
      });
      const pattern = AF.createPattern(
        DF.variable('x'),
        DF.variable('p'),
        DF.quad(DF.variable('os'), DF.variable('op'), DF.variable('x')),
      );
      const bindingsStream = StreamingQuerySourceRdfJs.quadsToBindings(quadStream, pattern, BF, false);

      // Check results
      await expect(bindingsStream).toEqualBindingsStream([
        BF.fromRecord({
          x: DF.namedNode('s2'),
          p: DF.namedNode('p2'),
          os: DF.namedNode('o2s'),
          op: DF.namedNode('o2p'),
        }).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);

      // Check metadata
      const metadata = await new Promise(resolve => bindingsStream.getProperty('metadata', resolve));
      expect(metadata).toEqual({
        canContainUndefs: false,
        cardinality: { type: 'estimate', value: 3 },
        variables: [ DF.variable('x'), DF.variable('p'), DF.variable('os'), DF.variable('op') ],
      });
    });
  });
});
