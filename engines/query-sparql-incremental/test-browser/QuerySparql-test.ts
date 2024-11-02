/** @jest-environment setup-polly-jest/jest-environment-node */

// Needed to undo automock from actor-http-native, cleaner workarounds do not appear to be working.
import 'jest-rdf';
import '@incremunica/incremental-jest';
import type { EventEmitter } from 'events';
import type { BindingsStream, QueryStringContext } from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import { DevTools } from '@incremunica/dev-tools';
import { StreamingStore } from '@incremunica/incremental-rdf-streaming-store';
import type { Quad } from '@incremunica/incremental-types';
import { DataFactory } from 'rdf-data-factory';
import { QueryEngine } from '../lib/QueryEngine';
import { usePolly } from '../test/util';

async function partialArrayifyStream(stream: EventEmitter, num: number): Promise<any[]> {
  const array: any[] = [];
  for (let i = 0; i < num; i++) {
    await new Promise<void>(resolve => stream.once('data', (bindings: any) => {
      array.push(bindings);
      resolve();
    }));
  }
  return array;
}

if (!globalThis.window) {
  jest.unmock('follow-redirects');
}

const quad = require('rdf-quad');

const DF = new DataFactory();

describe('System test: QuerySparql (without polly)', () => {
  let BF: BindingsFactory;
  let engine: QueryEngine;

  beforeEach(async() => {
    engine = new QueryEngine();
    BF = await DevTools.createTestBindingsFactory(DF);
  });

  describe('using Streaming Store', () => {
    let streamingStore: StreamingStore<Quad>;

    beforeEach(async() => {
      streamingStore = new StreamingStore<Quad>();
    });

    it('simple query', async() => {
      streamingStore.addQuad(quad('s1', 'p1', 'o1'));
      streamingStore.addQuad(quad('s2', 'p2', 'o2'));

      const bindingStream = await engine.queryBindings(`SELECT * WHERE {
          ?s ?p ?o.
          }`, {
        sources: [ streamingStore ],
      });

      expect(await partialArrayifyStream(bindingStream, 2)).toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s1') ],
          [ DF.variable('p'), DF.namedNode('p1') ],
          [ DF.variable('o'), DF.namedNode('o1') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s2') ],
          [ DF.variable('p'), DF.namedNode('p2') ],
          [ DF.variable('o'), DF.namedNode('o2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);

      streamingStore.addQuad(quad('s3', 'p3', 'o3'));

      expect(await partialArrayifyStream(bindingStream, 1)).toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s3') ],
          [ DF.variable('p'), DF.namedNode('p3') ],
          [ DF.variable('o'), DF.namedNode('o3') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);

      streamingStore.removeQuad(quad('s3', 'p3', 'o3'));

      expect(await partialArrayifyStream(bindingStream, 1)).toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s3') ],
          [ DF.variable('p'), DF.namedNode('p3') ],
          [ DF.variable('o'), DF.namedNode('o3') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), false),
      ]);

      streamingStore.end();
    });

    it('query with joins', async() => {
      streamingStore.addQuad(quad('s1', 'p1', 'o1'));
      streamingStore.addQuad(quad('o1', 'p2', 'o2'));

      const bindingStream = await engine.queryBindings(`SELECT * WHERE {
          ?s1 ?p1 ?o1.
          ?o1 ?p2 ?o2.
          }`, {
        sources: [ streamingStore ],
      });

      expect(await partialArrayifyStream(bindingStream, 1)).toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s1'), DF.namedNode('s1') ],
          [ DF.variable('p1'), DF.namedNode('p1') ],
          [ DF.variable('o1'), DF.namedNode('o1') ],
          [ DF.variable('p2'), DF.namedNode('p2') ],
          [ DF.variable('o2'), DF.namedNode('o2') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);

      streamingStore.addQuad(quad('o1', 'p3', 'o3'));

      expect(await partialArrayifyStream(bindingStream, 1)).toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s1'), DF.namedNode('s1') ],
          [ DF.variable('p1'), DF.namedNode('p1') ],
          [ DF.variable('o1'), DF.namedNode('o1') ],
          [ DF.variable('p2'), DF.namedNode('p3') ],
          [ DF.variable('o2'), DF.namedNode('o3') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);

      streamingStore.removeQuad(quad('o1', 'p3', 'o3'));

      expect(await partialArrayifyStream(bindingStream, 1)).toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s1'), DF.namedNode('s1') ],
          [ DF.variable('p1'), DF.namedNode('p1') ],
          [ DF.variable('o1'), DF.namedNode('o1') ],
          [ DF.variable('p2'), DF.namedNode('p3') ],
          [ DF.variable('o2'), DF.namedNode('o3') ],
        ]).setContextEntry(new ActionContextKeyIsAddition(), false),
      ]);

      streamingStore.end();
    });
  });
});

describe('System test: QuerySparql (with polly)', () => {
  usePolly();

  let bindingStream: BindingsStream;
  let engine: QueryEngine;
  beforeEach(async() => {
    engine = new QueryEngine();
    await engine.invalidateHttpCache();
  });

  afterEach(() => {
    bindingStream.destroy();
  });

  describe('simple SPO on a raw RDF document', () => {
    it('with results', async() => {
      bindingStream = await engine.queryBindings(`SELECT * WHERE {
    ?s ?p ?o.
  }`, { sources: [ 'https://www.rubensworks.net/' ]});

      expect(await partialArrayifyStream(bindingStream, 100)).toHaveLength(100);
    });

    it('repeated with the same engine', async() => {
      const query = `SELECT * WHERE {
     ?s ?p ?o.
     }`;
      const context: QueryStringContext = { sources: [ 'https://www.rubensworks.net/' ]};

      bindingStream = await engine.queryBindings(query, context);
      expect(await partialArrayifyStream(bindingStream, 100)).toHaveLength(100);

      bindingStream = await engine.queryBindings(query, context);
      expect(await partialArrayifyStream(bindingStream, 100)).toHaveLength(100);

      bindingStream = await engine.queryBindings(query, context);
      expect(await partialArrayifyStream(bindingStream, 100)).toHaveLength(100);

      bindingStream = await engine.queryBindings(query, context);
      expect(await partialArrayifyStream(bindingStream, 100)).toHaveLength(100);

      bindingStream = await engine.queryBindings(query, context);
      expect(await partialArrayifyStream(bindingStream, 100)).toHaveLength(100);

      bindingStream = await engine.queryBindings(query, context);
      expect(await partialArrayifyStream(bindingStream, 100)).toHaveLength(100);
    });

    it('repeated with the same engine and wait a bit until the polling is removed', async() => {
      const query = `SELECT * WHERE {
     ?s ?p ?o.
     }`;
      const context: QueryStringContext = { sources: [ 'https://www.rubensworks.net/' ]};

      bindingStream = await engine.queryBindings(query, context);
      expect(await partialArrayifyStream(bindingStream, 100)).toHaveLength(100);

      await new Promise<void>(resolve => setTimeout(() => resolve(), 4000));

      bindingStream = await engine.queryBindings(query, context);
      expect(await partialArrayifyStream(bindingStream, 100)).toHaveLength(100);
    });

    describe('simple SPS', () => {
      it('Raw Source', async() => {
        bindingStream = await engine.queryBindings(`SELECT * WHERE {
        ?s ?p ?s.
        }`, { sources: [ 'https://www.rubensworks.net/' ]});

        expect(await partialArrayifyStream(bindingStream, 1)).toHaveLength(1);
      });
    });

    describe('two-pattern query on a raw RDF document', () => {
      it('with results', async() => {
        bindingStream = await engine.queryBindings(`SELECT ?name WHERE {
        <https://www.rubensworks.net/#me> <http://xmlns.com/foaf/0.1/knows> ?v0.
        ?v0 <http://xmlns.com/foaf/0.1/name> ?name.
        }`, { sources: [ 'https://www.rubensworks.net/' ]});

        expect(await partialArrayifyStream(bindingStream, 20)).toHaveLength(20);
      });

      it('for the single source entry', async() => {
        bindingStream = await engine.queryBindings(`SELECT ?name WHERE {
        <https://www.rubensworks.net/#me> <http://xmlns.com/foaf/0.1/knows> ?v0.
        ?v0 <http://xmlns.com/foaf/0.1/name> ?name.
        }`, { sources: [ 'https://www.rubensworks.net/' ]});

        expect(await partialArrayifyStream(bindingStream, 20)).toHaveLength(20);
      });

      describe('SHACL Compact Syntax Serialisation', () => {
        it('handles the query with SHACL compact syntax as a source', async() => {
          bindingStream = await engine.queryBindings(`SELECT * WHERE {
        ?s a <http://www.w3.org/2002/07/owl#Ontology>.
        }`, {
            sources: [
              'https://raw.githubusercontent.com/w3c/data-shapes/gh-pages/shacl-compact-syntax/' +
              'tests/valid/basic-shape-iri.shaclc',
            ],
          });

          expect(await partialArrayifyStream(bindingStream, 1)).toHaveLength(1);
        });
      });
    });
  });
});
