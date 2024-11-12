/** @jest-environment setup-polly-jest/jest-environment-node */

// Needed to undo automock from actor-http-native, cleaner workarounds do not appear to be working.
import 'jest-rdf';
import '@incremunica/incremental-jest';
import type { EventEmitter } from 'node:events';
import * as http from 'node:http';
import type { Bindings, BindingsStream, QueryStringContext } from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { KeysBindings } from '@incremunica/context-entries';
import { DevTools } from '@incremunica/dev-tools';
import { StreamingStore } from '@incremunica/incremental-rdf-streaming-store';
import type { Quad } from '@incremunica/incremental-types';
import { DataFactory } from 'rdf-data-factory';
import { QueryEngine, QueryEngineFactory } from '../lib';
import { usePolly } from './util';

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

      await expect(partialArrayifyStream(bindingStream, 2)).resolves.toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s1') ],
          [ DF.variable('p'), DF.namedNode('p1') ],
          [ DF.variable('o'), DF.namedNode('o1') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s2') ],
          [ DF.variable('p'), DF.namedNode('p2') ],
          [ DF.variable('o'), DF.namedNode('o2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);

      streamingStore.addQuad(quad('s3', 'p3', 'o3'));

      await expect(partialArrayifyStream(bindingStream, 1)).resolves.toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s3') ],
          [ DF.variable('p'), DF.namedNode('p3') ],
          [ DF.variable('o'), DF.namedNode('o3') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);

      streamingStore.removeQuad(quad('s3', 'p3', 'o3'));

      await expect(partialArrayifyStream(bindingStream, 1)).resolves.toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s3') ],
          [ DF.variable('p'), DF.namedNode('p3') ],
          [ DF.variable('o'), DF.namedNode('o3') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
      ]);

      streamingStore.end();
    });

    it('simple query with QueryEngineFactory', async() => {
      engine = await new QueryEngineFactory().create();

      streamingStore.addQuad(quad('s1', 'p1', 'o1'));
      streamingStore.addQuad(quad('s2', 'p2', 'o2'));

      const bindingStream = await engine.queryBindings(`SELECT * WHERE {
          ?s ?p ?o.
          }`, {
        sources: [ streamingStore ],
      });

      await expect(partialArrayifyStream(bindingStream, 2)).resolves.toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s1') ],
          [ DF.variable('p'), DF.namedNode('p1') ],
          [ DF.variable('o'), DF.namedNode('o1') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s2') ],
          [ DF.variable('p'), DF.namedNode('p2') ],
          [ DF.variable('o'), DF.namedNode('o2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);

      streamingStore.addQuad(quad('s3', 'p3', 'o3'));

      await expect(partialArrayifyStream(bindingStream, 1)).resolves.toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s3') ],
          [ DF.variable('p'), DF.namedNode('p3') ],
          [ DF.variable('o'), DF.namedNode('o3') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);

      streamingStore.removeQuad(quad('s3', 'p3', 'o3'));

      await expect(partialArrayifyStream(bindingStream, 1)).resolves.toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s'), DF.namedNode('s3') ],
          [ DF.variable('p'), DF.namedNode('p3') ],
          [ DF.variable('o'), DF.namedNode('o3') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
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

      await expect(partialArrayifyStream(bindingStream, 1)).resolves.toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s1'), DF.namedNode('s1') ],
          [ DF.variable('p1'), DF.namedNode('p1') ],
          [ DF.variable('o1'), DF.namedNode('o1') ],
          [ DF.variable('p2'), DF.namedNode('p2') ],
          [ DF.variable('o2'), DF.namedNode('o2') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);

      streamingStore.addQuad(quad('o1', 'p3', 'o3'));

      await expect(partialArrayifyStream(bindingStream, 1)).resolves.toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s1'), DF.namedNode('s1') ],
          [ DF.variable('p1'), DF.namedNode('p1') ],
          [ DF.variable('o1'), DF.namedNode('o1') ],
          [ DF.variable('p2'), DF.namedNode('p3') ],
          [ DF.variable('o2'), DF.namedNode('o3') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ]);

      streamingStore.removeQuad(quad('o1', 'p3', 'o3'));

      await expect(partialArrayifyStream(bindingStream, 1)).resolves.toBeIsomorphicBindingsArray([
        BF.bindings([
          [ DF.variable('s1'), DF.namedNode('s1') ],
          [ DF.variable('p1'), DF.namedNode('p1') ],
          [ DF.variable('o1'), DF.namedNode('o1') ],
          [ DF.variable('p2'), DF.namedNode('p3') ],
          [ DF.variable('o2'), DF.namedNode('o3') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
      ]);

      streamingStore.end();
    });
  });

  describe('simple hypermedia queries', () => {
    const fetchData = {
      dataString: '',
      etag: '0',
      'cache-control': 'max-age=2',
      age: '1',
    };
    let server: http.Server;
    let bindingStream: BindingsStream;

    beforeEach(async() => {
      server = http.createServer((req, res) => {
        if (req.method === 'HEAD') {
          res.writeHead(200, 'OK', {
            etag: fetchData.etag,
            'content-type': 'text/turtle',
            'cache-control': fetchData['cache-control'],
            age: fetchData.age,
          });
        } else {
          res.setHeader('etag', fetchData.etag);
          res.setHeader('content-type', 'text/turtle');
          res.setHeader('cache-control', fetchData['cache-control']);
          res.setHeader('age', fetchData.age);
          res.write(fetchData.dataString);
        }
        res.end();
      });

      await new Promise<void>(resolve => server.listen(8787, 'localhost', () => {
        resolve();
      }));
    });

    afterEach(async() => {
      bindingStream.destroy();
      await new Promise<void>(resolve => server.close(() => {
        resolve();
      }));
      await new Promise<void>(resolve => setTimeout(() => resolve(), 500));
    });

    it('simple query', async() => {
      fetchData.dataString = '<http://localhost:8787/s1> <http://localhost:8787/p1> <http://localhost:8787/o1> .';
      fetchData.etag = '0';

      bindingStream = await engine.queryBindings(`SELECT * WHERE {
          ?s ?p ?o.
          }`, {
        sources: [ 'http://localhost:8787' ],
        pollingFrequency: 1000,
      });

      await expect(new Promise<Bindings>(resolve => bindingStream.once('data', (bindings) => {
        resolve(bindings);
      }))).resolves.toEqualBindings(BF.bindings([
        [ DF.variable('s'), DF.namedNode('http://localhost:8787/s1') ],
        [ DF.variable('p'), DF.namedNode('http://localhost:8787/p1') ],
        [ DF.variable('o'), DF.namedNode('http://localhost:8787/o1') ],
      ]).setContextEntry(KeysBindings.isAddition, true));
    });

    it('simple addition update query', async() => {
      fetchData.dataString = '<http://localhost:8787/s1> <http://localhost:8787/p1> <http://localhost:8787/o1> .';
      fetchData.etag = '0';

      bindingStream = await engine.queryBindings(`SELECT * WHERE {
          ?s ?p ?o.
          }`, {
        sources: [ 'http://localhost:8787' ],
      });

      await expect(new Promise<Bindings>(resolve => bindingStream.once('data', (bindings) => {
        resolve(bindings);
      }))).resolves.toEqualBindings(BF.bindings([
        [ DF.variable('s'), DF.namedNode('http://localhost:8787/s1') ],
        [ DF.variable('p'), DF.namedNode('http://localhost:8787/p1') ],
        [ DF.variable('o'), DF.namedNode('http://localhost:8787/o1') ],
      ]).setContextEntry(KeysBindings.isAddition, true));

      fetchData.dataString += '\n<http://localhost:8787/s2> <http://localhost:8787/p2> <http://localhost:8787/o2> .';
      fetchData.etag = '1';

      await expect(new Promise<Bindings>(resolve => bindingStream.once('data', (bindings) => {
        resolve(bindings);
      }))).resolves.toEqualBindings(BF.bindings([
        [ DF.variable('s'), DF.namedNode('http://localhost:8787/s2') ],
        [ DF.variable('p'), DF.namedNode('http://localhost:8787/p2') ],
        [ DF.variable('o'), DF.namedNode('http://localhost:8787/o2') ],
      ]).setContextEntry(KeysBindings.isAddition, true));
    });

    it('simple deletion update query', async() => {
      fetchData.dataString = '<http://localhost:8787/s1> <http://localhost:8787/p1> <http://localhost:8787/o1> .';
      fetchData.etag = '0';

      bindingStream = await engine.queryBindings(`SELECT * WHERE {
          ?s ?p ?o.
          }`, {
        sources: [ 'http://localhost:8787' ],
        pollingFrequency: 1000,
      });

      await expect(new Promise<Bindings>(resolve => bindingStream.once('data', (bindings) => {
        resolve(bindings);
      }))).resolves.toEqualBindings(BF.bindings([
        [ DF.variable('s'), DF.namedNode('http://localhost:8787/s1') ],
        [ DF.variable('p'), DF.namedNode('http://localhost:8787/p1') ],
        [ DF.variable('o'), DF.namedNode('http://localhost:8787/o1') ],
      ]).setContextEntry(KeysBindings.isAddition, true));

      fetchData.dataString = '';
      fetchData.etag = '1';

      await expect(new Promise<Bindings>(resolve => bindingStream.once('data', (bindings) => {
        resolve(bindings);
      }))).resolves.toEqualBindings(BF.bindings([
        [ DF.variable('s'), DF.namedNode('http://localhost:8787/s1') ],
        [ DF.variable('p'), DF.namedNode('http://localhost:8787/p1') ],
        [ DF.variable('o'), DF.namedNode('http://localhost:8787/o1') ],
      ]).setContextEntry(KeysBindings.isAddition, false));
    });

    it('simple addition update query with optional', async() => {
      fetchData.dataString = '<http://localhost:8787/s1> <http://localhost:8787/p1> <http://localhost:8787/o1> .';
      fetchData.etag = '0';
      bindingStream = await engine.queryBindings(`SELECT * WHERE {
           ?s1 <http://localhost:8787/p1> ?o1 .
           OPTIONAL { ?s2 <http://localhost:8787/p2> ?o2 . }
           }`, {
        sources: [ 'http://localhost:8787' ],
        pollingFrequency: 1000,
      });

      await expect(new Promise<Bindings>(resolve => bindingStream.once('data', (bindings) => {
        resolve(bindings);
      }))).resolves.toEqualBindings(BF.bindings([
        [ DF.variable('s1'), DF.namedNode('http://localhost:8787/s1') ],
        [ DF.variable('o1'), DF.namedNode('http://localhost:8787/o1') ],
      ]).setContextEntry(KeysBindings.isAddition, true));

      fetchData.dataString = '<http://localhost:8787/s1> <http://localhost:8787/p1> <http://localhost:8787/o1> . <http://localhost:8787/s2> <http://localhost:8787/p2> <http://localhost:8787/o2> .';
      fetchData.etag = '1';

      await expect(new Promise<Bindings>(resolve => bindingStream.once('data', (bindings) => {
        resolve(bindings);
      }))).resolves.toEqualBindings(BF.bindings([
        [ DF.variable('s1'), DF.namedNode('http://localhost:8787/s1') ],
        [ DF.variable('o1'), DF.namedNode('http://localhost:8787/o1') ],
      ]).setContextEntry(KeysBindings.isAddition, false));

      await expect(new Promise<Bindings>(resolve => bindingStream.once('data', (bindings) => {
        resolve(bindings);
      }))).resolves.toEqualBindings(BF.bindings([
        [ DF.variable('s1'), DF.namedNode('http://localhost:8787/s1') ],
        [ DF.variable('o1'), DF.namedNode('http://localhost:8787/o1') ],
        [ DF.variable('s2'), DF.namedNode('http://localhost:8787/s2') ],
        [ DF.variable('o2'), DF.namedNode('http://localhost:8787/o2') ],
      ]).setContextEntry(KeysBindings.isAddition, true));
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
  }`, {
        sources: [ 'https://www.rubensworks.net/' ],
        pollingFrequency: 1000,
      });

      await expect((partialArrayifyStream(bindingStream, 100))).resolves.toHaveLength(100);
    });

    it('repeated with the same engine', async() => {
      const query = `SELECT * WHERE {
     ?s ?p ?o.
     }`;
      const context: QueryStringContext = {
        pollingFrequency: 1000,
        sources: [ 'https://www.rubensworks.net/' ],
      };

      for (let i = 0; i < 5; i++) {
        bindingStream = await engine.queryBindings(query, context);
        await expect((partialArrayifyStream(bindingStream, 100))).resolves.toHaveLength(100);
        bindingStream.destroy();
      }
    });

    it('repeated with the same engine and wait a bit until the polling is removed', async() => {
      const query = `SELECT * WHERE {
     ?s ?p ?o.
     }`;
      const context: QueryStringContext = {
        sources: [ 'https://www.rubensworks.net/' ],
        pollingFrequency: 1000,
      };

      bindingStream = await engine.queryBindings(query, context);
      await expect((partialArrayifyStream(bindingStream, 100))).resolves.toHaveLength(100);
      bindingStream.destroy();

      await new Promise<void>(resolve => setTimeout(() => resolve(), 1000));

      bindingStream = await engine.queryBindings(query, context);
      await expect((partialArrayifyStream(bindingStream, 100))).resolves.toHaveLength(100);
      bindingStream.destroy();
    });

    describe('simple SPS', () => {
      it('Raw Source', async() => {
        bindingStream = await engine.queryBindings(`SELECT * WHERE {
        ?s ?p ?s.
        }`, {
          sources: [ 'https://www.rubensworks.net/' ],
          pollingFrequency: 1000,
        });

        await expect((partialArrayifyStream(bindingStream, 1))).resolves.toHaveLength(1);
      });
    });

    describe('two-pattern query on a raw RDF document', () => {
      it('with results', async() => {
        bindingStream = await engine.queryBindings(`SELECT ?name WHERE {
        <https://www.rubensworks.net/#me> <http://xmlns.com/foaf/0.1/knows> ?v0.
        ?v0 <http://xmlns.com/foaf/0.1/name> ?name.
        }`, {
          sources: [ 'https://www.rubensworks.net/' ],
          pollingFrequency: 1000,
        });

        await expect((partialArrayifyStream(bindingStream, 20))).resolves.toHaveLength(20);
      });

      it('for the single source entry', async() => {
        bindingStream = await engine.queryBindings(`SELECT ?name WHERE {
        <https://www.rubensworks.net/#me> <http://xmlns.com/foaf/0.1/knows> ?v0.
        ?v0 <http://xmlns.com/foaf/0.1/name> ?name.
        }`, {
          sources: [ 'https://www.rubensworks.net/' ],
          pollingFrequency: 1000,
        });

        await expect((partialArrayifyStream(bindingStream, 20))).resolves.toHaveLength(20);
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
            pollingFrequency: 1000,
          });

          await expect((partialArrayifyStream(bindingStream, 1))).resolves.toHaveLength(1);
        });
      });
    });
  });
});
