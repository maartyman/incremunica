/** @jest-environment setup-polly-jest/jest-environment-node */

// Needed to undo automock from actor-http-native, cleaner workarounds do not appear to be working.
import 'jest-rdf';
import arrayifyStream from 'arrayify-stream';
import { DataFactory } from 'rdf-data-factory';
import type { BindingsStream, QueryStringContext} from '@comunica/types';
import {Factory} from 'sparqlalgebrajs';
import {QueryEngine} from '../lib/QueryEngine';
import {mockHttp, usePolly} from './util';
import {Quad} from "@comunica/incremental-types";
import {EventEmitter} from "events";
import * as http from "http";

if (!globalThis.window) {
  jest.unmock('follow-redirects');
}

const stringifyStream = require('stream-to-string');

const DF = new DataFactory();
const factory = new Factory();

describe('System test: QuerySparql (without polly)', () => {
  let engine: QueryEngine;
  beforeEach(() => {
    engine = new QueryEngine();
  });

  describe('simple static queries', () => {
    let fetchData = {
      dataString: "",
      etag: "0",
      "cache-control": "max-age=2",
      age: "1"
    }
    let server: http.Server;
    let bindingStream: BindingsStream;

    beforeEach(async () => {
      server = http.createServer((req, res) => {
        if (req.method == "HEAD") {
          res.writeHead(200, "OK", {
            "etag": fetchData.etag,
            "content-type": "text/turtle",
            "cache-control": fetchData["cache-control"],
            age: fetchData.age
          });
        } else {
          res.setHeader("etag", fetchData.etag);
          res.setHeader("content-type", "text/turtle");
          res.setHeader("cache-control", "cache-control");
          res.setHeader("age", "fetchData.age");
          res.write(fetchData.dataString);
        }
        res.end();
      });

      await new Promise<void>(resolve => server.listen(8787, 'localhost', () => {
        resolve();
      }));
    });

    afterEach(async () => {
      bindingStream.destroy();
      await new Promise<void>(resolve => server.close(() => {
        resolve();
      }));
    })

    it('simple query', async () => {
      fetchData.dataString = "<s1> <p1> <o1> .";
      fetchData.etag = "0";

      bindingStream = await engine.queryBindings(`SELECT * WHERE {
          ?s ?p ?o.
          }`, {
        sources: ['http://localhost:8787']
      });

      await new Promise<void>((resolve) => bindingStream.once("data", async (bindings) => {
        expect(true).toEqual(true);
        resolve();
      }));
    });
  });

  describe('simple update queries', () => {
    let fetchData = {
      dataString: "",
      etag: "0"
    }
    let eventEmitter = new EventEmitter();
    let server: http.Server;
    let bindingStream: BindingsStream;

    beforeEach(async () => {
      server = http.createServer((req, res) => {
        if (req.method == "HEAD") {
          res.writeHead(200, "OK", {
            "etag": fetchData.etag,
            "content-type": "text/turtle"
          });
        } else {
          res.setHeader("etag", fetchData.etag);
          res.setHeader("content-type", "text/turtle");
          res.write(fetchData.dataString);
        }
        res.end();
      });

      await new Promise<void>(resolve => server.listen(6565, 'localhost', () => {
        resolve();
      }));
    });

    afterEach(async () => {
      bindingStream.destroy();
      await new Promise<void>(resolve => server.close(() => {
        resolve();
      }));
    })

    it('simple addition update query', async () => {
      fetchData.dataString = "<s1> <p1> <o1> .";
      fetchData.etag = "0";

      bindingStream = await engine.queryBindings(`SELECT * WHERE {
          ?s ?p ?o.
          }`, {
        sources: ['http://localhost:6565']
      });

      await new Promise<void>((resolve) => bindingStream.once("data", async (bindings) => {
        expect(true).toEqual(true);
        resolve();
      }));

      fetchData.dataString = "<s1> <p1> <o1> . <s2> <p2> <o2> .";
      fetchData.etag = "1";

      await new Promise<void>((resolve) => bindingStream.once("data", async (bindings) => {
        expect(true).toEqual(true);
        resolve();
      }));
    });

    /*
    it('simple addition update query with optional', async () => {
      fetchData.dataString = "<s1> <p1> <o1> .";
      fetchData.etag = "0";

      bindingStream = await engine.queryBindings(`SELECT * WHERE {
          ?s <http://localhost:6565/p1> ?o .
          OPTIONAL { ?s <http://localhost:6565/p2> ?o . }
          }`, {
        sources: ['http://localhost:6565']
      });

      await new Promise<void>((resolve) => bindingStream.once("data", async (bindings) => {
        console.log("s: ", bindings.get("s"), "p: ", bindings.get("p"), "o: ", bindings.get("o"));
        expect(true).toEqual(true);
        resolve();
      }));

      fetchData.dataString = "<s> <p1> <o> . <s> <p2> <o> .";
      fetchData.etag = "1";

      await new Promise<void>((resolve) => bindingStream.once("data", async (bindings) => {
        console.log("s: ", bindings.get("s"), "p: ", bindings.get("p"), "o: ", bindings.get("o"));
        expect(true).toEqual(true);
        resolve();
      }));
    });
     */
  });
});

describe('System test: QuerySparql (with polly)', () => {
  usePolly();

  let bindingStream: BindingsStream;
  let engine: QueryEngine;
  beforeEach(() => {
    engine = new QueryEngine();
    engine.invalidateHttpCache();
  });

  afterEach(() => {
    bindingStream.destroy();
  })

  describe('simple SPO on a raw RDF document', () => {
    it('with results', async() => {
      bindingStream = await engine.queryBindings(`SELECT * WHERE {
    ?s ?p ?o.
  }`, { sources: [ 'https://www.rubensworks.net/' ]});
      let count = 0;

      await new Promise<void>((resolve) => bindingStream.on("data", async () => {
        count++;
        if (count > 100) {
          expect(true).toEqual(true);
          bindingStream.destroy();
          resolve();
        }
      }));
    });

    it('repeated with the same engine', async() => {
      const query = `SELECT * WHERE {
     ?s ?p ?o.
     }`;
      const context: QueryStringContext = { sources: [ 'https://www.rubensworks.net/' ]};

      let count = 0;
      bindingStream = await engine.queryBindings(query, context);
      await new Promise<void>(async (resolve) => bindingStream.on("data", async () => {
        count++;
        if (count > 100) {
          expect(true).toEqual(true);
          bindingStream.destroy();
          resolve();
        }
      }));

      count = 0;
      bindingStream = await engine.queryBindings(query, context);
      await new Promise<void>(async (resolve) => bindingStream.on("data", async () => {
        count++;
        if (count > 100) {
          expect(true).toEqual(true);
          bindingStream.destroy();
          resolve();
        }
      }));

      count = 0;
      bindingStream = await engine.queryBindings(query, context);
      await new Promise<void>(async (resolve) => bindingStream.on("data", async () => {
        count++;
        if (count > 100) {
          expect(true).toEqual(true);
          bindingStream.destroy();
          resolve();
        }
      }));

      count = 0;
      bindingStream = await engine.queryBindings(query, context);
      await new Promise<void>(async (resolve) => bindingStream.on("data", async () => {
        count++;
        if (count > 100) {
          expect(true).toEqual(true);
          bindingStream.destroy();
          resolve();
        }
      }));

      count = 0;
      bindingStream = await engine.queryBindings(query, context);
      await new Promise<void>(async (resolve) => bindingStream.on("data", async () => {
        count++;
        if (count > 100) {
          expect(true).toEqual(true);
          bindingStream.destroy();
          resolve();
        }
      }));

      count = 0;
      bindingStream = await engine.queryBindings(query, context);
      await new Promise<void>(async (resolve) => bindingStream.on("data", async () => {
        count++;
        if (count > 100) {
          expect(true).toEqual(true);
          bindingStream.destroy();
          resolve();
        }
      }));
    });

    it('repeated with the same engine and wait a bit until the polling is removed', async() => {
      const query = `SELECT * WHERE {
     ?s ?p ?o.
     }`;
      const context: QueryStringContext = { sources: [ 'https://www.rubensworks.net/' ]};

      let count = 0;
      bindingStream = await engine.queryBindings(query, context);
      await new Promise<void>(async (resolve) => bindingStream.on("data", async () => {
        count++;
        if (count > 100) {
          expect(true).toEqual(true);
          bindingStream.destroy();
          resolve();
        }
      }));

      await new Promise<void>((resolve) => setTimeout(()=>resolve(),10000));

      count = 0;
      bindingStream = await engine.queryBindings(query, context);
      await new Promise<void>(async (resolve) => bindingStream.on("data", async () => {
        count++;
        if (count > 100) {
          expect(true).toEqual(true);
          bindingStream.destroy();
          resolve();
        }
      }));
    });

    describe('simple SPS', () => {
      it('Raw Source', async() => {
        bindingStream = await engine.queryBindings(`SELECT * WHERE {
        ?s ?p ?s.
        }`, { sources: [ 'https://www.rubensworks.net/' ]});

        await new Promise<void>((resolve) => bindingStream.on("data", async () => {
          expect(true).toEqual(true);
          bindingStream.destroy();
          resolve();
        }));
      });
    });

    describe('two-pattern query on a raw RDF document', () => {
      it('with results', async() => {
        bindingStream = await engine.queryBindings(`SELECT ?name WHERE {
        <https://www.rubensworks.net/#me> <http://xmlns.com/foaf/0.1/knows> ?v0.
        ?v0 <http://xmlns.com/foaf/0.1/name> ?name.
        }`, { sources: [ 'https://www.rubensworks.net/' ]});

        let count = 0;
        await new Promise<void>((resolve) => bindingStream.on("data", async () => {
          count++;
          if (count > 20) {
            expect(true).toEqual(true);
            bindingStream.destroy();
            resolve();
          }
        }));
      });

      it('for the single source entry', async() => {
        bindingStream = await engine.queryBindings(`SELECT ?name WHERE {
        <https://www.rubensworks.net/#me> <http://xmlns.com/foaf/0.1/knows> ?v0.
        ?v0 <http://xmlns.com/foaf/0.1/name> ?name.
        }`, { sources: [ 'https://www.rubensworks.net/' ]});

        let count = 0;
        await new Promise<void>((resolve) => bindingStream.on("data", async () => {
          count++;
          if (count > 20) {
            expect(true).toEqual(true);
            bindingStream.destroy();
            resolve();
          }
        }));
      });

      describe('SHACL Compact Syntax Serialisation', () => {
        it('handles the query with SHACL compact syntax as a source', async () => {
          bindingStream = await engine.queryBindings(`SELECT * WHERE {
        ?s a <http://www.w3.org/2002/07/owl#Ontology>.
        }`, {
            sources: [
              'https://raw.githubusercontent.com/w3c/data-shapes/gh-pages/shacl-compact-syntax/' +
              'tests/valid/basic-shape-iri.shaclc',
            ]
          });

          let count = 0;
          await new Promise<void>((resolve) => bindingStream.on("data", async () => {
            count++;
            if (count > 0) {
              expect(true).toEqual(true);
              bindingStream.destroy();
              resolve();
            }
          }));
        });
      });
    });
  });
});
