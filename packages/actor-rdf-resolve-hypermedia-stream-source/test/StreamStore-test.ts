import {ActionContext, Bus} from '@comunica/core';
import { ActorRdfResolveHypermediaStreamSource } from '../lib/ActorRdfResolveHypermediaStreamSource';
import { StreamStore } from '../lib/StreamStore';
import {DataFactory} from "rdf-data-factory";
import {ActorRdfResolveHypermedia} from "@comunica/bus-rdf-resolve-hypermedia";
import {IActionContext} from "@comunica/types";
import {Readable} from "stream";
import 'jest-rdf';
const quad = require('rdf-quad');
import arrayifyStream from "arrayify-stream";
import {Transform} from "readable-stream";
import {Quad} from "@rdfjs/types";
const streamifyArray = require('streamify-array');

describe('StreamStore', () => {
  describe("Testing functionality", () => {
    beforeEach(() => {
    });

    it("test", () => {
      let quads = streamifyArray([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
      ]);
      let store = new StreamStore(quads);

      let test = new Transform({
        transform(chunk: Quad, encoding: BufferEncoding, callback: (error?: (Error | null), data?: any) => void) {
          console.log(chunk);
        },
        objectMode: true
      })

      store.match(undefined, undefined, undefined, undefined).pipe(test);
    });
  });

  /*
  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('The StreamStore module', () => {
    it('should be a function', () => {
      expect(ActorRdfResolveHypermediaStreamSource).toBeInstanceOf(Function);
    });

    it('should be a ActorRdfResolveHypermediaStreamSource constructor', () => {
      expect(new (<any> ActorRdfResolveHypermediaStreamSource)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorRdfResolveHypermediaStreamSource);
      expect(new (<any> ActorRdfResolveHypermediaStreamSource)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorRdfResolveHypermedia);
    });

    it('should not be able to create new ActorRdfResolveHypermediaStreamSource objects without \'new\'', () => {
      expect(() => { (<any> ActorRdfResolveHypermediaStreamSource)(); }).toThrow();
    });
  });
   */
});
