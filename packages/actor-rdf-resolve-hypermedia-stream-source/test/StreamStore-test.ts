import { StreamStore } from '../lib/StreamStore';
import 'jest-rdf';

const quad = require('rdf-quad');
import {Quad} from "@rdfjs/types";
import {ActorRdfResolveHypermediaNone} from "@comunica/actor-rdf-resolve-hypermedia-none";
import {ActorRdfResolveHypermedia} from "@comunica/bus-rdf-resolve-hypermedia";
import {Bus} from "@comunica/core";
const streamifyArray = require('streamify-array');

describe('StreamStore', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('The StreamStore module', () => {
    it('should be a function', () => {
      expect(StreamStore).toBeInstanceOf(Function);
    });

    it('should be a ActorRdfResolveHypermediaNone constructor', () => {
      expect(new (<any> StreamStore)())
        .toBeInstanceOf(StreamStore);
      expect(new (<any> StreamStore)())
        .toBeInstanceOf(StreamStore);
    });

    it('should not be able to create new ActorRdfResolveHypermediaNone objects without \'new\'', () => {
      expect(() => { (<any> ActorRdfResolveHypermediaNone)(); }).toThrow();
    });
  });

  /*
  describe("Testing functionality", () => {
    beforeEach(() => {
    });

    it("test", async () => {
      let array: Quad[] = [];

      await new Promise<void>((resolve) => {
        let quads = streamifyArray([
          quad('s1', 'p1', 'o1'),
          quad('s2', 'p2', 'o2')
        ]);
        let store = new StreamStore(quads);

        store.match(undefined, undefined, undefined, undefined);

        let quads2 = streamifyArray([
          quad('s3', 'p3', 'o3'),
          quad('s4', 'p4', 'o4')
        ]);

        store.attachStream(quads2);
      });

      expect(array).toEqual([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4')
      ])
    });
  });

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
  });

   */
});
