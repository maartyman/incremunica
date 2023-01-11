import {ActionContext, Bus} from '@comunica/core';
import { ActorRdfResolveHypermediaStreamSource } from '../lib/ActorRdfResolveHypermediaStreamSource';
import {DataFactory} from "rdf-data-factory";
import {ActorRdfResolveHypermedia} from "@comunica/bus-rdf-resolve-hypermedia";
import {IActionContext} from "@comunica/types";
import arrayifyStream from "arrayify-stream";
import {Readable} from "stream";
import 'jest-rdf';

const quad = require('rdf-quad');
const streamifyArray = require('streamify-array');

const DF = new DataFactory();
const v = DF.variable('v');

describe('ActorRdfResolveHypermediaStreamSource', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('The ActorRdfResolveHypermediaStreamSource module', () => {
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

  describe('An ActorRdfResolveHypermediaStreamSource instance', () => {
    let actor: ActorRdfResolveHypermediaStreamSource;
    let context: IActionContext;

    beforeEach(() => {
      actor = new ActorRdfResolveHypermediaStreamSource({ name: 'actor', bus });
      context = new ActionContext();
    });

    it('should test', () => {
      return expect(actor.test({ metadata: <any> null, quads: <any> null, url: '', context }))
        .resolves.toEqual({ filterFactor: 0 });
    });

    it('should run', async() => {
      const quads = streamifyArray([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
      ]);
      const { source } = await actor.run({ metadata: <any> null, quads, url: '', context });
      expect(source.match).toBeTruthy();
      await expect(new Promise((resolve, reject) => {
        const stream = source.match(v, v, v, v);
        stream.getProperty('metadata', resolve);
      })).resolves.toEqual({ cardinality: { type: 'exact', value: 2 }, canContainUndefs: false });
      expect(await arrayifyStream(source.match(v, v, v, v))).toEqualRdfQuadArray([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
      ]);
    });

    it('should run and delegate error events', async() => {
      const quads = streamifyArray([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
      ]);
      await expect(new Promise(async(resolve, reject) => {
        const { source } = await actor.run({ metadata: <any> null, quads, url: '', context });
        (<any> source).source.match = () => {
          const str = new Readable();
          str._read = () => {
            str.emit('error', new Error('Dummy error'));
          };
          return str;
        };
        const stream = source.match(v, v, v, v);
        stream.on('error', resolve);
        stream.on('data', () => {
          // Do nothing
        });
        stream.on('end', () => reject(new Error('Got no error event.')));
      })).resolves.toEqual(new Error('Dummy error'));
    });
  });
});
