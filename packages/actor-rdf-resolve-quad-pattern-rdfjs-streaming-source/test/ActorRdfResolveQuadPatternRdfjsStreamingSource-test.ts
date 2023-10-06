import {ActionContext, Bus} from '@comunica/core';
import { ActorRdfResolveQuadPatternRdfjsStreamingSource } from '../lib/ActorRdfResolveQuadPatternRdfjsStreamingSource';
import {IActionContext} from "@comunica/types";
import * as RDF from "rdf-js";
import { KeysRdfResolveQuadPattern } from '@comunica/context-entries';
import {DataFactory} from "rdf-data-factory";
import arrayifyStream from 'arrayify-stream';
import {RdfJsQuadStreamingSource} from "../lib";
import {Store} from "n3";
import 'jest-rdf';
import {StreamingStore} from "@incremunica/incremental-rdf-streaming-store";

import {promisifyEventEmitter} from "event-emitter-promisify/dist";
import {MetadataValidationState} from "@comunica/metadata";
import {KeysGuard, KeysStreamingSource} from "@incremunica/context-entries";
import EventEmitter = require("events");

const quad = require('rdf-quad');
const streamifyArray = require('streamify-array');
const DF = new DataFactory();

describe('ActorRdfResolveQuadPatternRdfjsStreamingSource', () => {
  let bus: any;
  let context: IActionContext;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    context = new ActionContext();
  });

  describe('The ActorRdfResolveQuadPatternRdfjsStreamingSource module', () => {
    it('should be a function', () => {
      expect(ActorRdfResolveQuadPatternRdfjsStreamingSource).toBeInstanceOf(Function);
    });

    it('should be a ActorRdfResolveQuadPatternRdfJsSource constructor', () => {
      expect(new (<any> ActorRdfResolveQuadPatternRdfjsStreamingSource)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorRdfResolveQuadPatternRdfjsStreamingSource);
      expect(new (<any> ActorRdfResolveQuadPatternRdfjsStreamingSource)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorRdfResolveQuadPatternRdfjsStreamingSource);
    });

    it('should not be able to create new ActorRdfResolveQuadPatternRdfJsSource objects without \'new\'', () => {
      expect(() => { (<any> ActorRdfResolveQuadPatternRdfjsStreamingSource)(); }).toThrow();
    });
  });

  describe('An ActorRdfResolveQuadPatternRdfjsStreamingSource instance', () => {
    let actor: ActorRdfResolveQuadPatternRdfjsStreamingSource;
    let source: RDF.Source;

    beforeEach(() => {
      actor = new ActorRdfResolveQuadPatternRdfjsStreamingSource({ name: 'actor', bus });
      source = { match: () => <any> null };
      Object.setPrototypeOf(source, StreamingStore.prototype);
    });

    it('should test', () => {
      return expect(actor.test({ pattern: <any> null,
        context: new ActionContext(
          { [KeysRdfResolveQuadPattern.source.name]: { type: 'rdfjsSource', value: source }},
        ) }))
        .resolves.toBeTruthy();
    });

    it('should test on raw source form', () => {
      return expect(actor.test({ pattern: <any> null,
        context: new ActionContext(
          { [KeysRdfResolveQuadPattern.source.name]: source },
        ) }))
        .resolves.toBeTruthy();
    });

    /*
    it('should not test with a normal store', () => {
      return expect(actor.test({ pattern: <any> null,
        context: new ActionContext(
          { [KeysRdfResolveQuadPattern.source.name]: { type: 'rdfjsSource', value: new Store() }},
        ) }))
        .rejects.toEqual(new Error("actor didn't receive a StreamingStore."))
    });
    */

    it('should not test without a source', () => {
      return expect(actor.test({ pattern: <any> null, context: new ActionContext({}) })).rejects.toBeTruthy();
    });

    it('should not test on an invalid source', () => {
      return expect(actor.test({ pattern: <any> null,
        context: new ActionContext(
          { [KeysRdfResolveQuadPattern.source.name]: { type: 'rdfjsSource', value: undefined }},
        ) }))
        .rejects.toBeTruthy();
    });

    it('should not test on an invalid source type', () => {
      return expect(actor.test({ pattern: <any> null,
        context: new ActionContext(
          { [KeysRdfResolveQuadPattern.source.name]: { type: 'rdfjsSource', value: {}}},
        ) }))
        .rejects.toBeTruthy();
    });

    it('should not test on no source', () => {
      return expect(actor.test({ pattern: <any> null,
        context: new ActionContext(
          { [KeysRdfResolveQuadPattern.source.name]: { type: 'entrypoint', value: null }},
        ) }))
        .rejects.toBeTruthy();
    });

    it('should not test on no sources', () => {
      return expect(actor.test({ pattern: <any> null,
        context: new ActionContext(
          { '@comunica/bus-rdf-resolve-quad-pattern:sources': []},
        ) }))
        .rejects.toBeTruthy();
    });

    it('should not test on multiple sources', () => {
      return expect(actor.test(
        { context: new ActionContext(
            { '@comunica/bus-rdf-resolve-quad-pattern:sources': [{ type: 'rdfjsSource', value: source },
                { type: 'rdfjsSource', value: source }]},
          ),
          pattern: <any> null },
      ))
        .rejects.toBeTruthy();
    });

    it('should get the source', () => {
      let context = new ActionContext({ [KeysRdfResolveQuadPattern.source.name]:
          { type: 'rdfjsSource', value: source }});
      return expect((<any> actor).getSource(context))
        .resolves.toMatchObject(new RdfJsQuadStreamingSource(<any>source, context));
    });

    it('should get the source on raw source form', () => {
      let context = new ActionContext({ [KeysRdfResolveQuadPattern.source.name]: source });
      return expect((<any> actor).getSource(context))
        .resolves.toMatchObject(new RdfJsQuadStreamingSource(<any>source, context));
    });

    it("stopMatches in context should be kept undefined", async () => {
      let context = new ActionContext({[KeysRdfResolveQuadPattern.source.name]: source});
      expect(context.get<any[]>(KeysStreamingSource.matchOptions)).toBeUndefined();
      (await (<any>actor).getSource(context)).match()
      expect(context.get<any[]>(KeysStreamingSource.matchOptions)).toBeUndefined();
    });

    it("should error on stopMatch if StreamingStore isn't doing it's job", async () => {
      let context: IActionContext = new ActionContext({[KeysRdfResolveQuadPattern.source.name]: source});
      context = context.set(KeysStreamingSource.matchOptions, []);
      (await (<any>actor).getSource(context)).match()
      let stopMatches = context.get<any[]>(KeysStreamingSource.matchOptions)
      if (stopMatches === undefined) {
        throw new Error("stopMatches in context is undefined")
      }
      expect(stopMatches.length).toEqual(1);
      expect(stopMatches[0].stopMatch).toThrowError("stopMatch function has not been replaced in streaming store.")
    });

    it('should run with a real store', async() => {
      const store = new StreamingStore();

      await promisifyEventEmitter(store.import(streamifyArray([
        quad('s1', 'p', 'o1'),
        quad('s2', 'p', 'o2'),
        quad('s3', 'px', 'o3'),
      ])));

      context = new ActionContext({ [KeysRdfResolveQuadPattern.source.name]: store });
      context = context.set(KeysStreamingSource.matchOptions, []);
      const pattern: any = {
        subject: DF.variable('s'),
        predicate: DF.namedNode('p'),
        object: DF.variable('o'),
        graph: DF.variable('g'),
      };
      //make sure the store imports the quads
      await new Promise<void>(resolve=>setTimeout(()=>resolve(), 100));

      const { data } = await actor.run({ pattern, context });

      let number = 2;
      data.on("data", () => {
        number--;
        if (number == 0) {
          data.close();
        }
      });

      expect(await arrayifyStream(data)).toBeRdfIsomorphic([
        DF.quad(DF.namedNode('s1'), DF.namedNode('p'), DF.namedNode('o1')),
        DF.quad(DF.namedNode('s2'), DF.namedNode('p'), DF.namedNode('o2')),
      ]);
      expect(await new Promise(resolve => data.getProperty('metadata', resolve)))
        .toEqual({
          state: new MetadataValidationState(),
          cardinality: { type: 'exact', value: 1 },
          canContainUndefs: false
        });

      let stopMatches = context.get<any[]>(KeysStreamingSource.matchOptions)
      if (stopMatches === undefined) {
        throw new Error("stopMatches in context is undefined")
      }
      expect(stopMatches.length).toEqual(1);
      expect(stopMatches[0].stopMatch).not.toThrowError();
      store.end();
    });

    it('should run without a store', async() => {
      const source = new RdfJsQuadStreamingSource();
      source.store.end()
      expect(await arrayifyStream(source.store.match())).toEqualRdfQuadArray([]);
    });

    it('should set the `up-to-date` property to true by default', async () => {
      const store = new StreamingStore();

      context = new ActionContext({[KeysRdfResolveQuadPattern.source.name]: store});
      const pattern: any = {
        subject: DF.variable('s'),
        predicate: DF.namedNode('p'),
        object: DF.variable('o'),
        graph: DF.variable('g'),
      };

      const {data} = await actor.run({pattern, context});

      expect(await new Promise(resolve => data.getProperty('up-to-date', resolve)))
        .toEqual(true);

      store.end();
    });

    it('should set the `up-to-date` based on the guard events', async () => {
      const store = new StreamingStore();
      const guardEvents = new EventEmitter();

      context = new ActionContext({[KeysRdfResolveQuadPattern.source.name]: store});
      context = context.set(KeysGuard.events, guardEvents);

      const pattern: any = {
        subject: DF.variable('s'),
        predicate: DF.namedNode('p'),
        object: DF.variable('o'),
        graph: DF.variable('g'),
      };

      const {data} = await actor.run({pattern, context});

      expect(EventEmitter.getEventListeners(guardEvents, 'modified').length).toEqual(1);
      expect(EventEmitter.getEventListeners(guardEvents, 'up-to-date').length).toEqual(1);

      expect(await new Promise(resolve => data.getProperty('up-to-date', resolve)))
        .toEqual(true);

      guardEvents.emit('modified');

      expect(await new Promise(resolve => data.getProperty('up-to-date', resolve)))
        .toEqual(false);

      guardEvents.emit('up-to-date');

      expect(await new Promise(resolve => data.getProperty('up-to-date', resolve)))
        .toEqual(true);

      store.end();
    });

    /*
    it('should delegate its error event', async() => {
      const it = new Readable();
      it._read = () => {
        it.emit('error', new Error('RdfJsSource error'));
      };
      source = <any> { match: () => it };
      context = new ActionContext({ [KeysRdfResolveQuadPattern.source.name]: source });
      const pattern: any = {
        subject: DF.variable('s'),
        predicate: DF.namedNode('p'),
        object: DF.variable('o'),
        graph: DF.variable('g'),
      };
      const { data } = await actor.run({ pattern, context });
      await expect(arrayifyStream(data)).rejects.toThrow(new Error('RdfJsSource error'));
    });
     */
  });
});
