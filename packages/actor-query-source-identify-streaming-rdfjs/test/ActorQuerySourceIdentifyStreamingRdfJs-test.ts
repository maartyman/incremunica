import { ActorQuerySourceIdentify } from '@comunica/bus-query-source-identify';
import { ActionContext, Bus } from '@comunica/core';
import { DevTools } from '@incremunica/dev-tools';
import type * as RDF from '@rdfjs/types';
import { ActorQuerySourceIdentifyStreamingRdfJs, StreamingQuerySourceRdfJs } from '..';
import 'jest-rdf';
import '@comunica/utils-jest';

const mediatorMergeBindingsContext: any = {
  mediate(arg: any) {
    return {};
  },
};

describe('ActorQuerySourceIdentifyStreamingRdfJs', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('The ActorQuerySourceIdentifyStreamingRdfJs module', () => {
    it('should be a function', () => {
      expect(ActorQuerySourceIdentifyStreamingRdfJs).toBeInstanceOf(Function);
    });

    it('should be a ActorQuerySourceIdentifyStreamingRdfJs constructor', () => {
      expect(new (<any> ActorQuerySourceIdentifyStreamingRdfJs)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorQuerySourceIdentifyStreamingRdfJs);
      expect(new (<any> ActorQuerySourceIdentifyStreamingRdfJs)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorQuerySourceIdentify);
    });

    it('should not be able to create new ActorQuerySourceIdentifyStreamingRdfJs objects without \'new\'', () => {
      expect(() => {
        (<any> ActorQuerySourceIdentifyStreamingRdfJs)();
      }).toThrow(`Class constructor ActorQuerySourceIdentifyStreamingRdfJs cannot be invoked without 'new'`);
    });
  });

  describe('An ActorQuerySourceIdentifyStreamingRdfJs instance', () => {
    let actor: ActorQuerySourceIdentifyStreamingRdfJs;
    let source: RDF.Source;

    beforeEach(() => {
      actor = new ActorQuerySourceIdentifyStreamingRdfJs({ name: 'actor', bus, mediatorMergeBindingsContext });
      source = { match: () => <any> null };
    });

    describe('test', () => {
      it('should test', async() => {
        await expect(actor.test({
          querySourceUnidentified: { type: 'rdfjs', value: source },
          context: new ActionContext(),
        })).resolves.toBeTruthy();
      });

      it('should not test with sparql type', async() => {
        await expect(actor.test({
          querySourceUnidentified: { type: 'sparql', value: source },
          context: new ActionContext(),
        })).resolves.toFailTest(`actor requires a single query source with rdfjs type to be present in the context.`);
      });

      it('should not test with string value', async() => {
        await expect(actor.test({
          querySourceUnidentified: { type: 'rdfjs', value: 'abc' },
          context: new ActionContext(),
        })).resolves.toFailTest(`actor received an invalid streaming rdfjs query source.`);
      });

      it('should not test with invalid source value', async() => {
        await expect(actor.test({
          querySourceUnidentified: { type: 'rdfjs', value: <any>{}},
          context: new ActionContext(),
        })).resolves.toFailTest(`actor received an invalid streaming rdfjs query source.`);
      });
    });

    describe('run', () => {
      it('should get the source', async() => {
        const contextIn = DevTools.createTestContextWithDataFactory();
        const ret = await actor.run({
          querySourceUnidentified: { type: 'rdfjs', value: source },
          context: contextIn,
        });
        expect(ret.querySource.source).toBeInstanceOf(StreamingQuerySourceRdfJs);
        expect(ret.querySource.context).not.toBe(contextIn);
      });

      it('should get the source with context', async() => {
        const contextIn = DevTools.createTestContextWithDataFactory();
        const contextSource = new ActionContext();
        const ret = await actor.run({
          querySourceUnidentified: { type: 'rdfjs', value: source, context: contextSource },
          context: contextIn,
        });
        expect(ret.querySource.source).toBeInstanceOf(StreamingQuerySourceRdfJs);
        expect(ret.querySource.context).not.toBe(contextIn);
        expect(ret.querySource.context).toBe(contextSource);
      });
    });
  });
});
