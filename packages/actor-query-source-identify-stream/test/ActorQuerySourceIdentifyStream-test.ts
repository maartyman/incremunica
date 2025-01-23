import type { MediatorContextPreprocess } from '@comunica/bus-context-preprocess';
import { ActorQuerySourceIdentify } from '@comunica/bus-query-source-identify';
import type { MediatorRdfMetadataAccumulate } from '@comunica/bus-rdf-metadata-accumulate';
import { KeysInitQuery, KeysQueryOperation } from '@comunica/context-entries';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import { KeysBindings } from '@incremunica/context-entries';
import { createTestContextWithDataFactory, AF, DF, BF, partialArrayifyAsyncIterator } from '@incremunica/dev-tools';
import type { IQuerySourceStreamElement } from '@incremunica/types';
import { ArrayIterator, AsyncIterator } from 'asynciterator';
import { ActorQuerySourceIdentifyStream } from '../lib';
import 'jest-rdf';
import '@comunica/utils-jest';
import { StreamQuerySources } from '../lib/StreamQuerySources';

describe('ActorQuerySourceIdentifyStream', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('The ActorQuerySourceIdentifyStream module', () => {
    it('should be a function', () => {
      expect(ActorQuerySourceIdentifyStream).toBeInstanceOf(Function);
    });

    it('should be a ActorQuerySourceIdentifyStream constructor', () => {
      expect(new (<any> ActorQuerySourceIdentifyStream)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorQuerySourceIdentifyStream);
      expect(new (<any> ActorQuerySourceIdentifyStream)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorQuerySourceIdentify);
    });

    it('should not be able to create new ActorQuerySourceIdentifyStream objects without \'new\'', () => {
      expect(() => {
        (<any> ActorQuerySourceIdentifyStream)();
      }).toThrow(`Class constructor ActorQuerySourceIdentifyStream cannot be invoked without 'new'`);
    });
  });

  describe('An ActorQuerySourceIdentifyStream instance', () => {
    let actor: ActorQuerySourceIdentifyStream;
    let source: AsyncIterator<any>;
    let mediatorRdfMetadataAccumulate: MediatorRdfMetadataAccumulate;
    let mediatorContextPreprocess: MediatorContextPreprocess;
    let context: IActionContext;
    let deleteCallback: () => void;

    beforeEach(() => {
      deleteCallback = jest.fn();
      mediatorRdfMetadataAccumulate = <MediatorRdfMetadataAccumulate><any> {
        mediate: jest.fn((action) => {
          return { source: {}, context: new ActionContext() };
        }),
      };
      mediatorContextPreprocess = <MediatorContextPreprocess><any> {
        mediate: jest.fn((action) => {
          return Promise.resolve({
            context: action.context.set(KeysQueryOperation.querySources, [{
              source: {
                queryBindings: () => {
                  const it = new AsyncIterator();
                  it.read = () => {
                    if (it.readable) {
                      it.readable = false;
                      return BF.bindings([
                        [ DF.variable('v'), DF.namedNode('a') ],
                      ]).setContextEntry(KeysBindings.isAddition, true);
                    }
                    return null;
                  };
                  it.readable = true;

                  it.setProperty<() => void>('delete', () => {
                    deleteCallback();
                    it.read = () => {
                      if (it.readable) {
                        it.readable = false;
                        return BF.bindings([
                          [ DF.variable('v'), DF.namedNode('a') ],
                        ]).setContextEntry(KeysBindings.isAddition, false);
                      }
                      it.close();
                      return null;
                    };
                    it.readable = true;
                  });

                  return it;
                },
              },
              context: new ActionContext(),
            }]),
          });
        }),
      };
      actor = new ActorQuerySourceIdentifyStream({
        name: 'actor',
        bus,
        mediatorRdfMetadataAccumulate,
        mediatorContextPreprocess,
      });
      source = new ArrayIterator<IQuerySourceStreamElement>();
      context = createTestContextWithDataFactory();
    });

    describe('test', () => {
      it('should test', async() => {
        await expect(actor.test({
          querySourceUnidentified: { type: 'stream', value: <any>source },
          context: new ActionContext(),
        })).resolves.toBeTruthy();
      });

      it('should not test with sparql type', async() => {
        await expect(actor.test({
          querySourceUnidentified: { type: 'sparql', value: <any>source },
          context: new ActionContext(),
        })).resolves.toFailTest(`actor requires a single query source with stream type to be present in the context.`);
      });
    });

    describe('run', () => {
      it('should get the source', async() => {
        const result = await actor.run({
          querySourceUnidentified: { type: 'stream', value: <any>source },
          context,
        });
        expect(result.querySource.source).toBeInstanceOf(StreamQuerySources);
        expect(result.querySource.context).not.toBe(context);
      });

      it('should work with different versions of query sources', async() => {
        const sources = [
          {
            isAddition: true,
            querySource: 'http://example.org/',
          },
          {
            isAddition: true,
            querySource: '<http://example.org/s> <http://example.org/p> <http://example.org/o>.',
          },
          {
            isAddition: true,
            querySource: {
              type: 'serialized',
              value: '<http://example.org/s> <http://example.org/p> <http://example.org/o>.',
            },
          },
          {
            isAddition: true,
            querySource: {
              value: 'http://example.org/',
            },
          },
          {
            isAddition: true,
            querySource: {
              value: 'http://example.org/',
              context: new ActionContext(),
            },
          },
        ];
        source = new ArrayIterator(sources);
        const result = await actor.run({
          querySourceUnidentified: { type: 'stream', value: <any>source },
          context,
        });
        expect(result.querySource.source).toBeInstanceOf(StreamQuerySources);
        expect(result.querySource.context).not.toBe(context);
        const bindings = result.querySource.source.queryBindings(
          AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o')),
          new ActionContext(),
        );
        await expect(partialArrayifyAsyncIterator(bindings, 5)).resolves.toEqualBindingsArray([
          BF.bindings([[ DF.variable('v'), DF.namedNode('a') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('v'), DF.namedNode('a') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('v'), DF.namedNode('a') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('v'), DF.namedNode('a') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('v'), DF.namedNode('a') ]]).setContextEntry(KeysBindings.isAddition, true),
        ]);
        expect(mediatorContextPreprocess.mediate).toHaveBeenNthCalledWith(
          1,
          { context: context.set(KeysInitQuery.querySourcesUnidentified, [ sources[0].querySource ]) },
        );
        expect(mediatorContextPreprocess.mediate).toHaveBeenNthCalledWith(
          2,
          { context: context.set(KeysInitQuery.querySourcesUnidentified, [ sources[1].querySource ]) },
        );
        expect(mediatorRdfMetadataAccumulate.mediate).toHaveBeenCalledTimes(0);
      });

      it('should work with immediate deletions 1', async() => {
        const sources = [
          {
            isAddition: true,
            querySource: 'http://example.org/',
          },
          {
            isAddition: false,
            querySource: 'http://example.org/',
          },
        ];
        source = new ArrayIterator(sources);
        const result = await actor.run({
          querySourceUnidentified: { type: 'stream', value: <any>source },
          context,
        });
        expect(result.querySource.source).toBeInstanceOf(StreamQuerySources);
        expect(result.querySource.context).not.toBe(context);
        const bindings = result.querySource.source.queryBindings(
          AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o')),
          new ActionContext(),
        );
        await new Promise(resolve => setTimeout(resolve, 10));
        expect(deleteCallback).toHaveBeenCalledTimes(0);
        expect(mediatorContextPreprocess.mediate).toHaveBeenNthCalledWith(
          1,
          { context: context.set(KeysInitQuery.querySourcesUnidentified, [ sources[0].querySource ]) },
        );
      });

      it('should work with immediate deletions 2', async() => {
        const sources = [
          {
            isAddition: true,
            querySource: 'http://example.org/',
          },
          {
            isAddition: true,
            querySource: 'http://example.org/',
          },
          {
            isAddition: true,
            querySource: 'http://example.org/',
          },
          {
            isAddition: false,
            querySource: 'http://example.org/',
          },
          {
            isAddition: false,
            querySource: 'http://example.org/',
          },
        ];
        source = new ArrayIterator(sources);
        const result = await actor.run({
          querySourceUnidentified: { type: 'stream', value: <any>source },
          context,
        });
        expect(result.querySource.source).toBeInstanceOf(StreamQuerySources);
        expect(result.querySource.context).not.toBe(context);
        const bindings = result.querySource.source.queryBindings(
          AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o')),
          new ActionContext(),
        );
        await expect(partialArrayifyAsyncIterator(bindings, 1)).resolves.toEqualBindingsArray([
          BF.bindings([[ DF.variable('v'), DF.namedNode('a') ]]).setContextEntry(KeysBindings.isAddition, true),
        ]);
        expect(deleteCallback).toHaveBeenCalledTimes(0);
        expect(mediatorContextPreprocess.mediate).toHaveBeenNthCalledWith(
          1,
          { context: context.set(KeysInitQuery.querySourcesUnidentified, [ sources[0].querySource ]) },
        );
      });

      it('should work with slow deletions', async() => {
        const sources = [
          {
            isAddition: true,
            querySource: 'http://example.org/',
          },
          {
            isAddition: false,
            querySource: 'http://example.org/',
          },
        ];
        source = new AsyncIterator();
        source.read = () => {
          if (sources.length === 0) {
            source.close();
            return null;
          }
          if (source.readable) {
            source.readable = false;
            return sources.shift();
          }
          source.readable = false;
          return null;
        };
        source.readable = true;
        const result = await actor.run({
          querySourceUnidentified: { type: 'stream', value: <any>source },
          context,
        });
        expect(result.querySource.source).toBeInstanceOf(StreamQuerySources);
        expect(result.querySource.context).not.toBe(context);
        const bindings = result.querySource.source.queryBindings(
          AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o')),
          new ActionContext(),
        );
        await expect(partialArrayifyAsyncIterator(bindings, 1)).resolves.toEqualBindingsArray([
          BF.bindings([[ DF.variable('v'), DF.namedNode('a') ]]).setContextEntry(KeysBindings.isAddition, true),
        ]);
        source.readable = true;
        await expect(partialArrayifyAsyncIterator(bindings, 1)).resolves.toEqualBindingsArray([
          BF.bindings([[ DF.variable('v'), DF.namedNode('a') ]]).setContextEntry(KeysBindings.isAddition, false),
        ]);
        expect(deleteCallback).toHaveBeenCalledTimes(1);
        expect(mediatorContextPreprocess.mediate).toHaveBeenNthCalledWith(
          1,
          { context: context.set(KeysInitQuery.querySourcesUnidentified, [ 'http://example.org/' ]) },
        );
      });

      it('should fail on non existing deletions', async() => {
        const sources = [
          {
            isAddition: true,
            querySource: 'http://example.org/1',
          },
          {
            isAddition: true,
            querySource: 'http://example.org/2',
          },
          {
            isAddition: false,
            querySource: 'http://example.org/a',
          },
        ];
        source = new AsyncIterator();
        source.read = () => {
          if (sources.length === 0) {
            source.close();
            return null;
          }
          return sources.shift();
        };
        source.readable = false;
        const result = (await actor.run({
          querySourceUnidentified: { type: 'stream', value: <any>source },
          context,
        })).querySource;
        const bindingsStream = result.source.queryBindings(
          AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o')),
          new ActionContext(),
        );
        source.readable = true;
        await expect(new Promise<void>((resolve, reject) => {
          bindingsStream.on('data', () => {
            resolve();
          });
          bindingsStream.on('end', () => {
            resolve();
          });
          bindingsStream.on('error', (e) => {
            reject(e);
          });
        })).rejects.toThrow('Deleted source: "http://example.org/a" has not been added. List of added sources:\n[\nhttp://example.org/1,\nhttp://example.org/2\n]');
        expect(() => {
          result.source.queryBindings(
            AF.createPattern(DF.variable('s'), DF.namedNode('p'), DF.variable('o')),
            new ActionContext(),
          );
        }).toThrow('Deleted source: "http://example.org/a" has not been added. List of added sources:\n[\nhttp://example.org/1,\nhttp://example.org/2\n]');
      });

      it('should get the source with context', async() => {
        const contextSource = new ActionContext();
        const ret = await actor.run({
          querySourceUnidentified: { type: 'stream', value: <any>source, context: contextSource },
          context,
        });
        expect(ret.querySource.source).toBeInstanceOf(StreamQuerySources);
        expect(ret.querySource.context).not.toBe(context);
        expect(ret.querySource.context).toBe(contextSource);
      });
    });
  });
});
