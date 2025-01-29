import 'jest';
import '@comunica/utils-jest';
import { KeysInitQuery } from '@comunica/context-entries';
import type { IAction } from '@comunica/core';
import { ActionContext, Bus } from '@comunica/core';
import type { QuerySourceStreamExpanded } from '@incremunica/types';
import { arrayifyStream } from 'arrayify-stream';
import { ArrayIterator, AsyncIterator, UnionIterator, WrappingIterator } from 'asynciterator';
import { Store } from 'n3';
import { Readable } from 'readable-stream';
import { ActorContextPreprocessQuerySourceConvertStreams } from '../lib';

describe('ActorContextPreprocessQuerySourceConvertStreams', () => {
  let action: IAction;
  let actor: ActorContextPreprocessQuerySourceConvertStreams;

  beforeEach(() => {
    action = {
      context: new ActionContext(),
    };
    const bus: any = new Bus({ name: 'bus' });
    actor = new ActorContextPreprocessQuerySourceConvertStreams({
      name: 'actor',
      bus,
    });
  });

  it('should test', async() => {
    await expect(actor.test(action)).resolves.toPassTestVoid();
  });

  it('should run without sources', async() => {
    await expect(actor.run(action)).resolves.toEqual(action);
  });

  it('should leave non-stream sources', async() => {
    const sources = [
      'stringSource',
      {
        type: 'someType',
        value: 'objectSource',
      },
      new Store(),
    ];
    action.context = action.context.set(KeysInitQuery.querySourcesUnidentified, sources);

    await expect(actor.run(action)).resolves.toEqual(action);
  });

  it('should put AsyncIterator source in objects with value and type', async() => {
    const sources: any = [
      new AsyncIterator(),
    ];
    action.context = action.context.set(KeysInitQuery.querySourcesUnidentified, sources);

    expect((await actor.run(action)).context.get(KeysInitQuery.querySourcesUnidentified)).toEqual([
      {
        type: 'stream',
        value: expect.any(AsyncIterator),
      },
    ]);
  });

  it('should convert a readable stream to an AsyncIterator', async() => {
    const sources: any = [
      new Readable({ read: () => {} }),
    ];
    action.context = action.context.set(KeysInitQuery.querySourcesUnidentified, sources);

    expect((await actor.run(action)).context.get(KeysInitQuery.querySourcesUnidentified)).toEqual([
      {
        type: 'stream',
        value: expect.any(AsyncIterator),
      },
    ]);
  });

  it('should run with multiple streaming sources', async() => {
    const sources: any = [
      new Readable({ read: () => {} }),
      new AsyncIterator(),
      new UnionIterator([]),
      new WrappingIterator(),
    ];
    action.context = action.context.set(KeysInitQuery.querySourcesUnidentified, sources);

    expect((await actor.run(action)).context.get(KeysInitQuery.querySourcesUnidentified)).toEqual([
      {
        type: 'stream',
        value: expect.any(AsyncIterator),
      },
      {
        type: 'stream',
        value: expect.any(AsyncIterator),
      },
      {
        type: 'stream',
        value: expect.any(AsyncIterator),
      },
      {
        type: 'stream',
        value: expect.any(AsyncIterator),
      },
    ]);
  });

  it('should run with multiple source types in streaming sources', async() => {
    const sources: any = [
      new ArrayIterator([
        'http://example.org/',
        '<a1> <b1> <c1>.',
        {
          querySource: 'http://example.org/',
        },
        {
          querySource: '<a1> <b1> <c1>.',
        },
        {
          isAddition: false,
          querySource: 'http://example.org/',
        },
        {
          isAddition: false,
          querySource: '<a1> <b1> <c1>.',
        },
      ]),
    ];
    action.context = action.context.set(KeysInitQuery.querySourcesUnidentified, sources);

    const result = (await actor.run(action)).context.get(KeysInitQuery.querySourcesUnidentified);
    expect(result).toEqual([
      {
        type: 'stream',
        value: expect.any(AsyncIterator),
      },
    ]);
    await expect(arrayifyStream((<QuerySourceStreamExpanded><any>result[0]).value)).resolves.toEqual([
      {
        isAddition: true,
        querySource: 'http://example.org/',
      },
      {
        isAddition: true,
        querySource: '<a1> <b1> <c1>.',
      },
      {
        isAddition: true,
        querySource: 'http://example.org/',
      },
      {
        isAddition: true,
        querySource: '<a1> <b1> <c1>.',
      },
      {
        isAddition: false,
        querySource: 'http://example.org/',
      },
      {
        isAddition: false,
        querySource: '<a1> <b1> <c1>.',
      },
    ]);
  });
});
