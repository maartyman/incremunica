import 'jest';
import '@comunica/utils-jest';
import { KeysInitQuery } from '@comunica/context-entries';
import type { IAction } from '@comunica/core';
import { ActionContext, Bus } from '@comunica/core';
import { AsyncIterator, UnionIterator, WrappingIterator } from 'asynciterator';
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
        value: expect.any(WrappingIterator),
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
});
