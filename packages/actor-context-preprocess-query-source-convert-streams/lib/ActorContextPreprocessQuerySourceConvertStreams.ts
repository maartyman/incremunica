import type {
  IActorContextPreprocessOutput,
  IActorContextPreprocessArgs,
} from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysInitQuery } from '@comunica/context-entries';
import type { IAction, IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type {
  ContextQuerySource,
  IQuerySourceStreamElement,
  NonStreamingQuerySource,
  QuerySourceStreamExpanded,
} from '@incremunica/types';

import type { AsyncIterator } from 'asynciterator';
import { WrappingIterator } from 'asynciterator';
import type { Readable } from 'readable-stream';

/**
 * An Incremunica Query Source Identify Streams Context Preprocess Actor.
 */
export class ActorContextPreprocessQuerySourceConvertStreams extends ActorContextPreprocess {
  public constructor(args: IActorContextPreprocessArgs) {
    super(args);
  }

  public async test(_action: IAction): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IAction): Promise<IActorContextPreprocessOutput> {
    let context = action.context;

    // Rewrite sources
    if (context.has(KeysInitQuery.querySourcesUnidentified)) {
      const querySourcesUnidentified = action.context
        .get<ContextQuerySource[]>(KeysInitQuery.querySourcesUnidentified)!;

      context = action.context
        .set(KeysInitQuery.querySourcesUnidentified, querySourcesUnidentified.map((source: ContextQuerySource) => {
          if (
            typeof (<Readable>source) === 'object' &&
            typeof (<Readable>source).pipe === 'function' &&
            typeof (<Readable>source).read === 'function' &&
            typeof (<Readable>source).readable === 'boolean' &&
            typeof (<Readable>source).destroy === 'function' &&
            typeof (<Readable>source).destroyed === 'boolean'
          ) {
            return <QuerySourceStreamExpanded>{
              type: 'stream',
              value: (new WrappingIterator<IQuerySourceStreamElement | NonStreamingQuerySource>((<Readable>source)))
                .map(ActorContextPreprocessQuerySourceConvertStreams.normalizeSource),
            };
          }
          if (
            typeof (<AsyncIterator<any>>source) === 'object' &&
            typeof (<AsyncIterator<any>>source).on === 'function' &&
            typeof (<AsyncIterator<any>>source).close === 'function' &&
            typeof (<AsyncIterator<any>>source).transform === 'function' &&
            typeof (<AsyncIterator<any>>source).map === 'function' &&
            typeof (<AsyncIterator<any>>source).destroy === 'function' &&
            typeof (<AsyncIterator<any>>source).read === 'function' &&
            typeof (<AsyncIterator<any>>source).filter === 'function' &&
            typeof (<AsyncIterator<any>>source).closed === 'boolean' &&
            typeof (<AsyncIterator<any>>source).ended === 'boolean' &&
            typeof (<AsyncIterator<any>>source).destroyed === 'boolean' &&
            typeof (<AsyncIterator<any>>source).readable === 'boolean'
          ) {
            return <QuerySourceStreamExpanded>{
              type: 'stream',
              value: (<AsyncIterator<IQuerySourceStreamElement | NonStreamingQuerySource>><any>source)
                .map(ActorContextPreprocessQuerySourceConvertStreams.normalizeSource),
            };
          }
          return source;
        }));
    }
    return { context };
  }

  private static normalizeSource(
    source: IQuerySourceStreamElement | NonStreamingQuerySource,
  ): IQuerySourceStreamElement {
    if (typeof source === 'object' && 'querySource' in source) {
      source.isAddition = source.isAddition ?? true;
      return source;
    }
    return { isAddition: true, querySource: source };
  }
}
