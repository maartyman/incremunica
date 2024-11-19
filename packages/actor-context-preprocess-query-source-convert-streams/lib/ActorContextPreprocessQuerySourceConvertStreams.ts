import type {
  IActorContextPreprocessOutput,
  IActorContextPreprocessArgs,
} from '@comunica/bus-context-preprocess';
import { ActorContextPreprocess } from '@comunica/bus-context-preprocess';
import { KeysInitQuery } from '@comunica/context-entries';
import type { IAction, IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import { WrappingIterator } from 'asynciterator';

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
        .get(KeysInitQuery.querySourcesUnidentified)!;

      context = action.context
        .set(KeysInitQuery.querySourcesUnidentified, querySourcesUnidentified.map((source: any) => {
          if (
            typeof source === 'object' &&
            typeof source.pipe === 'function' &&
            typeof source.read === 'function' &&
            typeof source.readable === 'boolean' &&
            typeof source.readableObjectMode === 'boolean' &&
            typeof source.destroy === 'function' &&
            typeof source.destroyed === 'boolean'
          ) {
            return { type: 'stream', value: new WrappingIterator(source) };
          }
          if (
            typeof source === 'object' &&
            typeof source.on === 'function' &&
            typeof source.close === 'function' &&
            typeof source.transform === 'function' &&
            typeof source.map === 'function' &&
            typeof source.destroy === 'function' &&
            typeof source.read === 'function' &&
            typeof source.filter === 'function' &&
            typeof source.closed === 'boolean' &&
            typeof source.ended === 'boolean' &&
            typeof source.destroyed === 'boolean' &&
            typeof source.readable === 'boolean'
          ) {
            return { type: 'stream', value: source };
          }
          return source;
        }));
    }
    return { context };
  }
}
