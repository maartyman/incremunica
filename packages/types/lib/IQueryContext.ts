import type { FunctionArgumentsCache, IProxyHandler, QueryExplainMode, Logger } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import type { ISourceWatchEventEmitter } from './ISourceWatchEventEmitter';
import type { ContextQuerySource } from './QuerySource';

/**
 * Query context when a string-based query was passed.
 */
export type QueryStringContext =
  RDF.QueryStringContext & RDF.QuerySourceContext<ContextQuerySource> & IQueryContextCommon;
/**
 * Query context when an algebra-based query was passed.
 */
export type QueryAlgebraContext =
  RDF.QueryAlgebraContext & RDF.QuerySourceContext<ContextQuerySource> & IQueryContextCommon;

/**
 * Common query context interface
 */
export interface IQueryContextCommon {
  // Types of these entries should be aligned with contextKeyShortcuts in ActorInitQueryBase
  pollingPeriod?: number;
  deferredEvaluationTrigger?: ISourceWatchEventEmitter;
  initialBindings?: RDF.Bindings;
  log?: Logger;
  datetime?: Date;
  httpProxyHandler?: IProxyHandler;
  lenient?: boolean;
  httpIncludeCredentials?: boolean;
  httpAuth?: string;
  httpTimeout?: number;
  httpBodyTimeout?: boolean;
  httpRetryCount?: number;
  httpRetryDelayFallback?: number;
  httpRetryDelayLimit?: number;
  fetch?: typeof fetch;
  readOnly?: boolean;
  extensionFunctionCreator?: (functionNamedNode: RDF.NamedNode)
  => ((args: RDF.Term[]) => Promise<RDF.Term>) | undefined;
  functionArgumentsCache?: FunctionArgumentsCache;
  extensionFunctions?: Record<string, (args: RDF.Term[]) => Promise<RDF.Term>>;
  explain?: QueryExplainMode;
  recoverBrokenLinks?: boolean;
}
