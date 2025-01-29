import type { IActionContext } from '@comunica/types';
import type {
  IQuerySourceSerialized,
  IQuerySourceUnidentifiedExpanded,
  IQuerySourceUnidentifiedExpandedRawContext,
} from '@comunica/types/lib/IQuerySource';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import type { Stream } from 'readable-stream';

export type NonStreamingQuerySource =
  string |
  { type?: string; value: string; context?: IActionContext } |
  { type?: string; value: string; context?: Record<string, any> };
export interface IQuerySourceStreamElement {
  isAddition: boolean;
  querySource: NonStreamingQuerySource;
}

export type QuerySourceStream = AsyncIterator<IQuerySourceStreamElement>;
export type QuerySourceStreamExpanded = {
  type: 'stream';
  value: AsyncIterator<IQuerySourceStreamElement>;
  context?: IActionContext;
};
export type QuerySourceUnidentifiedExpanded =
  IQuerySourceUnidentifiedExpanded | IQuerySourceSerialized | QuerySourceStreamExpanded;

export type ContextQuerySourceStream = AsyncIterator<IQuerySourceStreamElement | NonStreamingQuerySource> | Stream;
export type ContextQuerySource = string | RDF.Source | RDF.Store | QuerySourceUnidentifiedExpanded |
  IQuerySourceUnidentifiedExpandedRawContext | ContextQuerySourceStream;
