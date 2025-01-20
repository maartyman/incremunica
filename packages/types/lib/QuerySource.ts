import type { IActionContext } from '@comunica/types';
import type {
  IQuerySourceSerialized,
  IQuerySourceUnidentifiedExpanded,
  IQuerySourceUnidentifiedExpandedRawContext,
} from '@comunica/types/lib/IQuerySource';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';

export interface IQuerySourceStreamElement {
  isAddition: boolean;
  querySource:
    string |
    { type?: string; value: string; context?: IActionContext } |
    { type?: string; value: string; context?: Record<string, any> };
}
export type QuerySourceStream = AsyncIterator<IQuerySourceStreamElement>;

export type QuerySourceUnidentifiedExpanded = IQuerySourceUnidentifiedExpanded | IQuerySourceSerialized;
export type QuerySourceUnidentified = string | RDF.Source | RDF.Store | QuerySourceUnidentifiedExpanded |
  IQuerySourceUnidentifiedExpandedRawContext | QuerySourceStream;
