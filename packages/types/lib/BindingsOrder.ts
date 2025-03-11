import type * as RDF from '@rdfjs/types';

export type BindingsOrder = {
  hash: string;
  result: RDF.Term | undefined;
  index: number;
};
