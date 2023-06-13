import type { IQuadSource } from '@comunica/bus-rdf-resolve-quad-pattern';
import { StreamingStore } from '@comunica/incremental-rdf-streaming-store';
import type { Quad } from '@comunica/incremental-types';
import type * as RDF from '@rdfjs/types';
import { wrap as wrapAsyncIterator } from 'asynciterator';
import type { AsyncIterator } from 'asynciterator';

export class RdfJsQuadStreamingSource implements IQuadSource {
  public store: StreamingStore<Quad>;

  public constructor(store?: StreamingStore<Quad>) {
    if (store !== undefined) {
      this.store = store;
    } else {
      this.store = new StreamingStore();
    }
  }

  public static nullifyVariables(term?: RDF.Term): RDF.Term | undefined {
    return !term || term.termType === 'Variable' ? undefined : term;
  }

  public match(subject: RDF.Term, predicate: RDF.Term, object: RDF.Term, graph: RDF.Term): AsyncIterator<Quad> {
    const rawStream = this.store.match(
      RdfJsQuadStreamingSource.nullifyVariables(subject),
      RdfJsQuadStreamingSource.nullifyVariables(predicate),
      RdfJsQuadStreamingSource.nullifyVariables(object),
      RdfJsQuadStreamingSource.nullifyVariables(graph),
    );

    const it = wrapAsyncIterator<Quad>(rawStream, { autoStart: false });

    it.setProperty('metadata', { cardinality: { type: 'exact', value: 1 }, canContainUndefs: false });

    // TODO implement setMetadata
    // this.setMetadata(it, subject, predicate, object, graph)
    // .catch(error => it.destroy(error));

    return it;
  }

  // TODO implement setMetadata make a proper estimation for the cardinality
  // protected async setMetadata(
  //  it: AsyncIterator<RDF.Quad>,
  //  subject: RDF.Term,
  //  predicate: RDF.Term,
  //  object: RDF.Term,
  //  graph: RDF.Term,
  // ): Promise<void> {
  //
  // }
}
