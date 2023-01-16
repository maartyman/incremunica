import type { IQuadSource } from '@comunica/bus-rdf-resolve-quad-pattern';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import {wrap as wrapAsyncIterator} from 'asynciterator';
import {IActionRdfResolveHypermedia} from "@comunica/bus-rdf-resolve-hypermedia";
import {StreamStore} from "./StreamStore";
import {Readable} from "readable-stream";
import {Quad} from "@comunica/types/lib/Quad";

/**
 * A quad source that wraps over an {@link RDF.Source}.
 */
export class RdfJsQuadStreamSource implements IQuadSource {
  public source;
  public store;

  public constructor(source: IActionRdfResolveHypermedia) {
    this.source = source;
    this.store = new StreamStore(<Readable><any>this.source.quads);
  }

  public static nullifyVariables(term?: RDF.Term): RDF.Term | undefined {
    return !term || term.termType === 'Variable' ? undefined : term;
  }

  public match(subject: RDF.Term, predicate: RDF.Term, object: RDF.Term, graph: RDF.Term): AsyncIterator<Quad> {
    const rawStream = this.store.match(
      RdfJsQuadStreamSource.nullifyVariables(subject),
      RdfJsQuadStreamSource.nullifyVariables(predicate),
      RdfJsQuadStreamSource.nullifyVariables(object),
      RdfJsQuadStreamSource.nullifyVariables(graph),
    );

    const it = wrapAsyncIterator<Quad>(rawStream, { autoStart: false });

    this.setMetadata(it, subject, predicate, object, graph)
      .catch(error => it.destroy(error));

    return it;
  }

  protected async setMetadata(
    it: AsyncIterator<RDF.Quad>,
    subject: RDF.Term,
    predicate: RDF.Term,
    object: RDF.Term,
    graph: RDF.Term,
  ): Promise<void> {
    //TODO make a proper estimation for the cardinality
    it.setProperty('metadata', { cardinality: { type: 'exact', value: 1 }, canContainUndefs: false });
  }
}
