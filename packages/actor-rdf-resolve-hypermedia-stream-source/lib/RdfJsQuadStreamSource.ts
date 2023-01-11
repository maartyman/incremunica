import type { IQuadSource } from '@comunica/bus-rdf-resolve-quad-pattern';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import {EmptyIterator, IntegerIterator, SingletonIterator, wrap as wrapAsyncIterator} from 'asynciterator';
import {IActionRdfResolveHypermedia} from "@comunica/bus-rdf-resolve-hypermedia";
import {Quad} from "rdf-data-factory";

/**
 * A quad source that wraps over an {@link RDF.Source}.
 */
export class RdfJsQuadStreamSource implements IQuadSource {

  public constructor(source: IActionRdfResolveHypermedia) {

  }

  public static nullifyVariables(term?: RDF.Term): RDF.Term | undefined {
    return !term || term.termType === 'Variable' ? undefined : term;
  }

  public match(subject: RDF.Term, predicate: RDF.Term, object: RDF.Term, graph: RDF.Term): AsyncIterator<RDF.Quad> {
    // Create an async iterator from the matched quad stream
    /*
    const rawStream = this.source.match(
      RdfJsQuadStreamSource.nullifyVariables(subject),
      RdfJsQuadStreamSource.nullifyVariables(predicate),
      RdfJsQuadStreamSource.nullifyVariables(object),
      RdfJsQuadStreamSource.nullifyVariables(graph),
    );
    const it = wrapAsyncIterator<RDF.Quad>(rawStream, { autoStart: false });

    // Determine metadata
    this.setMetadata(it, subject, predicate, object, graph)
      .catch(error => it.destroy(error));
    */
    return new EmptyIterator();
  }

  protected async setMetadata(
    it: AsyncIterator<RDF.Quad>,
    subject: RDF.Term,
    predicate: RDF.Term,
    object: RDF.Term,
    graph: RDF.Term,
  ): Promise<void> {
    /*
    let cardinality: number;
    if (this.source.countQuads) {
      // If the source provides a dedicated method for determining cardinality, use that.
      cardinality = await this.source.countQuads(
        RdfJsQuadStreamSource.nullifyVariables(subject),
        RdfJsQuadStreamSource.nullifyVariables(predicate),
        RdfJsQuadStreamSource.nullifyVariables(object),
        RdfJsQuadStreamSource.nullifyVariables(graph),
      );
    } else {
      // Otherwise, fallback to a sub-optimal alternative where we just call match again to count the quads.
      // WARNING: we can NOT reuse the original data stream here,
      // because we may loose data elements due to things happening async.
      let i = 0;
      cardinality = await new Promise((resolve, reject) => {
        const matches = this.source.match(
          RdfJsQuadStreamSource.nullifyVariables(subject),
          RdfJsQuadStreamSource.nullifyVariables(predicate),
          RdfJsQuadStreamSource.nullifyVariables(object),
          RdfJsQuadStreamSource.nullifyVariables(graph),
        );
        matches.on('error', reject);
        matches.on('end', () => resolve(i));
        matches.on('data', () => i++);
      });
    }
    it.setProperty('metadata', { cardinality: { type: 'exact', value: cardinality }, canContainUndefs: false });

     */
    it.setProperty('metadata', { cardinality: { type: 'exact', value: 0 }, canContainUndefs: false });
  }
}
