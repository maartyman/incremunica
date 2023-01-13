import type { IQuadSource } from '@comunica/bus-rdf-resolve-quad-pattern';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import {wrap as wrapAsyncIterator} from 'asynciterator';
import {IActionRdfResolveHypermedia} from "@comunica/bus-rdf-resolve-hypermedia";
import {StreamStore} from "./StreamStore";
import {Readable} from "readable-stream";
import {Quad} from "@comunica/types/lib/Quad";
const streamifyArray = require('streamify-array');
const quad = require('rdf-quad');

/**
 * A quad source that wraps over an {@link RDF.Source}.
 */
export class RdfJsQuadStreamSource implements IQuadSource {
  private source;
  private store;

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

    //test:
    setTimeout(() => {
      this.store.attachStream(streamifyArray([quad("<http://test.com/s_laat>", "<http://test.com//p_laat>", "<http://test.com/o_laat>")]))
    }, 10000);

    /*
    TODO guard resource mediator:
      input:
        -the streaming store
        -info about the resource: metadata and url

      no output

      function:
        -finds a suitable actor (polling, solid websocket protocol v1/v2, LDES maybe, ...)
        -the actor will find the changes in the resource and attach the changes stream onto the store (store.attachStream)
     */

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
