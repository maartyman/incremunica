import type { IQuadSource } from '@comunica/bus-rdf-resolve-quad-pattern';
import { ActionContextKey } from '@comunica/core/lib/ActionContext';
import { MetadataValidationState } from '@comunica/metadata';
import type { IActionContext } from '@comunica/types';
import { StreamingStore } from '@incremunica/incremental-rdf-streaming-store';
import type { Quad } from '@incremunica/incremental-types';
import type * as RDF from '@rdfjs/types';
import { wrap as wrapAsyncIterator } from 'asynciterator';
import type { AsyncIterator } from 'asynciterator';

export class RdfJsQuadStreamingSource implements IQuadSource {
  public store;
  public context;

  public constructor(
    store?: StreamingStore<Quad>,
    context?: IActionContext | undefined,
  ) {
    if (store !== undefined) {
      this.store = store;
    } else {
      this.store = new StreamingStore();
    }
    this.context = context;
  }

  public static nullifyVariables(term?: RDF.Term): RDF.Term | undefined {
    return !term || term.termType === 'Variable' ? undefined : term;
  }

  public match(subject: RDF.Term, predicate: RDF.Term, object: RDF.Term, graph: RDF.Term): AsyncIterator<Quad> {
    const matchOptions = {
      stopMatch() {
        throw new Error('stopMatch function has not been replaced in streaming store.');
      },
    };
    const rawStream = this.store.match(
      RdfJsQuadStreamingSource.nullifyVariables(subject),
      RdfJsQuadStreamingSource.nullifyVariables(predicate),
      RdfJsQuadStreamingSource.nullifyVariables(object),
      RdfJsQuadStreamingSource.nullifyVariables(graph),
      matchOptions,
    );

    if (this.context) {
      const matchOptionsArray: ({ stopMatch: () => void })[] | undefined = this.context.get(
        new ActionContextKey<({ stopMatch: () => void })[]>('matchOptions'),
      );
      if (matchOptionsArray !== undefined) {
        matchOptionsArray.push(matchOptions);
      }
    }

    const it = wrapAsyncIterator<Quad>(rawStream, { autoStart: false });

    // In case this setMetadata can cause errors, catch the error and emit it on the iterator (it). For now ignore it!
    /* eslint-disable-next-line @typescript-eslint/no-floating-promises */
    this.setMetadata(it, subject, predicate, object, graph);

    return it;
  }

  // TODO implement setMetadata make a proper estimation for the cardinality
  protected async setMetadata(
    it: AsyncIterator<RDF.Quad>,
    subject: RDF.Term,
    predicate: RDF.Term,
    object: RDF.Term,
    graph: RDF.Term,
  ): Promise<void> {
    const cardinality = 1;

    it.setProperty('metadata', {
      state: new MetadataValidationState(),
      cardinality: { type: 'exact', value: cardinality },
      canContainUndefs: false,
    });
  }
}
