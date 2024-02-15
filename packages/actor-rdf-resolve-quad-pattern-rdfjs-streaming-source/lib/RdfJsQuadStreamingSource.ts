import type { IQuadSource } from '@comunica/bus-rdf-resolve-quad-pattern';
import { MetadataValidationState } from '@comunica/metadata';
import type { IActionContext } from '@comunica/types';
import { KeysGuard, KeysStreamingSource } from '@incremunica/context-entries';
import { StreamingStore } from '@incremunica/incremental-rdf-streaming-store';
import type { IGuardEvents, Quad } from '@incremunica/incremental-types';
import type * as RDF from '@rdfjs/types';
import { WrappingIterator } from 'asynciterator';
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

  public static nullifyVariables(term?: RDF.Term): RDF.Term | null {
    return !term || term.termType === 'Variable' ? null : term;
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
        KeysStreamingSource.matchOptions,
      );
      if (matchOptionsArray !== undefined) {
        matchOptionsArray.push(matchOptions);
      }
    }

    const it = new WrappingIterator<Quad>(rawStream);

    if (this.store.constructor.name === 'StreamingStore') {
      let upToDate = this.store.isHalted();
      // Set up-to-date event listener
      const guardEvents = this.context?.get<IGuardEvents>(KeysGuard.events);
      if (guardEvents) {
        // TODO doesn't work with guard events
        guardEvents.on('up-to-date', () => {
          upToDate = true;
          it.readable = true;
        });
        guardEvents.on('modified', () => {
          upToDate = false;
          it.readable = true;
        });
      }
    }

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
