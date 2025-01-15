import {
  filterMatchingQuotedQuads,
  getVariables,
  getDuplicateElementLinks,
  setMetadata,
} from '@comunica/bus-query-source-identify';
import { KeysQueryOperation } from '@comunica/context-entries';
import type {
  BindingsStream,
  IActionContext,
  FragmentSelectorShape,
  Bindings,
  ComunicaDataFactory,
} from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { ClosableIterator } from '@comunica/utils-iterator';
import { MetadataValidationState } from '@comunica/utils-metadata';
import { KeysBindings, KeysStreamingSource } from '@incremunica/context-entries';
import type { Quad } from '@incremunica/incremental-types';
import { StreamingQuerySource, StreamingQuerySourceStatus } from '@incremunica/streaming-query-source';
import type { StreamingStore } from '@incremunica/streaming-store';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { wrap as wrapAsyncIterator } from 'asynciterator';
import type {
  QuadTermName,
} from 'rdf-terms';
import {
  getValueNestedPath,
  reduceTermsNested,
  someTermsNested,
} from 'rdf-terms';
import { Factory } from 'sparqlalgebrajs';
import type { Algebra } from 'sparqlalgebrajs';

export class StreamingQuerySourceRdfJs extends StreamingQuerySource {
  // TODO [2025-01-01]: generalize store type
  public store: StreamingStore<Quad>;
  private registeredQueries: number;
  protected readonly selectorShape: FragmentSelectorShape;
  private readonly dataFactory: ComunicaDataFactory;
  private readonly bindingsFactory: BindingsFactory;

  public constructor(store: StreamingStore<Quad>, dataFactory: ComunicaDataFactory, bindingsFactory: BindingsFactory) {
    super();
    this.store = store;
    this.referenceValue = store;
    this.registeredQueries = 0;
    this.dataFactory = dataFactory;
    this.bindingsFactory = bindingsFactory;
    const AF = new Factory(<RDF.DataFactory> this.dataFactory);
    this.selectorShape = {
      type: 'operation',
      operation: {
        operationType: 'pattern',
        pattern: AF.createPattern(
          this.dataFactory.variable('s'),
          this.dataFactory.variable('p'),
          this.dataFactory.variable('o'),
        ),
      },
      variablesOptional: [
        this.dataFactory.variable('s'),
        this.dataFactory.variable('p'),
        this.dataFactory.variable('o'),
      ],
    };
  }

  public override halt(): void {
    this.store.halt();
  }

  public override resume(): void {
    this.store.resume();
  }

  public static nullifyVariables(term: RDF.Term | undefined, quotedTripleFiltering: boolean): RDF.Term | null {
    return !term || term.termType === 'Variable' || (!quotedTripleFiltering &&
      term.termType === 'Quad' && someTermsNested(term, value => value.termType === 'Variable')) ?
      null :
      term;
  }

  public override async getSelectorShape(): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public override queryBindings(operation: Algebra.Operation, context: IActionContext): BindingsStream {
    this.registeredQueries++;
    if (this.registeredQueries === 1) {
      this.status = StreamingQuerySourceStatus.Running;
    }

    if (operation.type !== 'pattern') {
      throw new Error(`Attempted to pass non-pattern operation '${operation.type}' to StreamingQuerySourceRdfJs`);
    }

    const matchOptions = {
      closeStream: () => {
        throw new Error('closeStream function has not been replaced in streaming store.');
      },
      deleteStream: () => {
        throw new Error('deleteStream function has not been replaced in streaming store.');
      },
    };

    // Create an async iterator from the matched quad stream
    const rawStream = this.store.match(
      StreamingQuerySourceRdfJs.nullifyVariables(operation.subject, false),
      StreamingQuerySourceRdfJs.nullifyVariables(operation.predicate, false),
      StreamingQuerySourceRdfJs.nullifyVariables(operation.object, false),
      StreamingQuerySourceRdfJs.nullifyVariables(operation.graph, false),
      matchOptions,
    );

    if (context) {
      const matchOptionsArray: ({ closeStream: () => void })[] | undefined = context.get(
        KeysStreamingSource.matchOptions,
      );
      if (matchOptionsArray !== undefined) {
        matchOptionsArray.push(matchOptions);
      }
    }

    const quads = filterMatchingQuotedQuads(operation, wrapAsyncIterator<RDF.Quad>(rawStream, { autoStart: false }));

    // Set up-to-date property
    // quads.setProperty('up-to-date', true);
    // if (context) {
    //  const determineChangesEvents = context.get<IDetermineChangesEvents>(KeysDetermineChanges.events);
    //  if (determineChangesEvents) {
    //    determineChangesEvents.on('modified', () => {
    //      quads.setProperty('up-to-date', false);
    //    });
    //   determineChangesEvents.on('up-to-date', () => {
    //      quads.setProperty('up-to-date', true);
    //    });
    //  }
    // }

    // Determine metadata
    if (!quads.getProperty('metadata')) {
      this.setMetadata(quads, operation)
        .catch(error => quads.destroy(error));
    }

    const it = StreamingQuerySourceRdfJs.quadsToBindings(
      quads,
      operation,
      this.dataFactory,
      this.bindingsFactory,
      Boolean(context.get(KeysQueryOperation.unionDefaultGraph)),
      () => {
        this.registeredQueries--;
        if (this.registeredQueries === 0) {
          this.status = StreamingQuerySourceStatus.Idle;
        }
      },
    );

    it.setProperty('delete', () => {
      matchOptions.deleteStream();
    });
    return it;
  }

  protected async setMetadata(
    it: AsyncIterator<RDF.Quad>,
    operation: Algebra.Pattern,
  ): Promise<void> {
    // TODO [2025-01-01]:Check if the source supports quoted triple filtering
    // const quotedTripleFiltering = Boolean(this.store.features?.quotedTripleFiltering);
    const quotedTripleFiltering = false;
    let cardinality: number;
    if (this.store.countQuads) {
      // If the source provides a dedicated method for determining cardinality, use that.
      cardinality = this.store.countQuads(
        StreamingQuerySourceRdfJs.nullifyVariables(operation.subject, quotedTripleFiltering),
        StreamingQuerySourceRdfJs.nullifyVariables(operation.predicate, quotedTripleFiltering),
        StreamingQuerySourceRdfJs.nullifyVariables(operation.object, quotedTripleFiltering),
        StreamingQuerySourceRdfJs.nullifyVariables(operation.graph, quotedTripleFiltering),
      );
    } else {
      // Otherwise, fallback to a sub-optimal alternative where we just call match again to count the quads.
      // WARNING: we can NOT reuse the original data stream here,
      // because we may lose data elements due to things happening async.
      let i = 0;
      cardinality = await new Promise((resolve, reject) => {
        const matches = this.store.match(
          StreamingQuerySourceRdfJs.nullifyVariables(operation.subject, quotedTripleFiltering),
          StreamingQuerySourceRdfJs.nullifyVariables(operation.predicate, quotedTripleFiltering),
          StreamingQuerySourceRdfJs.nullifyVariables(operation.object, quotedTripleFiltering),
          StreamingQuerySourceRdfJs.nullifyVariables(operation.graph, quotedTripleFiltering),
        );
        matches.on('error', reject);
        matches.on('end', () => resolve(i));
        matches.on('data', () => i++);
      });
    }

    it.setProperty('metadata', {
      state: new MetadataValidationState(),
      cardinality: { type: 'exact', value: cardinality },
    });
  }

  public override queryQuads(
    _operation: Algebra.Operation,
    _context: IActionContext,
  ): AsyncIterator<RDF.Quad> {
    throw new Error('queryQuads is not implemented in StreamingQuerySourceRdfJs');
  }

  public override queryBoolean(
    _operation: Algebra.Ask,
    _context: IActionContext,
  ): Promise<boolean> {
    throw new Error('queryBoolean is not implemented in StreamingQuerySourceRdfJs');
  }

  public override queryVoid(
    _operation: Algebra.Update,
    _context: IActionContext,
  ): Promise<void> {
    throw new Error('queryVoid is not implemented in StreamingQuerySourceRdfJs');
  }

  public override toString(): string {
    return `StreamingQuerySourceRdfJs(${this.store.constructor.name})`;
  }

  private static quadsToBindings(
    quads: AsyncIterator<RDF.Quad>,
    pattern: Algebra.Pattern,
    dataFactory: ComunicaDataFactory,
    bindingsFactory: BindingsFactory,
    unionDefaultGraph: boolean,
    onClose: () => void,
  ): BindingsStream {
    const variables = getVariables(pattern);

    // If non-default-graph triples need to be filtered out
    const filterNonDefaultQuads = pattern.graph.termType === 'Variable' &&
      !unionDefaultGraph;

    // Detect duplicate variables in the pattern
    const duplicateElementLinks: Record<string, QuadTermName[][]> | undefined = getDuplicateElementLinks(pattern);

    // Convenience datastructure for mapping quad elements to variables
    const elementVariables: Record<string, string> = reduceTermsNested(
      pattern,
      (acc: Record<string, string>, term: RDF.Term, keys: QuadTermName[]) => {
        if (term.termType === 'Variable') {
          acc[keys.join('_')] = term.value;
        }
        return acc;
      },
      {},
    );

    // Optionally filter, and construct bindings
    let filteredOutput = quads;

    // SPARQL query semantics allow graph variables to only match with named graphs, excluding the default graph
    // But this is not the case when using union default graph semantics
    if (filterNonDefaultQuads) {
      filteredOutput = filteredOutput.filter(quad => quad.graph.termType !== 'DefaultGraph');
    }

    // If there are duplicate variables in the search pattern,
    // make sure that we filter out the triples that don't have equal values for those triple elements,
    // as the rdf-resolve-quad-pattern bus ignores variable names.
    if (duplicateElementLinks) {
      filteredOutput = filteredOutput.filter((quad) => {
        for (const keyLeft in duplicateElementLinks) {
          const keysLeft: QuadTermName[] = <QuadTermName[]> keyLeft.split('_');
          const valueLeft = getValueNestedPath(quad, keysLeft);
          for (const keysRight of duplicateElementLinks[keyLeft]) {
            if (!valueLeft.equals(getValueNestedPath(quad, keysRight))) {
              return false;
            }
          }
        }
        return true;
      });
    }

    // Wrap it in a ClosableIterator, so we can propagate destroy calls
    const bindingsStream = new ClosableIterator(filteredOutput.map<Bindings>(quad => bindingsFactory
      .bindings(Object.keys(elementVariables).map((key) => {
        const keys: QuadTermName[] = <any>key.split('_');
        const variable = elementVariables[key];
        const term = getValueNestedPath(quad, keys);
        return [ dataFactory.variable(variable), term ];
        // TODO [2024-12-01]: write a test for this
      })).setContextEntry(
        KeysBindings.isAddition,
        ((<any>quad).isAddition === undefined) ? true : (<any>quad).isAddition,
      )), {
      onClose: () => {
        quads.destroy();
        onClose();
      },
    });

    // Set the metadata property
    setMetadata(
      dataFactory,
      bindingsStream,
      quads,
      elementVariables,
      variables,
      filterNonDefaultQuads || Boolean(duplicateElementLinks),
    );

    return bindingsStream;
  }
}
