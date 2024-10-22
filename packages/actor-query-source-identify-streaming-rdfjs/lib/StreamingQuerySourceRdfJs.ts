import {
  filterMatchingQuotedQuads,
  getVariables,
  getDuplicateElementLinks,
  setMetadata,
} from '@comunica/bus-query-source-identify';
import { KeysQueryOperation } from '@comunica/context-entries';
import type {
  IQuerySource,
  BindingsStream,
  IActionContext,
  FragmentSelectorShape,
  Bindings,
  ComunicaDataFactory,
} from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { ClosableIterator } from '@comunica/utils-iterator';
import { MetadataValidationState } from '@comunica/utils-metadata';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import { KeysGuard, KeysStreamingSource } from '@incremunica/context-entries';
import type { StreamingStore } from '@incremunica/incremental-rdf-streaming-store';
import type { IGuardEvents, Quad } from '@incremunica/incremental-types';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { wrap as wrapAsyncIterator } from 'asynciterator';
import type {
  QuadTermName,
} from 'rdf-terms';
import {
  filterTermsNested,
  getValueNestedPath,
  reduceTermsNested,
  someTermsNested,
  uniqTerms,
} from 'rdf-terms';
import { Factory } from 'sparqlalgebrajs';
import type { Algebra } from 'sparqlalgebrajs';

export class StreamingQuerySourceRdfJs implements IQuerySource {
  public referenceValue: string | RDF.Source;
  public context?: IActionContext;
  public store: StreamingStore<Quad>;
  protected readonly selectorShape: FragmentSelectorShape;
  private readonly dataFactory: ComunicaDataFactory;
  private readonly bindingsFactory: BindingsFactory;

  public constructor(store: StreamingStore<Quad>, dataFactory: ComunicaDataFactory, bindingsFactory: BindingsFactory) {
    this.store = store;
    this.referenceValue = store;
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

  public static nullifyVariables(term: RDF.Term | undefined, quotedTripleFiltering: boolean): RDF.Term | undefined {
    return !term || term.termType === 'Variable' || (!quotedTripleFiltering &&
      term.termType === 'Quad' && someTermsNested(term, value => value.termType === 'Variable')) ?
      undefined :
      term;
  }

  public static hasDuplicateVariables(pattern: RDF.BaseQuad): boolean {
    const variables = filterTermsNested(pattern, term => term.termType === 'Variable');
    return variables.length > 1 && uniqTerms(variables).length < variables.length;
  }

  public async getSelectorShape(): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public queryBindings(operation: Algebra.Operation, context: IActionContext): BindingsStream {
    if (operation.type !== 'pattern') {
      throw new Error(`Attempted to pass non-pattern operation '${operation.type}' to StreamingQuerySourceRdfJs`);
    }

    const matchOptions = {
      stopMatch() {
        throw new Error('stopMatch function has not been replaced in streaming store.');
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
      const matchOptionsArray: ({ stopMatch: () => void })[] | undefined = context.get(
        KeysStreamingSource.matchOptions,
      );
      if (matchOptionsArray !== undefined) {
        matchOptionsArray.push(matchOptions);
      }
    }

    const quads = filterMatchingQuotedQuads(operation, wrapAsyncIterator<RDF.Quad>(rawStream, { autoStart: false }));

    // Set up-to-date property
    quads.setProperty('up-to-date', true);
    if (context) {
      const guardEvents = context.get<IGuardEvents>(KeysGuard.events);
      if (guardEvents) {
        guardEvents.on('modified', () => {
          quads.setProperty('up-to-date', false);
        });
        guardEvents.on('up-to-date', () => {
          quads.setProperty('up-to-date', true);
        });
      }
    }

    // Determine metadata
    if (!quads.getProperty('metadata')) {
      this.setMetadata(quads, operation)
        .catch(error => quads.destroy(error));
    }

    return StreamingQuerySourceRdfJs.quadsToBindings(
      quads,
      operation,
      this.dataFactory,
      this.bindingsFactory,
      Boolean(context.get(KeysQueryOperation.unionDefaultGraph)),
    );
  }

  // TODO implement setMetadata make a proper estimation for the cardinality
  protected async setMetadata(
    it: AsyncIterator<RDF.Quad>,
    _operation: Algebra.Pattern,
  ): Promise<void> {
    const cardinality = 1;

    it.setProperty('metadata', {
      state: new MetadataValidationState(),
      cardinality: { type: 'exact', value: cardinality },
      canContainUndefs: false,
    });
  }

  public queryQuads(
    _operation: Algebra.Operation,
    _context: IActionContext,
  ): AsyncIterator<RDF.Quad> {
    throw new Error('queryQuads is not implemented in StreamingQuerySourceRdfJs');
  }

  public queryBoolean(
    _operation: Algebra.Ask,
    _context: IActionContext,
  ): Promise<boolean> {
    throw new Error('queryBoolean is not implemented in StreamingQuerySourceRdfJs');
  }

  public queryVoid(
    _operation: Algebra.Update,
    _context: IActionContext,
  ): Promise<void> {
    throw new Error('queryVoid is not implemented in StreamingQuerySourceRdfJs');
  }

  public toString(): string {
    return `StreamingQuerySourceRdfJs(${this.store.constructor.name})`;
  }

  private static quadsToBindings(
    quads: AsyncIterator<RDF.Quad>,
    pattern: Algebra.Pattern,
    dataFactory: ComunicaDataFactory,
    bindingsFactory: BindingsFactory,
    unionDefaultGraph: boolean,
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
        // TODO write a test for this
      })).setContextEntry(
        new ActionContextKeyIsAddition(),
        ((<any>quad).diff === undefined) ? true : (<any>quad).diff,
      )), {
      onClose: () => quads.destroy(),
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
