import { MetadataValidationState } from '@comunica/metadata';
import type {IQuerySource, BindingsStream, IActionContext, FragmentSelectorShape, Bindings} from '@comunica/types';
import { KeysGuard, KeysStreamingSource } from '@incremunica/context-entries';
import {IGuardEvents, Quad} from '@incremunica/incremental-types';
import type * as RDF from '@rdfjs/types';
import { wrap as wrapAsyncIterator } from 'asynciterator';
import { AsyncIterator } from 'asynciterator';
import {Algebra, Factory} from 'sparqlalgebrajs';
import { DataFactory } from 'rdf-data-factory';
import {BindingsFactory} from "@comunica/bindings-factory";
import {
  filterTermsNested,
  getValueNestedPath,
  QuadTermName,
  reduceTermsNested,
  someTermsNested,
  uniqTerms
} from "rdf-terms";
import { filterMatchingQuotedQuads, getVariables, getDuplicateElementLinks, setMetadata } from '@comunica/bus-query-source-identify';
import {KeysQueryOperation} from "@comunica/context-entries";
import {StreamingStore} from "@incremunica/incremental-rdf-streaming-store";
import { ClosableIterator } from '@comunica/bus-query-operation';
import {ActionContextKeyIsAddition} from "@incremunica/actor-merge-bindings-context-is-addition";
import {Duplex, Transform} from "readable-stream";

const AF = new Factory();
const DF = new DataFactory<RDF.BaseQuad>();

export class StreamingQuerySourceRdfJs implements IQuerySource {
  protected static readonly SELECTOR_SHAPE: FragmentSelectorShape = {
    type: 'operation',
    operation: {
      operationType: 'pattern',
      pattern: AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o')),
    },
    variablesOptional: [
      DF.variable('s'),
      DF.variable('p'),
      DF.variable('o'),
    ],
  };

  public referenceValue: string | RDF.Source;
  public context?: IActionContext;
  public store: StreamingStore<Quad>;
  private readonly bindingsFactory: BindingsFactory;

  public constructor(store: StreamingStore<Quad>, bindingsFactory: BindingsFactory) {
    this.store = store;
    this.referenceValue = store;
    this.bindingsFactory = bindingsFactory;
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
    return StreamingQuerySourceRdfJs.SELECTOR_SHAPE;
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
    )

    if (context) {
      const matchOptionsArray: ({ stopMatch: () => void })[] | undefined = context.get(
        KeysStreamingSource.matchOptions,
      );
      if (matchOptionsArray !== undefined) {
        matchOptionsArray.push(matchOptions);
      }
    }

    let quads = filterMatchingQuotedQuads(operation, wrapAsyncIterator<RDF.Quad>(rawStream, { autoStart: false }));

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
      this.bindingsFactory,
      Boolean(context.get(KeysQueryOperation.unionDefaultGraph)),
    );
  }

  // TODO implement setMetadata make a proper estimation for the cardinality
  protected async setMetadata(
    it: AsyncIterator<RDF.Quad>,
    operation: Algebra.Pattern,
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

  static quadsToBindings(
    quads: AsyncIterator<RDF.Quad>,
    pattern: Algebra.Pattern,
    bindingsFactory: BindingsFactory,
    unionDefaultGraph: boolean,
  ): BindingsStream {
    const variables = getVariables(pattern);

    // If non-default-graph triples need to be filtered out
    const filterNonDefaultQuads = pattern.graph.termType === 'Variable'
      && !unionDefaultGraph;

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
    const bindingsStream = new ClosableIterator(filteredOutput.map<Bindings>(quad => {
      return bindingsFactory
        .bindings(Object.keys(elementVariables).map((key) => {
          const keys: QuadTermName[] = <any>key.split('_');
          const variable = elementVariables[key];
          const term = getValueNestedPath(quad, keys);
          return [ DF.variable(variable), term ];
          //TODO write a test for this
        })).setContextEntry(new ActionContextKeyIsAddition(), ((<any>quad).diff == undefined)? true : (<any>quad).diff);
    }), {
      onClose: () => quads.destroy(),
    });

    // Set the metadata property
    setMetadata(bindingsStream, quads, elementVariables, variables, filterNonDefaultQuads || Boolean(duplicateElementLinks));

    return bindingsStream;
  }
}