import { ClosableTransformIterator } from '@comunica/bus-query-operation';
import type {
  IActorRdfResolveQuadPatternOutput,
  IQuadSource,
  MediatorRdfResolveQuadPattern,
} from '@comunica/bus-rdf-resolve-quad-pattern';
import { getDataSourceContext } from '@comunica/bus-rdf-resolve-quad-pattern';
import { KeysRdfResolveQuadPattern } from '@comunica/context-entries';
import { BlankNodeScoped } from '@comunica/data-factory';
import type { Quad } from '@comunica/incremental-types';
import { MetadataValidationState } from '@comunica/metadata';
import type { IActionContext, DataSources, IDataSource, MetadataQuads } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { ArrayIterator, UnionIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { mapTerms } from 'rdf-terms';
import type { Algebra } from 'sparqlalgebrajs';
import { Factory } from 'sparqlalgebrajs';

const DF = new DataFactory();

/**
 * A IncrementalFederatedQuadSource can evaluate quad pattern queries over the union of different heterogeneous sources.
 * It will call the given mediator to evaluate each quad pattern query separately.
 */
export class IncrementalFederatedQuadSource implements IQuadSource {
  private static readonly SKOLEM_PREFIX = 'urn:comunica_skolem:source_';

  protected readonly mediatorResolveQuadPattern: MediatorRdfResolveQuadPattern;

  protected readonly sources: DataSources;
  protected readonly contextDefault: IActionContext;
  protected readonly emptyPatterns: Map<IDataSource, RDF.BaseQuad[]>;
  protected readonly sourceIds: Map<IDataSource, string>;
  protected readonly skipEmptyPatterns: boolean;
  protected readonly algebraFactory: Factory;

  public constructor(mediatorResolveQuadPattern: MediatorRdfResolveQuadPattern,
    context: IActionContext, emptyPatterns: Map<IDataSource, RDF.Quad[]>,
    skipEmptyPatterns: boolean) {
    this.mediatorResolveQuadPattern = mediatorResolveQuadPattern;
    this.sources = context.get(KeysRdfResolveQuadPattern.sources)!;
    this.contextDefault = context.delete(KeysRdfResolveQuadPattern.sources);
    this.emptyPatterns = emptyPatterns;
    this.sourceIds = context.get(KeysRdfResolveQuadPattern.sourceIds) ?? new Map();
    this.skipEmptyPatterns = skipEmptyPatterns;
    this.algebraFactory = new Factory();

    // Initialize sources in the emptyPatterns datastructure
    if (this.skipEmptyPatterns) {
      for (const source of this.sources) {
        if (!this.emptyPatterns.has(source)) {
          this.emptyPatterns.set(source, []);
        }
      }
    }
  }

  /**
   * Check if the given RDF term is not bound to an exact value.
   * I.e., if it is not a Variable.
   * @param {RDF.Term} term An RDF term.
   * @return {boolean} If it is not bound.
   */
  public static isTermBound(term: RDF.Term): boolean {
    return term.termType !== 'Variable';
  }

  /**
   * Checks if the given (child) pattern is a more bound version of the given (parent) pattern.
   * This will also return true if the patterns are equal.
   * @param {RDF.BaseQuad} child A child pattern.
   * @param {RDF.BaseQuad} parent A parent pattern.
   * @return {boolean} If child is a sub-pattern of parent
   */
  public static isSubPatternOf(child: RDF.BaseQuad, parent: RDF.BaseQuad): boolean {
    return (!IncrementalFederatedQuadSource.isTermBound(parent.subject) || parent.subject.equals(child.subject)) &&
      (!IncrementalFederatedQuadSource.isTermBound(parent.predicate) || parent.predicate.equals(child.predicate)) &&
      (!IncrementalFederatedQuadSource.isTermBound(parent.object) || parent.object.equals(child.object)) &&
      (!IncrementalFederatedQuadSource.isTermBound(parent.graph) || parent.graph.equals(child.graph));
  }

  /**
   * If the given term is a blank node, return a deterministic named node for it
   * based on the source id and the blank node value.
   * @param term Any RDF term.
   * @param sourceId A source identifier.
   * @return If the given term was a blank node, this will return a skolemized named node, otherwise the original term.
   */
  public static skolemizeTerm(term: RDF.Term, sourceId: string): RDF.Term | BlankNodeScoped {
    if (term.termType === 'BlankNode') {
      return new BlankNodeScoped(`bc_${sourceId}_${term.value}`,
        DF.namedNode(`${IncrementalFederatedQuadSource.SKOLEM_PREFIX}${sourceId}:${term.value}`));
    }
    return term;
  }

  /**
   * Skolemize all terms in the given quad.
   * @param quad An RDF quad.
   * @param sourceId A source identifier.
   * @return The skolemized quad.
   */
  public static skolemizeQuad<Q extends RDF.BaseQuad = RDF.Quad>(quad: Q, sourceId: string): Q {
    return mapTerms(quad, term => IncrementalFederatedQuadSource.skolemizeTerm(term, sourceId));
  }

  /**
   * If a given term was a skolemized named node for the given source id,
   * deskolemize it again to a blank node.
   * If the given term was a skolemized named node for another source, return false.
   * If the given term was not a skolemized named node, return the original term.
   * @param term Any RDF term.
   * @param sourceId A source identifier.
   */
  public static deskolemizeTerm(term: RDF.Term, sourceId: string): RDF.Term | null {
    if (term.termType === 'BlankNode' && 'skolemized' in term) {
      term = (<BlankNodeScoped> term).skolemized;
    }
    if (term.termType === 'NamedNode' && term.value.startsWith(IncrementalFederatedQuadSource.SKOLEM_PREFIX)) {
      const colonSeparator = term.value.indexOf(':', IncrementalFederatedQuadSource.SKOLEM_PREFIX.length);
      const termSourceId = term.value.slice(IncrementalFederatedQuadSource.SKOLEM_PREFIX.length, colonSeparator);
      // We had a skolemized term
      if (termSourceId === sourceId) {
        // It came from the correct source
        const termLabel = term.value.slice(colonSeparator + 1, term.value.length);
        return DF.blankNode(termLabel);
      }
      // It came from a different source
      return null;
    }
    return term;
  }

  /**
   * Deskolemize all terms in the given quad.
   * @param quad An RDF quad.
   * @param sourceId A source identifier.
   * @return The deskolemized quad.
   */
  public static deskolemizeQuad<Q extends RDF.BaseQuad = RDF.Quad>(quad: Q, sourceId: string): Q {
    return mapTerms(quad, (term: RDF.Term): RDF.Term => {
      const newTerm = IncrementalFederatedQuadSource.deskolemizeTerm(term, sourceId);
      // If the term was skolemized in a different source then dont deskolemize it
      return !newTerm ? term : newTerm;
    });
  }

  /**
   * If the given source is guaranteed to produce an empty result for the given pattern.
   *
   * This prediction is done based on the 'emptyPatterns' datastructure that is stored within this actor.
   * Every time an empty pattern is passed, this pattern is stored in this datastructure for this source.
   * If this pattern (or a more bound pattern) is queried, we know for certain that it will be empty again.
   * This is under the assumption that sources will remain static during query evaluation.
   *
   * @param {IQuerySource} source
   * @param {RDF.BaseQuad} pattern
   * @return {boolean}
   */
  public isSourceEmpty(source: IDataSource, pattern: RDF.BaseQuad): boolean {
    if (!this.skipEmptyPatterns) {
      return false;
    }
    const emptyPatterns: RDF.BaseQuad[] | undefined = this.emptyPatterns.get(source);
    if (emptyPatterns) {
      for (const emptyPattern of emptyPatterns) {
        if (IncrementalFederatedQuadSource.isSubPatternOf(pattern, emptyPattern)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Get the unique, deterministic id for the given source.
   * @param source A data source.
   * @return The id of the given source.
   */
  public getSourceId(source: IDataSource): string {
    let sourceId = this.sourceIds.get(source);
    if (sourceId === undefined) {
      sourceId = `${this.sourceIds.size}`;
      this.sourceIds.set(source, sourceId);
    }
    return sourceId;
  }

  public match(subject: RDF.Term, predicate: RDF.Term, object: RDF.Term, graph: RDF.Term): AsyncIterator<RDF.Quad> {
    // Counters for our metadata
    const metadata: MetadataQuads = {
      cardinality: { type: 'exact', value: 0 },
      canContainUndefs: false,
      state: new MetadataValidationState(),
    };
    let remainingSources = this.sources.length;

    // Anonymous function to handle cardinality from metadata
    const checkEmitMetadata = (currentTotalItems: number, source: IDataSource,
      pattern: RDF.BaseQuad | undefined, lastMetadata?: MetadataQuads): void => {
      if (this.skipEmptyPatterns && !currentTotalItems && pattern && !this.isSourceEmpty(source, pattern)) {
        this.emptyPatterns.get(source)!.push(pattern);
      }
      if (!remainingSources) {
        if (lastMetadata && this.sources.length === 1) {
          // If we only had one source, emit the metadata as-is.
          it.setProperty('metadata', lastMetadata);
        } else {
          it.setProperty('metadata', metadata);
        }
      }
    };

    const proxyIt: Promise<AsyncIterator<RDF.Quad>[]> = Promise.all(this.sources.map(async source => {
      const sourceId = this.getSourceId(source);

      // Deskolemize terms, so we send the original blank nodes to each source.
      // Note that some sources may not match bnodes by label. SPARQL endpoints for example consider them variables.
      const patternS = IncrementalFederatedQuadSource.deskolemizeTerm(subject, sourceId);
      const patternP = IncrementalFederatedQuadSource.deskolemizeTerm(predicate, sourceId);
      const patternO = IncrementalFederatedQuadSource.deskolemizeTerm(object, sourceId);
      const patternG = IncrementalFederatedQuadSource.deskolemizeTerm(graph, sourceId);
      let pattern: Algebra.Pattern | undefined;

      // Prepare the context for this specific source
      let context: IActionContext = getDataSourceContext(source, this.contextDefault);

      context = context.set(KeysRdfResolveQuadPattern.source, source);

      let output: IActorRdfResolveQuadPatternOutput;
      // If any of the deskolemized blank nodes originate from another source,
      // or if we can predict that the given source will have no bindings for the given pattern,
      // return an empty iterator.
      if (!patternS || !patternP || !patternO || !patternG ||
        // eslint-disable-next-line no-cond-assign
        this.isSourceEmpty(source, pattern = this.algebraFactory
          .createPattern(patternS, patternP, patternO, patternG))) {
        output = { data: new ArrayIterator([], { autoStart: false }) };
        output.data.setProperty('metadata', { cardinality: 0, canContainUndefs: false });
      } else {
        output = await this.mediatorResolveQuadPattern.mediate({ pattern, context });
      }

      // Handle the metadata from this source
      output.data.getProperty('metadata', (subMetadata: MetadataQuads) => {
        if (!subMetadata.cardinality || !Number.isFinite(subMetadata.cardinality.value)) {
          // We're already at infinite, so ignore any later metadata
          metadata.cardinality.type = 'estimate';
          metadata.cardinality.value = Number.POSITIVE_INFINITY;
          remainingSources = 0;
        } else {
          if (subMetadata.cardinality.type === 'estimate') {
            metadata.cardinality.type = 'estimate';
          }
          metadata.cardinality.value += subMetadata.cardinality.value;
          remainingSources--;
        }
        if (metadata.requestTime || subMetadata.requestTime) {
          metadata.requestTime = metadata.requestTime || 0;
          subMetadata.requestTime = subMetadata.requestTime || 0;
          metadata.requestTime += subMetadata.requestTime;
        }
        if (metadata.pageSize || subMetadata.pageSize) {
          metadata.pageSize = metadata.pageSize || 0;
          subMetadata.pageSize = subMetadata.pageSize || 0;
          metadata.pageSize += subMetadata.pageSize;
        }
        if (subMetadata.canContainUndefs) {
          metadata.canContainUndefs = true;
        }
        checkEmitMetadata(metadata.cardinality.value, source, pattern, subMetadata);
      });

      // Determine the data stream from this source
      const data = output.data.map(quad => {
        const skolemizedQuad = IncrementalFederatedQuadSource.skolemizeQuad<Quad>(<Quad>quad, sourceId);
        skolemizedQuad.diff = (<Quad>quad).diff;
        return skolemizedQuad;
      });

      // Forward errors to our final iterator
      data.on('error', error => it.emit('error', error));

      return data;
    }));

    // Take the union of all source streams
    const it = new ClosableTransformIterator(async() => new UnionIterator(await proxyIt), {
      autoStart: false,
      onClose() {
        // Destroy the sub-iterators
        proxyIt.then(proxyItResolved => {
          for (const subIt of proxyItResolved) {
            subIt.destroy();
          }
        }, () => {
          // Void errors
        });
      },
    });

    // If we have 0 sources, immediately emit metadata
    if (this.sources.length === 0) {
      it.setProperty('metadata', metadata);
    }

    return it;
  }
}
