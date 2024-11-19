import { KeysExpressionEvaluator } from '@comunica/context-entries';
import type { IExpressionEvaluator, ISuperTypeProvider } from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import * as Eval from '@comunica/utils-expression-evaluator';
import { KeysBindings } from '@incremunica/context-entries';
import type * as RDF from '@rdfjs/types';
import * as RdfString from 'rdf-string';

/**
 * This is the base class for all aggregators.
 * NOTE: The wildcard count aggregator significantly differs from the others and overloads parts of this class.
 */
export abstract class AggregateEvaluator {
  private errorOccurred = false;
  private lastResult: undefined | null | RDF.Term = undefined;

  protected readonly variableValues: Map<string, number>;

  protected readonly superTypeProvider: ISuperTypeProvider;
  protected readonly termTransformer: Eval.TermTransformer;

  protected constructor(
    protected readonly evaluator: IExpressionEvaluator,
    protected readonly distinct: boolean,
    private readonly throwError = false,
  ) {
    this.errorOccurred = false;
    this.superTypeProvider = evaluator.context.getSafe(KeysExpressionEvaluator.superTypeProvider);
    this.termTransformer = new Eval.TermTransformer(this.superTypeProvider);

    this.variableValues = new Map();
  }

  protected abstract putTerm(term: RDF.Term): void;
  protected abstract removeTerm(term: RDF.Term): void;
  protected abstract termResult(): RDF.Term | undefined;

  public emptyValueTerm(): RDF.Term | undefined {
    return undefined;
  }

  /**
   * The spec says to throw an error when a set function is called on an empty
   * set (unless explicitly mentioned otherwise like COUNT).
   * However, aggregate error handling says to not bind the result in case of an
   * error. So to simplify logic in the caller, we return undefined by default.
   */
  public emptyValue(): RDF.Term | undefined {
    const val = this.emptyValueTerm();
    if (val === undefined && this.throwError) {
      throw new Eval.EmptyAggregateError();
    }
    return val;
  }

  /**
   * Base implementation of putBindings, that evaluates to a term and then calls putTerm.
   * The WildcardCountAggregator will completely discard this implementation.
   * @param bindings
   */
  public async putBindings(bindings: Bindings): Promise<void> {
    if (this.errorOccurred) {
      return;
    }
    try {
      const term = await this.evaluator.evaluate(bindings);
      if (!term || this.errorOccurred) {
        return;
      }

      if (bindings.getContextEntry(KeysBindings.isAddition)) {
        // Handle DISTINCT before putting the term
        if (this.distinct) {
          const hash = RdfString.termToString(term);
          let count = this.variableValues.get(hash);
          if (!count) {
            this.putTerm(term);
            count = 0;
          }
          this.variableValues.set(hash, count + 1);
        } else {
          this.putTerm(term);
        }
      } else if (this.distinct) {
        // Handle DISTINCT before putting the term
        const hash = RdfString.termToString(term);
        const count = this.variableValues.get(hash);
        if (count === 1) {
          this.removeTerm(term);
          this.variableValues.delete(hash);
        } else if (count === undefined) {
          this.safeThrow('count is undefined, this shouldn\'t happen');
        } else {
          this.variableValues.set(hash, count - 1);
        }
      } else {
        this.removeTerm(term);
      }
    } catch (error: unknown) {
      this.safeThrow(error);
    }
  }

  /**
   * Base implementation of result, that calls termResult and caches the result.
   * @returns {RDF.Term | undefined | null}
   * The result term, or undefined if no new result is available, or null if an error occurred.
   */
  public result(): RDF.Term | undefined | null {
    if (this.errorOccurred) {
      if (this.lastResult === null) {
        return undefined;
      }
      this.lastResult = null;
      return null;
    }
    const result = this.termResult();
    if ((result === undefined && this.lastResult === undefined) || result?.equals(this.lastResult)) {
      return undefined;
    }
    this.lastResult = result;
    return result;
  }

  private safeThrow(err: unknown): void {
    if (this.throwError) {
      throw err;
    } else {
      this.errorOccurred = true;
    }
  }

  protected termToNumericOrError(term: RDF.Term): Eval.NumericLiteral {
    if (term.termType !== 'Literal') {
      throw new Error(`Term with value ${term.value} has type ${term.termType} and is not a numeric literal`);
    } else if (
      !Eval.isSubTypeOf(term.datatype.value, Eval.TypeAlias.SPARQL_NUMERIC, this.superTypeProvider)) {
      throw new Error(`Term datatype ${term.datatype.value} with value ${term.value} has type ${term.termType} and is not a numeric literal`);
    }
    return <Eval.NumericLiteral> this.termTransformer.transformLiteral(term);
  }
}
