import type { ITermFunction } from '@comunica/bus-function-factory';
import type { ComunicaDataFactory, IExpressionEvaluator } from '@comunica/types';
import type { NumericLiteral } from '@comunica/utils-expression-evaluator';
import { typedLiteral, TypeURL } from '@comunica/utils-expression-evaluator';
import { AggregateEvaluator } from '@incremunica/bus-bindings-aggregator-factory';
import type * as RDF from '@rdfjs/types';
import { termToString } from 'rdf-string';

interface ISumState {
  index: Map<string, number>;
  sum: NumericLiteral;
}

export class SumAggregator extends AggregateEvaluator {
  private state: ISumState | undefined = undefined;

  public constructor(
    evaluator: IExpressionEvaluator,
    distinct: boolean,
    private readonly dataFactory: ComunicaDataFactory,
    private readonly additionFunction: ITermFunction,
    private readonly subtractionFunction: ITermFunction,
    throwError?: boolean,
  ) {
    super(evaluator, distinct, throwError);
  }

  public override emptyValueTerm(): RDF.Term {
    return typedLiteral('0', TypeURL.XSD_INTEGER);
  }

  protected putTerm(term: RDF.Term): void {
    const hash = termToString(term);
    const value = this.termToNumericOrError(term);
    if (this.state === undefined) {
      this.state = {
        index: new Map<string, number>([[ hash, 1 ]]),
        sum: value,
      };
      return;
    }
    let count = this.state.index.get(hash);
    if (count === undefined) {
      count = 0;
    }
    this.state.index.set(hash, count + 1);
    if (!this.distinct || count === 0) {
      this.state.sum = <NumericLiteral> this.additionFunction.applyOnTerms([ this.state.sum, value ], this.evaluator);
    }
  }

  protected removeTerm(term: RDF.Term): void {
    const hash = termToString(term);
    const value = this.termToNumericOrError(term);
    if (this.state === undefined) {
      throw new Error(`Cannot remove term ${hash} from empty sum aggregator`);
    }
    const count = this.state.index.get(hash);
    if (count === undefined) {
      throw new Error(`Cannot remove term ${hash} that was not added to sum aggregator`);
    }
    if (count === 1) {
      this.state.index.delete(hash);
      if (this.state.index.size === 0) {
        this.state = undefined;
        return;
      }
    } else {
      this.state.index.set(hash, count - 1);
    }
    if (!this.distinct || count === 1) {
      this.state.sum = <NumericLiteral> this.subtractionFunction
        .applyOnTerms([ this.state.sum, value ], this.evaluator);
    }
  }

  protected termResult(): RDF.Term | undefined {
    if (this.state === undefined) {
      return this.emptyValue();
    }
    return this.state.sum.toRDF(this.dataFactory);
  }
}
