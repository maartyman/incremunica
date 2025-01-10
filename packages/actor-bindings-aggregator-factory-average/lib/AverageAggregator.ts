import type { ITermFunction } from '@comunica/bus-function-factory';
import type { ComunicaDataFactory, IExpressionEvaluator } from '@comunica/types';
import * as Eval from '@comunica/utils-expression-evaluator';
import { AggregateEvaluator } from '@incremunica/bus-bindings-aggregator-factory';
import type { IBindingsAggregator } from '@incremunica/bus-bindings-aggregator-factory';
import type * as RDF from '@rdfjs/types';
import { termToString } from 'rdf-string';

interface IAverageState {
  sum: Eval.NumericLiteral;
  count: number;
}

export class AverageAggregator extends AggregateEvaluator implements IBindingsAggregator {
  private state: IAverageState | undefined = undefined;

  public constructor(
    evaluator: IExpressionEvaluator,
    distinct: boolean,
    private readonly dataFactory: ComunicaDataFactory,
    private readonly additionFunction: ITermFunction,
    private readonly subtractionFunction: ITermFunction,
    private readonly divisionFunction: ITermFunction,
    throwError?: boolean,
  ) {
    super(evaluator, distinct, throwError);
  }

  public override emptyValueTerm(): RDF.Term {
    return Eval.typedLiteral('0', Eval.TypeURL.XSD_INTEGER);
  }

  public putTerm(term: RDF.Term): void {
    if (this.state === undefined) {
      const sum = this.termToNumericOrError(term);
      this.state = { sum, count: 1 };
    } else {
      const internalTerm = this.termToNumericOrError(term);
      this.state.sum = <Eval.NumericLiteral> this.additionFunction
        .applyOnTerms([ this.state.sum, internalTerm ], this.evaluator);
      this.state.count++;
    }
  }

  protected removeTerm(term: RDF.Term): void {
    if (this.state === undefined) {
      throw new Error(`Cannot remove term ${termToString(term)} from empty average aggregator`);
    }
    const internalTerm = this.termToNumericOrError(term);
    this.state.sum = <Eval.NumericLiteral> this.subtractionFunction
      .applyOnTerms([ this.state.sum, internalTerm ], this.evaluator);
    this.state.count--;
  }

  public termResult(): RDF.Term | undefined {
    if (this.state === undefined) {
      return this.emptyValue();
    }
    const count = new Eval.IntegerLiteral(this.state.count);
    const result = this.divisionFunction.applyOnTerms([ this.state.sum, count ], this.evaluator);
    return result.toRDF(this.dataFactory);
  }
}
