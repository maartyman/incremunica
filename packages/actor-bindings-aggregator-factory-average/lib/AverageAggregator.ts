import type { ITermFunction } from '@comunica/bus-function-factory';
import type { ComunicaDataFactory, IExpressionEvaluator } from '@comunica/types';
import * as Eval from '@comunica/utils-expression-evaluator';
import { AggregateEvaluator } from '@incremunica/bus-bindings-aggregator-factory';
import type { IBindingsAggregator } from '@incremunica/bus-bindings-aggregator-factory';
import type * as RDF from '@rdfjs/types';
import { termToString } from 'rdf-string';

interface IAverageState {
  index: Map<string, number>;
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

  protected putTerm(term: RDF.Term): void {
    const hash = termToString(term);
    const value = this.termToNumericOrError(term);
    if (this.state === undefined) {
      this.state = { index: new Map<string, number>([[ hash, 1 ]]), sum: value, count: 1 };
      return;
    }
    this.state.index.set(hash, (this.state.index.get(hash) ?? 0) + 1);
    this.state.sum = <Eval.NumericLiteral> this.additionFunction
      .applyOnTerms([ this.state.sum, value ], this.evaluator);
    this.state.count++;
  }

  protected removeTerm(term: RDF.Term): void {
    const hash = termToString(term);
    const value = this.termToNumericOrError(term);
    if (this.state === undefined) {
      throw new Error(`Cannot remove term ${termToString(term)} from empty average aggregator`);
    }
    const count = this.state.index.get(hash);
    if (count === undefined) {
      throw new Error(`Cannot remove term ${termToString(term)} that was not added to average aggregator`);
    }
    if (count === 1) {
      this.state.index.delete(hash);
      if (this.state.count === 1) {
        this.state = undefined;
        return;
      }
    } else {
      this.state.index.set(hash, count - 1);
    }
    this.state.sum = <Eval.NumericLiteral> this.subtractionFunction
      .applyOnTerms([ this.state.sum, value ], this.evaluator);
    this.state.count--;
  }

  protected termResult(): RDF.Term | undefined {
    if (this.state === undefined) {
      return this.emptyValue();
    }
    const count = new Eval.IntegerLiteral(this.state.count);
    const result = this.divisionFunction.applyOnTerms([ this.state.sum, count ], this.evaluator);
    return result.toRDF(this.dataFactory);
  }
}
