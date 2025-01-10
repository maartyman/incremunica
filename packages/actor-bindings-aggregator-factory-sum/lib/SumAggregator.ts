import type { ITermFunction } from '@comunica/bus-function-factory';
import type { ComunicaDataFactory, IExpressionEvaluator } from '@comunica/types';
import type { NumericLiteral } from '@comunica/utils-expression-evaluator';
import { typedLiteral, TypeURL } from '@comunica/utils-expression-evaluator';
import { AggregateEvaluator } from '@incremunica/bus-bindings-aggregator-factory';
import type * as RDF from '@rdfjs/types';
import { termToString } from 'rdf-string';

type SumState = NumericLiteral;

export class SumAggregator extends AggregateEvaluator {
  private state: SumState | undefined = undefined;

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

  public putTerm(term: RDF.Term): void {
    if (this.state === undefined) {
      this.state = this.termToNumericOrError(term);
    } else {
      const internalTerm = this.termToNumericOrError(term);
      this.state = <NumericLiteral> this.additionFunction.applyOnTerms([ this.state, internalTerm ], this.evaluator);
    }
  }

  public removeTerm(term: RDF.Term): void {
    if (this.state === undefined) {
      throw new Error(`Cannot remove term ${termToString(term)} from empty sum aggregator`);
    } else {
      const internalTerm = this.termToNumericOrError(term);
      this.state = <NumericLiteral> this.subtractionFunction.applyOnTerms([ this.state, internalTerm ], this.evaluator);
    }
  }

  public termResult(): RDF.Term | undefined {
    if (this.state === undefined) {
      return this.emptyValue();
    }
    return this.state.toRDF(this.dataFactory);
  }
}
