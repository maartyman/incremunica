import type { IExpressionEvaluator } from '@comunica/types';
import { typedLiteral, TypeURL } from '@comunica/utils-expression-evaluator';
import type { IBindingsAggregator } from '@incremunica/bus-bindings-aggregator-factory';
import { AggregateEvaluator } from '@incremunica/bus-bindings-aggregator-factory';
import type * as RDF from '@rdfjs/types';
import { termToString } from 'rdf-string';

export class CountAggregator extends AggregateEvaluator implements IBindingsAggregator {
  private state: Map<string, number> | undefined = undefined;
  public constructor(evaluator: IExpressionEvaluator, distinct: boolean, throwError?: boolean) {
    super(evaluator, distinct, throwError);
  }

  public override emptyValueTerm(): RDF.Term {
    return typedLiteral('0', TypeURL.XSD_INTEGER);
  }

  protected putTerm(term: RDF.Term): void {
    const hash = termToString(term);
    if (this.state === undefined) {
      this.state = new Map<string, number>([[ hash, 1 ]]);
      return;
    }
    this.state.set(hash, (this.state.get(hash) ?? 0) + 1);
  }

  protected removeTerm(term: RDF.Term): void {
    const hash = termToString(term);
    if (this.state === undefined) {
      throw new Error(`Cannot remove term ${hash} from empty count aggregator`);
    }
    const count = this.state.get(hash);
    if (count === undefined) {
      throw new Error(`Cannot remove term ${hash} that was not added to count aggregator`);
    }
    if (count === 1) {
      this.state.delete(hash);
      if (this.state.size === 0) {
        this.state = undefined;
      }
      return;
    }
    this.state.set(hash, count - 1);
  }

  protected termResult(): RDF.Term | undefined {
    if (this.state === undefined) {
      return this.emptyValue();
    }
    if (this.distinct) {
      return typedLiteral(String(this.state.size), TypeURL.XSD_INTEGER);
    }
    let value = 0;
    for (const count of this.state.values()) {
      value += count;
    }
    return typedLiteral(String(value), TypeURL.XSD_INTEGER);
  }
}
