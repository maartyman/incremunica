import type { IExpressionEvaluator } from '@comunica/types';
import type { IBindingsAggregator } from '@incremunica/bus-bindings-aggregator-factory';
import { AggregateEvaluator } from '@incremunica/bus-bindings-aggregator-factory';
import type * as RDF from '@rdfjs/types';
import { termToString } from 'rdf-string';

export class SampleAggregator extends AggregateEvaluator implements IBindingsAggregator {
  private state: Map<string, { value: RDF.Term; count: number }> | undefined = undefined;

  public constructor(evaluator: IExpressionEvaluator, distinct: boolean, throwError?: boolean) {
    super(evaluator, distinct, throwError);
  }

  protected putTerm(term: RDF.Term): void {
    if (this.state === undefined) {
      this.state = new Map<string, { value: RDF.Term; count: number }>([[ term.value, { value: term, count: 1 }]]);
      return;
    }
    this.state.set(term.value, { value: term, count: (this.state.get(term.value)?.count ?? 0) + 1 });
  }

  protected removeTerm(term: RDF.Term): void {
    if (this.state === undefined) {
      throw new Error(`Cannot remove term ${termToString(term)} from empty sample aggregator`);
    }
    const count = this.state.get(term.value);
    if (count === undefined) {
      throw new Error(`Cannot remove term ${termToString(term)} that was not added to sample aggregator`);
    }
    if (count.count === 1) {
      this.state.delete(term.value);
    } else {
      this.state.set(term.value, { value: term, count: count.count - 1 });
    }
  }

  protected termResult(): RDF.Term | undefined {
    if (this.state === undefined) {
      return this.emptyValue();
    }
    if (this.state.size === 0) {
      return this.emptyValue();
    }
    return this.state.values().next().value.value;
  }
}
