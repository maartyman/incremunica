import type { IExpressionEvaluator } from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import { typedLiteral, TypeURL } from '@comunica/utils-expression-evaluator';
import type { IBindingsAggregator } from '@incremunica/bus-bindings-aggregator-factory';
import { AggregateEvaluator } from '@incremunica/bus-bindings-aggregator-factory';
import { KeysBindings } from '@incremunica/context-entries';
import type * as RDF from '@rdfjs/types';
import * as RdfString from 'rdf-string';

export class WildcardCountAggregator extends AggregateEvaluator implements IBindingsAggregator {
  private readonly bindingValues: Map<string, Map<string, number>> = new Map();
  private state = 0;

  public constructor(evaluator: IExpressionEvaluator, distinct: boolean, throwError?: boolean) {
    super(evaluator, distinct, throwError);
  }

  protected putTerm(_term: RDF.Term): void {
    // Do nothing, not needed
  }

  protected removeTerm(_term: RDF.Term): void {
    // Do nothing, not needed
  }

  public override async putBindings(bindings: Bindings): Promise<void> {
    if (!this.skipDistinctBindings(bindings)) {
      const isAddition = bindings.getContextEntry(KeysBindings.isAddition) ?? true;
      if (isAddition) {
        this.state++;
      } else {
        this.state--;
      }
    }
  }

  public override emptyValueTerm(): RDF.Term {
    return typedLiteral('0', TypeURL.XSD_INTEGER);
  }

  protected termResult(): RDF.Term | undefined {
    if (this.state === 0) {
      return this.emptyValue();
    }
    return typedLiteral(String(this.state), TypeURL.XSD_INTEGER);
  }

  /**
   * Returns true if the given bindings should be skipped.
   * @param bindings
   * @private
   */
  private skipDistinctBindings(bindings: Bindings): boolean {
    const bindingList: [RDF.Variable, RDF.Term][] = [ ...bindings ];
    bindingList.sort((first, snd) => first[0].value.localeCompare(snd[0].value));
    const variables = bindingList.map(([ variable ]) => variable.value).join(',');
    const terms = bindingList.map(([ , term ]) => RdfString.termToString(term)).join(',');
    let termsMap = this.bindingValues.get(variables);

    const isAddition = bindings.getContextEntry(KeysBindings.isAddition) ?? true;
    if (isAddition) {
      if (termsMap === undefined) {
        termsMap = new Map([[ terms, 1 ]]);
        this.bindingValues.set(variables, termsMap);
        return false;
      }
      const count = termsMap.get(terms);
      if (count === undefined) {
        termsMap.set(terms, 1);
        return false;
      }
      termsMap.set(terms, count + 1);
      // Return true if we are in distinct mode otherwise return false
      return this.distinct;
    }
    if (termsMap === undefined) {
      throw new Error(`Cannot remove bindings ${bindings.toString()} that was not added to wildcard-count aggregator`);
    }
    const count = termsMap.get(terms);
    if (count === undefined) {
      throw new Error(`Cannot remove bindings ${bindings.toString()} that was not added to wildcard-count aggregator`);
    }
    if (count === 1) {
      termsMap.delete(terms);
      if (termsMap.size === 0) {
        this.bindingValues.delete(variables);
      }
      // Return true if we are in distinct mode otherwise return false
      return false;
    }
    termsMap.set(terms, count - 1);
    return this.distinct;
  }
}
