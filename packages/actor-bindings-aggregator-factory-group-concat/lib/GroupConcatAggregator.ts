import type { ComunicaDataFactory, IExpressionEvaluator } from '@comunica/types';
import * as Eval from '@comunica/utils-expression-evaluator';
import type { IBindingsAggregator } from '@incremunica/bus-bindings-aggregator-factory';
import { AggregateEvaluator } from '@incremunica/bus-bindings-aggregator-factory';
import type * as RDF from '@rdfjs/types';
import { termToString } from 'rdf-string';

interface IGroupConcatStateValue {
  term: RDF.Term;
  count: number;
}

export class GroupConcatAggregator extends AggregateEvaluator implements IBindingsAggregator {
  private state: Map<string, IGroupConcatStateValue> | undefined = undefined;
  private readonly separator: string;

  public constructor(
    evaluator: IExpressionEvaluator,
    distinct: boolean,
    private readonly dataFactory: ComunicaDataFactory,
    separator?: string,
    throwError?: boolean,
  ) {
    super(evaluator, distinct, throwError);
    this.separator = separator ?? ' ';
  }

  public override emptyValueTerm(): RDF.Term {
    return Eval.typedLiteral('', Eval.TypeURL.XSD_STRING);
  }

  protected putTerm(term: RDF.Term): void {
    const hash = termToString(term);
    if (this.state === undefined) {
      this.state = new Map<string, IGroupConcatStateValue>([[ hash, { term, count: 1 }]]);
      return;
    }
    const stateValue = this.state.get(hash);
    if (stateValue === undefined) {
      this.state.set(hash, { term, count: 1 });
    } else {
      stateValue.count++;
    }
  }

  protected removeTerm(term: RDF.Term): void {
    const hash = termToString(term);
    if (this.state === undefined) {
      throw new Error(`Cannot remove term ${termToString(term)} from empty concat aggregator`);
    }
    const stateValue = this.state.get(hash);
    if (stateValue === undefined) {
      throw new Error(`Cannot remove term ${termToString(term)} that was not added to concat aggregator`);
    }
    if (stateValue.count === 1) {
      this.state.delete(hash);
      if (this.state.size === 0) {
        this.state = undefined;
      }
      return;
    }
    stateValue.count--;
  }

  protected termResult(): RDF.Term | undefined {
    if (this.state === undefined) {
      return this.emptyValue();
    }
    let resultString: string;
    if (this.distinct) {
      resultString = [ ...this.state.values() ].map(stateValue => stateValue.term.value).join(this.separator);
    } else {
      resultString = '';
      for (const stateValue of this.state.values()) {
        for (let i = 0; i < stateValue.count; i++) {
          if (resultString.length > 0) {
            resultString += this.separator;
          }
          resultString += stateValue.term.value;
        }
      }
    }
    let language = '';
    for (const stateValue of this.state.values()) {
      if (stateValue.term.termType !== 'Literal') {
        return Eval.typedLiteral(resultString, Eval.TypeURL.XSD_STRING);
      }
      if (!language) {
        language = stateValue.term.language;
        continue;
      }
      if (language !== stateValue.term.language) {
        return Eval.typedLiteral(resultString, Eval.TypeURL.XSD_STRING);
      }
    }
    if (!language) {
      return Eval.typedLiteral(resultString, Eval.TypeURL.XSD_STRING);
    }
    return Eval.langString(resultString, language).toRDF(this.dataFactory);
  }
}
