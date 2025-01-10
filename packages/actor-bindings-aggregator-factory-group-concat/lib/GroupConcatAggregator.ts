import type { ComunicaDataFactory, IExpressionEvaluator } from '@comunica/types';
import * as Eval from '@comunica/utils-expression-evaluator';
import type { IBindingsAggregator } from '@incremunica/bus-bindings-aggregator-factory';
import { AggregateEvaluator } from '@incremunica/bus-bindings-aggregator-factory';
import type * as RDF from '@rdfjs/types';
import { termToString } from 'rdf-string';

export class GroupConcatAggregator extends AggregateEvaluator implements IBindingsAggregator {
  private state: Map<string, number> | Set<string> | undefined = undefined;
  private lastLanguageValid = true;
  private lastLanguage: string | undefined = undefined;
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

  public putTerm(term: RDF.Term): void {
    if (this.state === undefined) {
      if (this.distinct) {
        this.state = new Set<string>([ term.value ]);
      } else {
        this.state = new Map<string, number>([[ term.value, 1 ]]);
      }
      if (term.termType === 'Literal') {
        this.lastLanguage = term.language;
      }
    } else {
      if (this.distinct) {
        (<Set<string>> this.state).add(term.value);
      } else {
        (<Map<string, number>> this.state)
          .set(term.value, ((<Map<string, number>> this.state).get(term.value) ?? 0) + 1);
      }
      if (this.lastLanguageValid && term.termType === 'Literal' && this.lastLanguage !== term.language) {
        this.lastLanguageValid = false;
        this.lastLanguage = undefined;
      }
    }
  }

  protected removeTerm(term: RDF.Term): void {
    if (this.state === undefined) {
      throw new Error(`Cannot remove term ${termToString(term)} from empty concat aggregator`);
    }
    if (this.distinct) {
      const count = (<Map<string, number>> this.state).get(term.value);
      if (count === undefined) {
        throw new Error(`Cannot remove term ${termToString(term)} that was not added to concat aggregator`);
      }
      if (count === 1) {
        this.state.delete(term.value);
      } else {
        (<Map<string, number>> this.state).set(term.value, count - 1);
      }
    } else if (!this.state.delete(term.value)) {
      throw new Error(`Cannot remove term ${termToString(term)} that was not added to concat aggregator`);
    }
  }

  public termResult(): RDF.Term | undefined {
    if (this.state === undefined) {
      return this.emptyValue();
    }
    let resultString: string;
    if (this.distinct) {
      resultString = [ ...(<Map<string, number>> this.state)
        .entries() ].map(([ value, count ]) => value.repeat(count)).join(this.separator);
    } else {
      resultString = [ ...this.state.keys() ].join(this.separator);
    }
    if (this.lastLanguageValid && this.lastLanguage) {
      return Eval.langString(resultString, this.lastLanguage).toRDF(this.dataFactory);
    }
    return Eval.typedLiteral(resultString, Eval.TypeURL.XSD_STRING);
  }
}
