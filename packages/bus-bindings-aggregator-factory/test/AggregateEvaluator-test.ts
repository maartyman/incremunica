import type { IExpressionEvaluator } from '@comunica/types';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { KeysBindings } from '@incremunica/context-entries';
import {
  getMockEEActionContext,
  getMockEEFactory,
  int,
  makeAggregate,
} from '@incremunica/dev-tools';
import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import { AggregateEvaluator } from '../lib';

const DF = new DataFactory();
const BF = new BindingsFactory(DF);

class EmptyEvaluator extends AggregateEvaluator {
  public constructor(evaluator: IExpressionEvaluator, distinct: boolean, throwError = false) {
    super(evaluator, distinct, throwError);
  }

  protected putTerm(_: RDF.Term): void {
    // Empty
  }

  protected removeTerm(_: RDF.Term): void {
    // Empty
  }

  protected termResult(): RDF.Term | undefined {
    return undefined;
  }
}

describe('aggregate evaluator', () => {
  it('handles errors using evaluations 1', async() => {
    const temp = await getMockEEFactory().run({
      algExpr: makeAggregate('sum').expression,
      context: getMockEEActionContext(),
    }, undefined);
    let first = true;
    temp.evaluate = async() => {
      if (first) {
        first = false;
        throw new Error('We only want the first to succeed');
      }
      return int('1');
    };
    const evaluator: AggregateEvaluator = new EmptyEvaluator(temp, false, false);
    await Promise.all([
      evaluator.putBindings(BF.bindings().setContextEntry(KeysBindings.isAddition, true)),
      evaluator.putBindings(BF.bindings().setContextEntry(KeysBindings.isAddition, true)),
      evaluator.putBindings(BF.bindings().setContextEntry(KeysBindings.isAddition, true)),
    ]);
    expect(evaluator.result()).toBeNull();
    expect(evaluator.result()).toBeUndefined();
    await evaluator.putBindings(BF.bindings().setContextEntry(KeysBindings.isAddition, true));
    expect(evaluator.result()).toBeUndefined();
  });

  it('handles errors using evaluations 2', async() => {
    const temp = await getMockEEFactory().run({
      algExpr: makeAggregate('sum').expression,
      context: getMockEEActionContext(),
    }, undefined);
    let first = true;
    temp.evaluate = async() => {
      if (first) {
        first = false;
        throw new Error('We only want the first to succeed');
      }
      return int('1');
    };
    const evaluator: AggregateEvaluator = new EmptyEvaluator(temp, false, false);
    await Promise.all([
      evaluator.putBindings(BF.bindings().setContextEntry(KeysBindings.isAddition, true)),
      evaluator.putBindings(BF.bindings().setContextEntry(KeysBindings.isAddition, true)),
      evaluator.putBindings(BF.bindings().setContextEntry(KeysBindings.isAddition, false)),
      evaluator.putBindings(BF.bindings().setContextEntry(KeysBindings.isAddition, false)),
    ]);
    expect(evaluator.result()).toBeNull();
  });

  it('returns undefined if result hasn\'t changed', async() => {
    const temp = await getMockEEFactory().run({
      algExpr: makeAggregate('sum').expression,
      context: getMockEEActionContext(),
    }, undefined);
    const evaluator: AggregateEvaluator = new EmptyEvaluator(temp, false, false);
    (<any>evaluator).termResult = () => int('1');
    expect(evaluator.result()).toEqual(int('1'));
    expect(evaluator.result()).toBeUndefined();
  });

  it('returns undefined if result was undefined twice', async() => {
    const temp = await getMockEEFactory().run({
      algExpr: makeAggregate('sum').expression,
      context: getMockEEActionContext(),
    }, undefined);
    const evaluator: AggregateEvaluator = new EmptyEvaluator(temp, false, false);
    (<any>evaluator).termResult = () => undefined;
    expect(evaluator.result()).toBeUndefined();
    expect(evaluator.result()).toBeUndefined();
  });
});
