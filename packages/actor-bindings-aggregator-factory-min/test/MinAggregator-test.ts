import { ActorFunctionFactoryTermEquality } from '@comunica/actor-function-factory-term-equality';
import { ActorFunctionFactoryTermLesserThan } from '@comunica/actor-function-factory-term-lesser-than';
import {
  ActorTermComparatorFactoryExpressionEvaluator,
} from '@comunica/actor-term-comparator-factory-expression-evaluator';
import type { ActorExpressionEvaluatorFactory } from '@comunica/bus-expression-evaluator-factory';
import type { MediatorTermComparatorFactory } from '@comunica/bus-term-comparator-factory';
import { Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import type { IBindingsAggregator } from '@incremunica/bus-bindings-aggregator-factory';
import { KeysBindings } from '@incremunica/context-entries';
import {
  BF,
  createFuncMediator,
  date,
  DF,
  double,
  float,
  getMockEEActionContext,
  getMockEEFactory,
  getMockMediatorMergeBindingsContext,
  getMockMediatorQueryOperation,
  int,
  makeAggregate,
  nonLiteral,
  string,
} from '@incremunica/dev-tools';
import type * as RDF from '@rdfjs/types';
import { MinAggregator } from '../lib/MinAggregator';

async function runAggregator(aggregator: IBindingsAggregator, input: Bindings[]): Promise<RDF.Term | undefined> {
  for (const bindings of input) {
    await aggregator.putBindings(bindings);
  }
  return aggregator.result();
}

async function createAggregator({
  expressionEvaluatorFactory,
  context,
  distinct,
  mediatorTermComparatorFactory,
}: {
  expressionEvaluatorFactory: ActorExpressionEvaluatorFactory;
  mediatorTermComparatorFactory: MediatorTermComparatorFactory;
  context: IActionContext;
  distinct: boolean;
}): Promise<MinAggregator> {
  return new MinAggregator(
    await expressionEvaluatorFactory.run({
      algExpr: makeAggregate('min', distinct).expression,
      context,
    }, undefined),
    distinct,
    await mediatorTermComparatorFactory.mediate({ context }),
    true,
  );
}
describe('MinAggregator', () => {
  let expressionEvaluatorFactory: ActorExpressionEvaluatorFactory;
  let mediatorTermComparatorFactory: MediatorTermComparatorFactory;
  let context: IActionContext;

  beforeEach(() => {
    expressionEvaluatorFactory = getMockEEFactory();
    // TODO [2025-02-01]: This can be replaced with createTermCompMediator in comunica
    mediatorTermComparatorFactory = <MediatorTermComparatorFactory> {
      async mediate(action) {
        return await new ActorTermComparatorFactoryExpressionEvaluator({
          name: 'actor',
          bus: new Bus({ name: 'bus' }),
          mediatorFunctionFactory: createFuncMediator([
            args => new ActorFunctionFactoryTermEquality(args),
            args => new ActorFunctionFactoryTermLesserThan(args),
          ], {}),
          mediatorQueryOperation: getMockMediatorQueryOperation(),
          mediatorMergeBindingsContext: getMockMediatorMergeBindingsContext(),
        }).run(action);
      },
    };

    context = getMockEEActionContext();
  });

  describe('non distinctive min', () => {
    let aggregator: IBindingsAggregator;

    beforeEach(async() => {
      aggregator = await createAggregator({
        expressionEvaluatorFactory,
        mediatorTermComparatorFactory,
        context,
        distinct: false,
      });
    });

    it('a list of bindings 1', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('3') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('4') ]]).setContextEntry(KeysBindings.isAddition, true),
      ];

      await expect(runAggregator(aggregator, input)).resolves.toEqual(int('1'));
    });

    it('a list of bindings 2', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('3') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('4') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([[ DF.variable('x'), int('4') ]]).setContextEntry(KeysBindings.isAddition, false),
      ];

      await expect(runAggregator(aggregator, input)).resolves.toEqual(int('2'));
    });

    it('a list of string bindings', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), string('11') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), string('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), string('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), string('3') ]]).setContextEntry(KeysBindings.isAddition, true),
      ];

      await expect(runAggregator(aggregator, input)).resolves.toEqual(string('1'));
    });

    it('a list of date bindings', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), date('2010-06-21Z') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), date('2010-06-21-08:00') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), date('2001-07-23') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), date('2010-06-21+09:00') ]]).setContextEntry(KeysBindings.isAddition, true),
      ];

      await expect(runAggregator(aggregator, input)).resolves.toEqual(date('2001-07-23'));
    });

    it('should work with different types', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), double('11.0') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), float('3') ]]).setContextEntry(KeysBindings.isAddition, true),
      ];

      await expect(runAggregator(aggregator, input)).resolves.toEqual(int('2'));
    });

    it('passing a non-literal should not be accepted', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), nonLiteral() ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('3') ]]).setContextEntry(KeysBindings.isAddition, true),
      ];
      await expect(runAggregator(aggregator, input)).rejects.toThrow(
        new Error(`Term with value ${nonLiteral().value} has type ${nonLiteral().termType} and is not a literal`),
      );
    });

    it('passing a non-literal should not be accepted even in non-first place', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), nonLiteral() ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('3') ]]).setContextEntry(KeysBindings.isAddition, true),
      ];
      await expect(runAggregator(aggregator, input)).rejects.toThrow(
        new Error(`Term with value ${nonLiteral().value} has type ${nonLiteral().termType} and is not a literal`),
      );
    });

    it('with respect to empty input', async() => {
      await expect(runAggregator(aggregator, [])).rejects.toThrow(
        new Error(`Empty aggregate expression`),
      );
    });

    it('should error on a deletion if aggregator empty', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, false),
      ];

      await expect(runAggregator(aggregator, input)).rejects.toThrow(
        new Error('Cannot remove term "2"^^http://www.w3.org/2001/XMLSchema#integer from empty min aggregator'),
      );
    });

    it('should error on a deletion that has not been added', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('3') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, false),
      ];

      await expect(runAggregator(aggregator, input)).rejects.toThrow(
        new Error('Cannot remove term "2"^^http://www.w3.org/2001/XMLSchema#integer that was not added to min aggregator'),
      );
    });

    it('delete everything', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, false),
      ];

      await expect(runAggregator(aggregator, input)).rejects.toThrow(
        new Error(`Empty aggregate expression`),
      );
    });
  });

  describe('distinctive Min', () => {
    let aggregator: IBindingsAggregator;

    beforeEach(async() => {
      aggregator = await createAggregator({
        expressionEvaluatorFactory,
        mediatorTermComparatorFactory,
        context,
        distinct: true,
      });
    });

    it('a list of bindings 1', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('x'), int('1') ],
          [ DF.variable('y'), int('1') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
      ];

      await expect(runAggregator(aggregator, input)).resolves.toEqual(int('1'));
    });

    it('a list of bindings 2', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('x'), int('1') ],
          [ DF.variable('y'), int('1') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('x'), int('1') ],
          [ DF.variable('y'), int('1') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
      ];

      await expect(runAggregator(aggregator, input)).resolves.toEqual(int('1'));
    });

    it('a list of bindings 3', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([
          [ DF.variable('x'), int('1') ],
          [ DF.variable('y'), int('1') ],
        ]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([
          [ DF.variable('x'), int('1') ],
          [ DF.variable('y'), int('1') ],
        ]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, false),
      ];

      await expect(runAggregator(aggregator, input)).resolves.toEqual(int('2'));
    });

    it('with respect to empty input', async() => {
      await expect(runAggregator(aggregator, [])).rejects.toThrow(
        new Error(`Empty aggregate expression`),
      );
    });
  });
});
