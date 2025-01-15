import { ActorFunctionFactoryTermAddition } from '@comunica/actor-function-factory-term-addition';
import { ActorFunctionFactoryTermDivision } from '@comunica/actor-function-factory-term-division';
import { ActorFunctionFactoryTermSubtraction } from '@comunica/actor-function-factory-term-subtraction';
import type { ActorExpressionEvaluatorFactory } from '@comunica/bus-expression-evaluator-factory';
import type { MediatorFunctionFactory } from '@comunica/bus-function-factory';
import { KeysInitQuery } from '@comunica/context-entries';
import type { IActionContext } from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import { SparqlOperator } from '@comunica/utils-expression-evaluator';
import type { AggregateEvaluator, IBindingsAggregator } from '@incremunica/bus-bindings-aggregator-factory';
import { KeysBindings } from '@incremunica/context-entries';
import {
  createFuncMediator,
  BF,
  decimal,
  DF,
  double,
  float,
  getMockEEActionContext,
  getMockEEFactory,
  int,
  makeAggregate,
} from '@incremunica/dev-tools';
import type * as RDF from '@rdfjs/types';
import { AverageAggregator } from '../lib';

async function runAggregator(aggregator: IBindingsAggregator, input: Bindings[]): Promise<RDF.Term | undefined> {
  for (const bindings of input) {
    await aggregator.putBindings(bindings);
  }
  return aggregator.result();
}

async function createAggregator({ expressionEvaluatorFactory, context, distinct, mediatorFunctionFactory }: {
  expressionEvaluatorFactory: ActorExpressionEvaluatorFactory;
  context: IActionContext;
  distinct: boolean;
  mediatorFunctionFactory: MediatorFunctionFactory;
}): Promise<AverageAggregator> {
  return new AverageAggregator(
    await expressionEvaluatorFactory.run({
      algExpr: makeAggregate('avg', distinct).expression,
      context,
    }, undefined),
    distinct,
    context.getSafe(KeysInitQuery.dataFactory),
    await mediatorFunctionFactory.mediate({
      context,
      functionName: SparqlOperator.ADDITION,
      requireTermExpression: true,
    }),
    await mediatorFunctionFactory.mediate({
      context,
      functionName: SparqlOperator.SUBTRACTION,
      requireTermExpression: true,
    }),
    await mediatorFunctionFactory.mediate({
      context,
      functionName: SparqlOperator.DIVISION,
      requireTermExpression: true,
    }),
    true,
  );
}

describe('AverageAggregator', () => {
  let expressionEvaluatorFactory: ActorExpressionEvaluatorFactory;
  let mediatorFunctionFactory: MediatorFunctionFactory;
  let context: IActionContext;

  beforeEach(() => {
    mediatorFunctionFactory = createFuncMediator([
      args => new ActorFunctionFactoryTermAddition(args),
      args => new ActorFunctionFactoryTermDivision(args),
      args => new ActorFunctionFactoryTermSubtraction(args),
    ], {});
    expressionEvaluatorFactory = getMockEEFactory({
      mediatorFunctionFactory,
    });

    context = getMockEEActionContext();
  });

  describe('non distinctive avg', () => {
    let aggregator: IBindingsAggregator;

    beforeEach(async() => {
      aggregator = await createAggregator({
        mediatorFunctionFactory,
        expressionEvaluatorFactory,
        context,
        distinct: false,
      });
    });

    it('a list of bindings 1', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), float('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('3') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('4') ]]).setContextEntry(KeysBindings.isAddition, true),
      ];

      await expect(runAggregator(aggregator, input)).resolves.toEqual(float('2.5'));
    });

    it('a list of bindings 2', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), float('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('3') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('4') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('4') ]]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, false),
      ];

      await expect(runAggregator(aggregator, input)).resolves.toEqual(float('2'));
    });

    it('with respect to empty input', async() => {
      await expect(runAggregator(aggregator, [])).resolves.toEqual(int('0'));
    });

    it('should error on a deletion if aggregator empty', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, false),
      ];

      await expect(runAggregator(aggregator, input)).rejects.toThrow(
        new Error('Cannot remove term "2"^^http://www.w3.org/2001/XMLSchema#integer from empty average aggregator'),
      );
    });

    it('should error on a deletion that has not been added', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('3') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, false),
      ];

      await expect(runAggregator(aggregator, input)).rejects.toThrow(
        new Error('Cannot remove term "2"^^http://www.w3.org/2001/XMLSchema#integer that was not added to average aggregator'),
      );
    });

    it('delete everything', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, false),
      ];

      await expect(runAggregator(aggregator, input)).resolves
        .toEqual((<AggregateEvaluator>aggregator).emptyValueTerm());
    });

    it('with respect to type promotion and subtype substitution 1', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#byte')) ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), float('3') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), DF.literal('4', DF.namedNode('http://www.w3.org/2001/XMLSchema#nonNegativeInteger')) ]]).setContextEntry(KeysBindings.isAddition, true),
      ];
      await expect(runAggregator(aggregator, input)).resolves.toEqual(float('2.5'));
    });

    it('with respect to type promotion and subtype substitution 2', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#byte')) ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), float('3') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), DF.literal('4', DF.namedNode('http://www.w3.org/2001/XMLSchema#nonNegativeInteger')) ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#byte')) ]]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([[ DF.variable('x'), float('3') ]]).setContextEntry(KeysBindings.isAddition, false),
      ];
      await expect(runAggregator(aggregator, input)).resolves.toEqual(float('3'));
    });

    it('with respect to type preservation 1', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('3') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('4') ]]).setContextEntry(KeysBindings.isAddition, true),
      ];
      await expect(runAggregator(aggregator, input)).resolves.toEqual(decimal('2.5'));
    });

    it('with respect to type preservation 2', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('3') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('4') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2') ]]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, false),
      ];
      await expect(runAggregator(aggregator, input)).resolves.toEqual(decimal('3.5'));
    });

    it('with respect to type promotion 1', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), double('1000') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2000') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), float('3000') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), double('4000') ]]).setContextEntry(KeysBindings.isAddition, true),
      ];
      await expect(runAggregator(aggregator, input)).resolves.toEqual(double('2.5E3'));
    });

    it('with respect to type promotion 2', async() => {
      const input = [
        BF.bindings([[ DF.variable('x'), double('1000') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), int('2000') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), float('3000') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), double('4000') ]]).setContextEntry(KeysBindings.isAddition, true),
        BF.bindings([[ DF.variable('x'), float('3000') ]]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([[ DF.variable('x'), double('1000') ]]).setContextEntry(KeysBindings.isAddition, false),
      ];
      await expect(runAggregator(aggregator, input)).resolves.toEqual(double('3.0E3'));
    });
  });

  describe('distinctive avg', () => {
    let aggregator: IBindingsAggregator;

    beforeEach(async() => {
      aggregator = await createAggregator({
        mediatorFunctionFactory,
        expressionEvaluatorFactory,
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

      await expect(runAggregator(aggregator, input)).resolves.toEqual(decimal('1.25'));
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
        BF.bindings([[ DF.variable('x'), int('1') ]]).setContextEntry(KeysBindings.isAddition, false),
      ];

      await expect(runAggregator(aggregator, input)).resolves.toEqual(decimal('1.5'));
    });

    it('with respect to empty input', async() => {
      await expect(runAggregator(aggregator, [])).resolves.toEqual(int('0'));
    });
  });
});
