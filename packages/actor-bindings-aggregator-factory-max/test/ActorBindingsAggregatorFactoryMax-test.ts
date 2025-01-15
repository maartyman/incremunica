import { ActorFunctionFactoryTermEquality } from '@comunica/actor-function-factory-term-equality';
import { ActorFunctionFactoryTermLesserThan } from '@comunica/actor-function-factory-term-lesser-than';
import {
  ActorTermComparatorFactoryExpressionEvaluator,
} from '@comunica/actor-term-comparator-factory-expression-evaluator';
import type { MediatorExpressionEvaluatorFactory } from '@comunica/bus-expression-evaluator-factory';
import type { MediatorTermComparatorFactory } from '@comunica/bus-term-comparator-factory';
import { Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import {
  BF,
  createFuncMediator,
  DF,
  getMockEEActionContext,
  getMockMediatorExpressionEvaluatorFactory,
  getMockMediatorMergeBindingsContext,
  getMockMediatorQueryOperation,
  makeAggregate,
} from '@incremunica/dev-tools';
import { ArrayIterator } from 'asynciterator';
import { ActorBindingsAggregatorFactoryMax } from '../lib';
import '@comunica/utils-jest';

describe('ActorBindingsAggregatorFactoryMax', () => {
  let bus: any;
  let mediatorExpressionEvaluatorFactory: MediatorExpressionEvaluatorFactory;
  let mediatorTermComparatorFactory: MediatorTermComparatorFactory;
  let context: IActionContext;
  const exception = 'This actor only supports the \'max\' aggregator.';

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });

    const mediatorQueryOperation: any = {
      mediate: (arg: any) => Promise.resolve({
        bindingsStream: new ArrayIterator([
          BF.bindings([[ DF.variable('x'), DF.literal('1') ]]),
          BF.bindings([[ DF.variable('x'), DF.literal('2') ]]),
          BF.bindings([[ DF.variable('x'), DF.literal('3') ]]),
        ], { autoStart: false }),
        metadata: () => Promise.resolve({ cardinality: 3, canContainUndefs: false, variables: [ DF.variable('x') ]}),
        operated: arg,
        type: 'bindings',
      }),
    };

    mediatorExpressionEvaluatorFactory = getMockMediatorExpressionEvaluatorFactory({
      mediatorQueryOperation,
    });
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

  describe('An ActorBindingsAggregatorFactoryMax instance', () => {
    let actor: ActorBindingsAggregatorFactoryMax;

    beforeEach(() => {
      actor = new ActorBindingsAggregatorFactoryMax({
        name: 'actor',
        bus,
        mediatorExpressionEvaluatorFactory,
        mediatorTermComparatorFactory,
      });
    });

    describe('test', () => {
      it('accepts max 1', async() => {
        await expect(actor.test({
          context,
          expr: makeAggregate('max', false),
        })).resolves.toPassTestVoid();
      });

      it('accepts max 2', async() => {
        await expect(actor.test({
          context,
          expr: makeAggregate('max', true),
        })).resolves.toPassTestVoid();
      });

      it('rejects sum', async() => {
        await expect(actor.test({
          context,
          expr: makeAggregate('sum', false),
        })).resolves.toFailTest(exception);
      });
    });

    it('should run', async() => {
      await expect(actor.run({
        context,
        expr: makeAggregate('max', false),
      })).resolves.toMatchObject({});
    });
  });
});
