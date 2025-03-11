import { ActorFunctionFactoryTermStrLen } from '@comunica/actor-function-factory-term-str-len';
import type { MediatorExpressionEvaluatorFactory } from '@comunica/bus-expression-evaluator-factory';
import { ActorQueryOperation } from '@comunica/bus-query-operation';
import type { MediatorTermComparatorFactory } from '@comunica/bus-term-comparator-factory';
import { Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import * as sparqlee from '@comunica/utils-expression-evaluator';
import { getSafeBindings } from '@comunica/utils-query-operation';
import { KeysBindings } from '@incremunica/context-entries';
import {
  DF,
  BF,
  createFuncMediator,
  getMockMediatorExpressionEvaluatorFactory,
  createTermCompMediator,
  getMockEEActionContext,
  partialArrayifyAsyncIterator,
} from '@incremunica/dev-tools';
import { arrayifyStream } from 'arrayify-stream';
import { ArrayIterator, AsyncIterator } from 'asynciterator';
import { Algebra } from 'sparqlalgebrajs';
import { ActorQueryOperationOrderBy } from '../lib';
import '@comunica/utils-jest';
import '@incremunica/jest';

function checkOrder(array: Bindings[], expected: Bindings[]) {
  expect(array).toBeIsomorphicBindingsArray(expected);
  for (let i = 0; i < expected.length; i++) {
    const index = array[i].getContextEntry(KeysBindings.order)!.index;
    expect(array[i]).toEqualBindings(expected[index]);
  }
}

const mediatorFunctionFactory = createFuncMediator([
  args => new ActorFunctionFactoryTermStrLen(args),
], {});

describe('ActorQueryOperationOrderBy', () => {
  describe('ActorQueryOperationOrderBy with mixed term types', () => {
    let bus: any;
    let mediatorQueryOperation: any;
    let mediatorExpressionEvaluatorFactory: MediatorExpressionEvaluatorFactory;
    let mediatorTermComparatorFactory: MediatorTermComparatorFactory;
    let context: IActionContext;

    beforeEach(() => {
      bus = new Bus({ name: 'bus' });
      mediatorQueryOperation = {
        mediate: (arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('http://example.com/a') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('http://example.com/b') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.blankNode('a') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.blankNode('b') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('11') ],
            ]),
            BF.bindings([]),
          ]),
          metadata: () => Promise.resolve({
            cardinality: 6,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
            ],
          }),
          operated: arg,
          type: 'bindings',
        }),
      };
      mediatorExpressionEvaluatorFactory = getMockMediatorExpressionEvaluatorFactory({
        mediatorFunctionFactory: createFuncMediator([], {}),
      });
      mediatorTermComparatorFactory = createTermCompMediator();
      context = getMockEEActionContext();
    });

    describe('An ActorQueryOperationOrderBy instance', () => {
      let actor: ActorQueryOperationOrderBy;
      let orderA: Algebra.TermExpression;
      let descOrderA: Algebra.OperatorExpression;

      beforeEach(() => {
        actor = new ActorQueryOperationOrderBy({
          name: 'actor',
          bus,
          mediatorQueryOperation,
          mediatorExpressionEvaluatorFactory,
          mediatorTermComparatorFactory,
        });
        orderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.TERM,
          term: DF.variable('a'),
        };
        descOrderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.OPERATOR,
          operator: 'desc',
          args: [ orderA ],
        };
      });

      it('should sort as an ascending undefined < blank node < named node < literal', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        const expected = [
          BF.bindings([]),
          BF.bindings([
            [ DF.variable('a'), DF.blankNode('a') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.blankNode('b') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.namedNode('http://example.com/a') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.namedNode('http://example.com/b') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('11') ],
          ]),
        ];
        checkOrder(array, expected);
      });

      it('should sort as an descending undefined < blank node < named node < literal', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ descOrderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        const expected = [
          BF.bindings([
            [ DF.variable('a'), DF.literal('11') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.namedNode('http://example.com/b') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.namedNode('http://example.com/a') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.blankNode('b') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.blankNode('a') ],
          ]),
          BF.bindings([]),
        ];
        checkOrder(array, expected);
      });
    });
  });

  describe('ActorQueryOperationOrderBySparqlee', () => {
    let bus: any;
    let mediatorQueryOperation: any;
    let mediatorExpressionEvaluatorFactory: MediatorExpressionEvaluatorFactory;
    let mediatorTermComparatorFactory: MediatorTermComparatorFactory;
    let context: IActionContext;

    beforeEach(() => {
      bus = new Bus({ name: 'bus' });
      mediatorQueryOperation = {
        mediate: (arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([[ DF.variable('a'), DF.literal('22') ]]),
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
            BF.bindings([[ DF.variable('a'), DF.literal('333') ]]),
          ]),
          metadata: () => Promise.resolve({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
            ],
          }),
          operated: arg,
          type: 'bindings',
        }),
      };

      mediatorExpressionEvaluatorFactory = getMockMediatorExpressionEvaluatorFactory({
        mediatorFunctionFactory,
      });
      mediatorTermComparatorFactory = createTermCompMediator();
      context = getMockEEActionContext();
    });

    describe('The ActorQueryOperationOrderBy module', () => {
      it('should be a function', () => {
        expect(ActorQueryOperationOrderBy).toBeInstanceOf(Function);
      });

      it('should be a ActorQueryOperationOrderBy constructor', () => {
        expect(new (<any>ActorQueryOperationOrderBy)({ name: 'actor', bus, mediatorQueryOperation }))
          .toBeInstanceOf(<any>ActorQueryOperationOrderBy);
        expect(new ActorQueryOperationOrderBy({
          name: 'actor',
          bus,
          mediatorQueryOperation,
          mediatorExpressionEvaluatorFactory,
          mediatorTermComparatorFactory,
        }))
          .toBeInstanceOf(ActorQueryOperation);
      });

      it('should not be able to create new ActorQueryOperationOrderBy objects without \'new\'', () => {
        expect(() => {
          (<any>ActorQueryOperationOrderBy)();
        }).toThrow(`Class constructor ActorQueryOperationOrderBy cannot be invoked without 'new'`);
      });
    });

    describe('An ActorQueryOperationOrderBy instance', () => {
      let actor: ActorQueryOperationOrderBy;
      let orderA: Algebra.TermExpression;
      let orderB: Algebra.TermExpression;
      let descOrderA: Algebra.OperatorExpression;
      let orderA1: Algebra.OperatorExpression;

      beforeEach(() => {
        actor = new ActorQueryOperationOrderBy({
          name: 'actor',
          bus,
          mediatorQueryOperation,
          mediatorExpressionEvaluatorFactory,
          mediatorTermComparatorFactory,
        });
        orderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.TERM,
          term: DF.variable('a'),
        };
        orderB = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.TERM,
          term: DF.variable('b'),
        };
        descOrderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.OPERATOR,
          operator: 'desc',
          args: [ orderA ],
        };
        orderA1 = {
          args: [ orderA ],
          expressionType: Algebra.expressionTypes.OPERATOR,
          operator: 'strlen',
          type: Algebra.types.EXPRESSION,
        };
      });

      it('should test on orderby', async() => {
        const op: any = { operation: { type: 'orderby', expressions: []}, context };
        await expect(actor.test(op)).resolves.toPassTestVoid();
      });

      it('should test on a descending orderby', async() => {
        const op: any = { operation: { type: 'orderby', expressions: [ descOrderA ]}, context };
        await expect(actor.test(op)).resolves.toPassTestVoid();
      });

      it('should test on multiple expressions', async() => {
        const op: any = {
          operation: { type: 'orderby', expressions: [ orderA, descOrderA, orderA1 ]},
          context,
        };
        await expect(actor.test(op)).resolves.toPassTestVoid();
      });

      it('should not test on non-orderby', async() => {
        const op: any = { operation: { type: 'some-other-type' }, context };
        await expect(actor.test(op)).resolves.toFailTest(`Actor actor only supports orderby operations, but got some-other-type`);
      });

      it('should test but not run on unsupported operators', async() => {
        const op: any = {
          operation: {
            type: 'orderby',
            expressions: [{
              args: [],
              expressionType: 'operator',
              operator: 'DUMMY',
            }],
          },
          context,
        };
        await expect(actor.test(op)).resolves.toPassTestVoid();
        await expect(actor.run(op, undefined)).rejects.toThrow(
          `No actors are able to reply to a message`,
        );
      });

      it('should run', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        await expect(getSafeBindings(output).metadata()).resolves
          .toEqual({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
            ],
          });
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(
          array,
          [
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
            BF.bindings([[ DF.variable('a'), DF.literal('22') ]]),
            BF.bindings([[ DF.variable('a'), DF.literal('333') ]]),
          ],
        );
      });

      it('should run operator expressions', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA1 ]},
          context,
        };
        const output = await actor.run(op, undefined);
        await expect(getSafeBindings(output).metadata()).resolves
          .toEqual({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
            ],
          });
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(
          array,
          [
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
            BF.bindings([[ DF.variable('a'), DF.literal('22') ]]),
            BF.bindings([[ DF.variable('a'), DF.literal('333') ]]),
          ],
        );
      });

      it('should run descend', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ descOrderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        await expect(getSafeBindings(output).metadata()).resolves
          .toEqual({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
            ],
          });
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(
          array,
          [
            BF.bindings([[ DF.variable('a'), DF.literal('333') ]]),
            BF.bindings([[ DF.variable('a'), DF.literal('22') ]]),
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
          ],
        );
      });

      it('should ignore undefined results', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderB ]},
          context,
        };
        const output = await actor.run(op, undefined);
        await expect(getSafeBindings(output).metadata()).resolves
          .toEqual({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
            ],
          });
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(
          array,
          [
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
            BF.bindings([[ DF.variable('a'), DF.literal('22') ]]),
            BF.bindings([[ DF.variable('a'), DF.literal('333') ]]),
          ],
        );
      });

      it('should emit an error on a hard erroring expression', async() => {
        // Mock the expression error test so we can force 'a programming error' and test the branch

        Object.defineProperty(sparqlee, 'isExpressionError', { writable: true });
        // eslint-disable-next-line jest/prefer-spy-on
        (<any>sparqlee).isExpressionError = jest.fn(() => false);
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderB ]},
          context,
        };
        const output = <any> await actor.run(op, undefined);
        await expect(new Promise<void>((resolve, reject) => {
          output.bindingsStream.on('data', () => resolve());
          output.bindingsStream.on('error', (e: any) => reject(e));
        })).rejects.toThrow('Unbound variable \'"?b"\'');
      });
    });
  });

  describe('ActorQueryOperationOrderBy with multiple comparators', () => {
    let bus: any;
    let mediatorQueryOperation: any;
    let mediatorExpressionEvaluatorFactory: MediatorExpressionEvaluatorFactory;
    let mediatorTermComparatorFactory: MediatorTermComparatorFactory;
    let context: IActionContext;

    beforeEach(() => {
      bus = new Bus({ name: 'bus' });
      mediatorQueryOperation = {
        mediate: (arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('Vermeulen') ],
              [ DF.variable('b'), DF.literal('Jos') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('Bosmans') ],
              [ DF.variable('b'), DF.literal('Jos') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('Vermeulen') ],
              [ DF.variable('b'), DF.literal('Ben') ],
            ]),
          ]),
          metadata: () => Promise.resolve({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
              {
                variable: DF.variable('b'),
                canBeUndef: true,
              },
            ],
          }),
          operated: arg,
          type: 'bindings',
        }),
      };

      mediatorExpressionEvaluatorFactory = getMockMediatorExpressionEvaluatorFactory({
        mediatorFunctionFactory,
      });
      mediatorTermComparatorFactory = createTermCompMediator();
      context = getMockEEActionContext();
    });

    describe('An ActorQueryOperationOrderBy instance multiple comparators', () => {
      let actor: ActorQueryOperationOrderBy;
      let orderA: Algebra.TermExpression;
      let orderB: Algebra.TermExpression;
      let descOrderA: Algebra.OperatorExpression;
      let descOrderB: Algebra.OperatorExpression;
      let orderA1: Algebra.OperatorExpression;
      let orderB1: Algebra.OperatorExpression;

      beforeEach(() => {
        actor = new ActorQueryOperationOrderBy({
          name: 'actor',
          bus,
          mediatorQueryOperation,
          mediatorTermComparatorFactory,
          mediatorExpressionEvaluatorFactory,
        });
        orderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.TERM,
          term: DF.variable('a'),
        };
        orderB = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.TERM,
          term: DF.variable('b'),
        };
        descOrderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.OPERATOR,
          operator: 'desc',
          args: [ orderA ],
        };
        descOrderB = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.OPERATOR,
          operator: 'desc',
          args: [ orderB ],
        };
        orderA1 = {
          args: [ orderA ],
          expressionType: Algebra.expressionTypes.OPERATOR,
          operator: 'strlen',
          type: Algebra.types.EXPRESSION,
        };
        orderB1 = {
          args: [ orderB ],
          expressionType: Algebra.expressionTypes.OPERATOR,
          operator: 'strlen',
          type: Algebra.types.EXPRESSION,
        };
      });

      it('should order A', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array: Bindings[] = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('Bosmans') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Ben') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
        ]);
      });

      it('should order B', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderB ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Ben') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Bosmans') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
        ]);
      });

      it('should order priority B and secondary A, ascending', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderB, orderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Ben') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Bosmans') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
        ]);
      });

      it('descending order A multiple orderby', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ descOrderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Ben') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Bosmans') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
        ]);
      });

      it('descending order B multiple orderby', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ descOrderB ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('Bosmans') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Ben') ],
          ]),
        ]);
      });

      it('strlen orderby with multiple comparators', async() => {
        // Priority goes to orderB1 then we secondarily sort by orderA1
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderB1, orderA1 ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('Bosmans') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Ben') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
        ]);
      });

      it('should order priority B and secondary A, descending', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ descOrderB, descOrderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Bosmans') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Ben') ],
          ]),
        ]);
      });

      it('should order priority A, descending and secondary B, ascending', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ descOrderA, orderB ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Ben') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Bosmans') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
        ]);
      });

      it('should order priority A length and secondary B, ascending', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA1, orderB ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('Bosmans') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Ben') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
        ]);
      });

      it('should order priority A length and secondary B length', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA1, orderB1 ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('Bosmans') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Ben') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('Vermeulen') ],
            [ DF.variable('b'), DF.literal('Jos') ],
          ]),
        ]);
      });

      it('should handle empty bindings stream', async() => {
        mediatorQueryOperation.mediate = () => Promise.resolve({
          bindingsStream: new ArrayIterator([]),
          metadata: () => Promise.resolve({ cardinality: 0, variables: []}),
          operated: {},
          type: 'bindings',
        });
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        expect(array).toEqual([]);
      });

      it('should handle different data types', async() => {
        mediatorQueryOperation.mediate = () => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('2') ],
              [ DF.variable('b'), DF.literal('a') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('1') ],
              [ DF.variable('b'), DF.literal('b') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2') ],
              [ DF.variable('b'), DF.literal('c') ],
            ]),
          ]),
          metadata: () => Promise.resolve({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
              {
                variable: DF.variable('b'),
                canBeUndef: true,
              },
            ],
          }),
          operated: {},
          type: 'bindings',
        });
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA, orderB ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1') ],
            [ DF.variable('b'), DF.literal('b') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('a') ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2') ],
            [ DF.variable('b'), DF.literal('c') ],
          ]),
        ]);
      });
    });
  });

  describe('ActorQueryOperationOrderBy with integer type', () => {
    let bus: any;
    let mediatorQueryOperation: any;
    let mediatorExpressionEvaluatorFactory: MediatorExpressionEvaluatorFactory;
    let mediatorTermComparatorFactory: MediatorTermComparatorFactory;
    let context: IActionContext;

    beforeEach(() => {
      bus = new Bus({ name: 'bus' });
      mediatorQueryOperation = {
        mediate: (arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('11', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
            ]),
          ]),
          metadata: () => Promise.resolve({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
            ],
          }),
          operated: arg,
          type: 'bindings',
        }),
      };

      mediatorExpressionEvaluatorFactory = getMockMediatorExpressionEvaluatorFactory({
        mediatorFunctionFactory,
      });
      mediatorTermComparatorFactory = createTermCompMediator();
      context = getMockEEActionContext();
    });

    describe('An ActorQueryOperationOrderBy instance', () => {
      let actor: ActorQueryOperationOrderBy;
      let orderA: Algebra.TermExpression;
      let descOrderA: Algebra.OperatorExpression;

      beforeEach(() => {
        actor = new ActorQueryOperationOrderBy({
          name: 'actor',
          bus,
          mediatorQueryOperation,
          mediatorTermComparatorFactory,
          mediatorExpressionEvaluatorFactory,
        });
        orderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.TERM,
          term: DF.variable('a'),
        };
        descOrderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.OPERATOR,
          operator: 'desc',
          args: [ orderA ],
        };
      });

      it('should sort as an ascending integer', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('11', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
        ]);
      });

      it('should sort as an descending integer', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ descOrderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('11', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
        ]);
      });
    });
  });

  describe('ActorQueryOperationOrderBy with double type', () => {
    let bus: any;
    let mediatorQueryOperation: any;
    let mediatorExpressionEvaluatorFactory: MediatorExpressionEvaluatorFactory;
    let mediatorTermComparatorFactory: MediatorTermComparatorFactory;
    let context: IActionContext;

    beforeEach(() => {
      bus = new Bus({ name: 'bus' });
      mediatorQueryOperation = {
        mediate: (arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('1.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#double')) ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('11.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#double')) ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#double')) ],
            ]),
          ]),
          metadata: () => Promise.resolve({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
            ],
          }),
          operated: arg,
          type: 'bindings',
        }),
      };

      mediatorExpressionEvaluatorFactory = getMockMediatorExpressionEvaluatorFactory({
        mediatorFunctionFactory,
      });
      mediatorTermComparatorFactory = createTermCompMediator();
      context = getMockEEActionContext();
    });

    describe('An ActorQueryOperationOrderBy instance', () => {
      let actor: ActorQueryOperationOrderBy;
      let orderA: Algebra.TermExpression;
      let descOrderA: Algebra.OperatorExpression;

      beforeEach(() => {
        actor = new ActorQueryOperationOrderBy({
          name: 'actor',
          bus,
          mediatorQueryOperation,
          mediatorTermComparatorFactory,
          mediatorExpressionEvaluatorFactory,
        });
        orderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.TERM,
          term: DF.variable('a'),
        };
        descOrderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.OPERATOR,
          operator: 'desc',
          args: [ orderA ],
        };
      });

      it('should sort as an ascending double', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#double')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#double')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('11.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#double')) ],
          ]),
        ]);
      });

      it('should sort as an descending double', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ descOrderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('11.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#double')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#double')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#double')) ],
          ]),
        ]);
      });
    });
  });

  describe('ActorQueryOperationOrderBy with decimal type', () => {
    let bus: any;
    let mediatorQueryOperation: any;
    let mediatorExpressionEvaluatorFactory: MediatorExpressionEvaluatorFactory;
    let mediatorTermComparatorFactory: MediatorTermComparatorFactory;
    let context: IActionContext;

    beforeEach(() => {
      bus = new Bus({ name: 'bus' });
      mediatorQueryOperation = {
        mediate: (arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#decimal')) ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('11', DF.namedNode('http://www.w3.org/2001/XMLSchema#decimal')) ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2', DF.namedNode('http://www.w3.org/2001/XMLSchema#decimal')) ],
            ]),
          ]),
          metadata: () => Promise.resolve({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
            ],
          }),
          operated: arg,
          type: 'bindings',
        }),
      };

      mediatorExpressionEvaluatorFactory = getMockMediatorExpressionEvaluatorFactory({
        mediatorFunctionFactory,
      });
      mediatorTermComparatorFactory = createTermCompMediator();
      context = getMockEEActionContext();
    });

    describe('An ActorQueryOperationOrderBy instance', () => {
      let actor: ActorQueryOperationOrderBy;
      let orderA: Algebra.TermExpression;
      let descOrderA: Algebra.OperatorExpression;

      beforeEach(() => {
        actor = new ActorQueryOperationOrderBy({
          name: 'actor',
          bus,
          mediatorQueryOperation,
          mediatorTermComparatorFactory,
          mediatorExpressionEvaluatorFactory,
        });
        orderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.TERM,
          term: DF.variable('a'),
        };
        descOrderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.OPERATOR,
          operator: 'desc',
          args: [ orderA ],
        };
      });

      it('should sort as an ascending decimal', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#decimal')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2', DF.namedNode('http://www.w3.org/2001/XMLSchema#decimal')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('11', DF.namedNode('http://www.w3.org/2001/XMLSchema#decimal')) ],
          ]),
        ]);
      });

      it('should sort as an descending decimal', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ descOrderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('11', DF.namedNode('http://www.w3.org/2001/XMLSchema#decimal')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2', DF.namedNode('http://www.w3.org/2001/XMLSchema#decimal')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#decimal')) ],
          ]),
        ]);
      });
    });
  });

  describe('ActorQueryOperationOrderBy with float type', () => {
    let bus: any;
    let mediatorQueryOperation: any;
    let mediatorExpressionEvaluatorFactory: MediatorExpressionEvaluatorFactory;
    let mediatorTermComparatorFactory: MediatorTermComparatorFactory;
    let context: IActionContext;

    beforeEach(() => {
      bus = new Bus({ name: 'bus' });
      mediatorQueryOperation = {
        mediate: (arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('1.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#float')) ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('11.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#float')) ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#float')) ],
            ]),
          ]),
          metadata: () => Promise.resolve({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
            ],
          }),
          operated: arg,
          type: 'bindings',
        }),
      };
      mediatorExpressionEvaluatorFactory = getMockMediatorExpressionEvaluatorFactory({
        mediatorFunctionFactory,
      });
      mediatorTermComparatorFactory = createTermCompMediator();
      context = getMockEEActionContext();
    });

    describe('An ActorQueryOperationOrderBy instance', () => {
      let actor: ActorQueryOperationOrderBy;
      let orderA: Algebra.TermExpression;
      let descOrderA: Algebra.OperatorExpression;

      beforeEach(() => {
        actor = new ActorQueryOperationOrderBy({
          name: 'actor',
          bus,
          mediatorQueryOperation,
          mediatorTermComparatorFactory,
          mediatorExpressionEvaluatorFactory,
        });
        orderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.TERM,
          term: DF.variable('a'),
        };
        descOrderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.OPERATOR,
          operator: 'desc',
          args: [ orderA ],
        };
      });

      it('should sort as an ascending float', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#float')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#float')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('11.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#float')) ],
          ]),
        ]);
      });

      it('should sort as an descending float', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ descOrderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('11.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#float')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#float')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#float')) ],
          ]),
        ]);
      });
    });
  });

  describe('ActorQueryOperationOrderBy with mixed literal types', () => {
    let bus: any;
    let mediatorQueryOperation: any;
    let mediatorExpressionEvaluatorFactory: MediatorExpressionEvaluatorFactory;
    let mediatorTermComparatorFactory: MediatorTermComparatorFactory;
    let context: IActionContext;

    beforeEach(() => {
      bus = new Bus({ name: 'bus' });
      mediatorQueryOperation = {
        mediate: (arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('11', DF.namedNode('http://www.w3.org/2001/XMLSchema#string')) ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#double')) ],
            ]),
          ]),
          metadata: () => Promise.resolve({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
            ],
          }),
          operated: arg,
          type: 'bindings',
        }),
      };
      mediatorExpressionEvaluatorFactory = getMockMediatorExpressionEvaluatorFactory({
        mediatorFunctionFactory,
      });
      mediatorTermComparatorFactory = createTermCompMediator();
      context = getMockEEActionContext();
    });

    describe('An ActorQueryOperationOrderBy instance', () => {
      let actor: ActorQueryOperationOrderBy;
      let orderA: Algebra.TermExpression;
      let descOrderA: Algebra.OperatorExpression;

      beforeEach(() => {
        actor = new ActorQueryOperationOrderBy({
          name: 'actor',
          bus,
          mediatorQueryOperation,
          mediatorTermComparatorFactory,
          mediatorExpressionEvaluatorFactory,
        });
        orderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.TERM,
          term: DF.variable('a'),
        };
        descOrderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.OPERATOR,
          operator: 'desc',
          args: [ orderA ],
        };
      });

      it('should sort as an ascending integer', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#double')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('11', DF.namedNode('http://www.w3.org/2001/XMLSchema#string')) ],
          ]),
        ]);
      });

      it('should sort as an descending integer', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ descOrderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
        checkOrder(array, [
          BF.bindings([
            [ DF.variable('a'), DF.literal('11', DF.namedNode('http://www.w3.org/2001/XMLSchema#string')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2.0e6', DF.namedNode('http://www.w3.org/2001/XMLSchema#double')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
        ]);
      });
    });
  });

  describe('Another ActorQueryOperationOrderBy with mixed types', () => {
    let bus: any;
    let mediatorQueryOperation: any;
    let mediatorExpressionEvaluatorFactory: MediatorExpressionEvaluatorFactory;
    let mediatorTermComparatorFactory: MediatorTermComparatorFactory;
    let context: IActionContext;

    beforeEach(() => {
      bus = new Bus({ name: 'bus' });
      mediatorQueryOperation = {
        mediate: (arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.variable('a') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.variable('b') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.variable('c') ],
            ]),
          ]),
          metadata: () => Promise.resolve({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
            ],
          }),
          operated: arg,
          type: 'bindings',
        }),
      };

      mediatorExpressionEvaluatorFactory = getMockMediatorExpressionEvaluatorFactory({
        mediatorFunctionFactory,
      });
      mediatorTermComparatorFactory = createTermCompMediator();
      context = getMockEEActionContext();
    });

    describe('An ActorQueryOperationOrderBy instance', () => {
      let actor: ActorQueryOperationOrderBy;
      let orderA: Algebra.TermExpression;

      beforeEach(() => {
        actor = new ActorQueryOperationOrderBy({
          name: 'actor',
          bus,
          mediatorQueryOperation,
          mediatorTermComparatorFactory,
          mediatorExpressionEvaluatorFactory,
        });
        orderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.TERM,
          term: DF.variable('a'),
        };
      });

      it('should not sort since its not a literal ascending', async() => {
        try {
          const op: any = {
            operation: { type: 'orderby', input: {}, expressions: [ orderA ]},
            context,
          };
          const output = await actor.run(op, undefined);
          const array = await arrayifyStream(getSafeBindings(output).bindingsStream);
          expect(array).toBeFalsy();
        } catch {
          // Is valid
        }
      });
    });
  });

  describe('ActorQueryOperationOrderBy with deletions', () => {
    let bus: any;
    let mediatorQueryOperation: any;
    let mediatorExpressionEvaluatorFactory: MediatorExpressionEvaluatorFactory;
    let mediatorTermComparatorFactory: MediatorTermComparatorFactory;
    let context: IActionContext;
    let queue: Bindings[];
    let iterator: AsyncIterator<Bindings>;

    beforeEach(() => {
      queue = [
        BF.bindings([
          [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('11', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('7', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('2', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
        ]),
        BF.bindings([
          [ DF.variable('a'), DF.literal('5', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
        ]),
      ];
      iterator = new AsyncIterator();
      iterator.read = () => {
        if (queue.length > 0) {
          return queue.shift() ?? null;
        }
        iterator.readable = false;
        return null;
      };
      iterator.readable = true;
      bus = new Bus({ name: 'bus' });
      mediatorQueryOperation = {
        mediate: (arg: any) => Promise.resolve({
          bindingsStream: iterator,
          metadata: () => Promise.resolve({
            cardinality: 3,
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: true,
              },
            ],
          }),
          operated: arg,
          type: 'bindings',
        }),
      };

      mediatorExpressionEvaluatorFactory = getMockMediatorExpressionEvaluatorFactory({
        mediatorFunctionFactory,
      });
      mediatorTermComparatorFactory = createTermCompMediator();
      context = getMockEEActionContext();
    });

    afterEach(() => {
      iterator.close();
    });

    describe('An ActorQueryOperationOrderBy instance', () => {
      let actor: ActorQueryOperationOrderBy;
      let orderA: Algebra.TermExpression;
      let descOrderA: Algebra.OperatorExpression;

      beforeEach(() => {
        actor = new ActorQueryOperationOrderBy({
          name: 'actor',
          bus,
          mediatorQueryOperation,
          mediatorTermComparatorFactory,
          mediatorExpressionEvaluatorFactory,
        });
        orderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.TERM,
          term: DF.variable('a'),
        };
        descOrderA = {
          type: Algebra.types.EXPRESSION,
          expressionType: Algebra.expressionTypes.OPERATOR,
          operator: 'desc',
          args: [ orderA ],
        };
      });

      it('should error on non existing deletion', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const bindingsStream = <AsyncIterator<Bindings>><any>getSafeBindings(output).bindingsStream;
        checkOrder(await partialArrayifyAsyncIterator(bindingsStream, 5), [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('5', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('7', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('11', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
        ]);
        queue.push(
          BF.bindings([
            [ DF.variable('a'), DF.literal('0', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]).setContextEntry(KeysBindings.isAddition, false),
        );
        iterator.readable = true;
        await expect(new Promise<void>((resolve, reject) => {
          bindingsStream.on('error', e => reject(e));
          bindingsStream.on('end', resolve);
          bindingsStream.on('data', resolve);
        })).rejects.toThrow('Deletion does not exist');
        expect(iterator.closed).toBeTruthy();
      });

      it('should sort as an ascending integer', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ orderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const bindingsStream = <AsyncIterator<Bindings>><any>getSafeBindings(output).bindingsStream;
        checkOrder(await partialArrayifyAsyncIterator(bindingsStream, 5), [
          BF.bindings([
            [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('5', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('7', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('11', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
        ]);
        queue.push(
          BF.bindings([
            [ DF.variable('a'), DF.literal('5', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('7', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]).setContextEntry(KeysBindings.isAddition, false),
        );
        iterator.readable = true;
        const deleteResults = await partialArrayifyAsyncIterator(bindingsStream, 3);
        expect(deleteResults[0]).toEqualBindings(BF.bindings([
          [ DF.variable('a'), DF.literal('5', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
        ]).setContextEntry(KeysBindings.isAddition, false));
        expect(deleteResults[0].getContextEntry(KeysBindings.order)!.index).toBe(2);
        expect(deleteResults[1]).toEqualBindings(BF.bindings([
          [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
        ]).setContextEntry(KeysBindings.isAddition, false));
        expect(deleteResults[1].getContextEntry(KeysBindings.order)!.index).toBe(0);
        expect(deleteResults[2]).toEqualBindings(BF.bindings([
          [ DF.variable('a'), DF.literal('7', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
        ]).setContextEntry(KeysBindings.isAddition, false));
        expect(deleteResults[2].getContextEntry(KeysBindings.order)!.index).toBe(1);
      });

      it('should sort as an descending integer', async() => {
        const op: any = {
          operation: { type: 'orderby', input: {}, expressions: [ descOrderA ]},
          context,
        };
        const output = await actor.run(op, undefined);
        const bindingsStream = <AsyncIterator<Bindings>><any>getSafeBindings(output).bindingsStream;
        checkOrder(await partialArrayifyAsyncIterator(bindingsStream, 5), [
          BF.bindings([
            [ DF.variable('a'), DF.literal('11', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('7', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('5', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('2', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]),
        ]);
        queue.push(
          BF.bindings([
            [ DF.variable('a'), DF.literal('5', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([
            [ DF.variable('a'), DF.literal('7', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
          ]).setContextEntry(KeysBindings.isAddition, false),
        );
        iterator.readable = true;
        const deleteResults = await partialArrayifyAsyncIterator(bindingsStream, 3);
        expect(deleteResults[0]).toEqualBindings(BF.bindings([
          [ DF.variable('a'), DF.literal('5', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
        ]).setContextEntry(KeysBindings.isAddition, false));
        expect(deleteResults[0].getContextEntry(KeysBindings.order)!.index).toBe(2);
        expect(deleteResults[1]).toEqualBindings(BF.bindings([
          [ DF.variable('a'), DF.literal('1', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
        ]).setContextEntry(KeysBindings.isAddition, false));
        expect(deleteResults[1].getContextEntry(KeysBindings.order)!.index).toBe(3);
        expect(deleteResults[2]).toEqualBindings(BF.bindings([
          [ DF.variable('a'), DF.literal('7', DF.namedNode('http://www.w3.org/2001/XMLSchema#integer')) ],
        ]).setContextEntry(KeysBindings.isAddition, false));
        expect(deleteResults[2].getContextEntry(KeysBindings.order)!.index).toBe(1);
      });
    });
  });
});
